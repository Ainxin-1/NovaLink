// 深度检测：调用 sing-box 对节点做真实协议握手与外网请求验证。
// TCP 粗筛只能证明端口可达（阶段2实测：大量节点端口活着协议已死），
// 深度检测通过"批量多入站/多出站配置"一次验证一批节点，成本可控。
package checker

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"novanode/model"
	"novanode/publish"
)

// 探测目标必须同时满足两个条件：
//
//  1. HTTPS —— 明文 HTTP 会被大陆出口/伪造节点本地应答欺骗，HTTPS 需要真实
//     完成到目标站的 TLS 握手，伪造不了。
//  2. 运行环境直连不可达 —— 否则"直连就能返回 204"，节点即使是死的也会被判可用。
//
// 实测（2026-09-16，国内家宽）：
//
//	https://www.gstatic.com/generate_204  直连 204（0.3s）  ← 不能用，会全判可用
//	https://www.google.com/generate_204   直连超时          ← 国内环境正确目标
//
// 注意运行环境差异：CI 跑在 GitHub 海外机房，google 在那里直连可达，
// 单靠 google 会重新陷入假阳性。因此允许用 NOVANODE_PROBE_URL 覆盖，
// 由运行方按所处网络环境选择"该环境直连不可达"的目标
// （CI 侧见 .github/workflows/node-pipeline.yml，用 youtube 等被墙目标）。
const (
	defaultProbeURL = "https://www.google.com/generate_204"
	roundGap        = 300 * time.Millisecond // 多轮采样之间的间隔
)

// ProbeURL 返回当前生效的探测目标（可用环境变量覆盖，便于跨网络环境部署）。
func ProbeURL() string {
	if v := strings.TrimSpace(os.Getenv("NOVANODE_PROBE_URL")); v != "" {
		return v
	}
	return defaultProbeURL
}

// ProbeTargets 返回一轮检测实际使用的目标列表。
//
// 默认三目标：单目标判"可用"太宽 —— 节点只要能到 google 就算通过，
// 但客户端连接验证要求 2/3，于是出现"本地实测可用、就是连不上"。
// 用 NOVANODE_PROBE_URL 指定单一目标时（比如特殊网络环境）退化为单目标口径。
// 这里的目标同样必须满足"运行环境直连不可达"，否则等于没测（见 probe_test.go）。
func ProbeTargets() []string {
	if v := strings.TrimSpace(os.Getenv("NOVANODE_PROBE_URL")); v != "" {
		return []string{v}
	}
	return []string{
		"https://www.google.com/generate_204",
		"https://www.youtube.com/generate_204",
		"https://www.facebook.com/generate_204",
	}
}

// PassVerdict 多目标判定：过半通过才算可用（且至少要有一个通过）。
func PassVerdict(okN, total int) bool {
	if total <= 0 {
		return false
	}
	return okN > 0 && okN >= (total+1)/2
}

// Stats 是同一节点多轮采样后的稳定性画像。
//
// 为什么不能只留一个平均值：2026-09-19 实测见过"平均 450ms 但一次请求 6s"的
// 免费节点，单次采样的均值与它的 P95 差了 13 倍，用它排序会把抖动节点排在稳的
// 节点前面。丢包率（几轮里死了几轮）同理 —— 免费节点的失效是分钟级的。
type Stats struct {
	Samples int     `json:"samples"`
	OKs     int     `json:"oks"`
	P50     int     `json:"p50_ms,omitempty"`
	P95     int     `json:"p95_ms,omitempty"`
	Jitter  int     `json:"jitter_ms,omitempty"` // P95-P50
	Loss    float64 `json:"loss,omitempty"`      // 失败轮占比 0~1
}

// Summarize 由每轮的均值延迟与成功轮数算出画像。lat 只收集成功轮。
func Summarize(lat []int, rounds, oks int) Stats {
	s := Stats{Samples: rounds, OKs: oks}
	if rounds <= 0 {
		return s
	}
	s.Loss = 1 - float64(oks)/float64(rounds)
	if len(lat) == 0 {
		return s
	}
	v := append([]int{}, lat...)
	sortInts(v)
	s.P50 = percentile(v, 50)
	s.P95 = percentile(v, 95)
	if s.P95 > s.P50 {
		s.Jitter = s.P95 - s.P50
	}
	return s
}

// percentile 最近秩法（round-up）。v 必须已升序。
func percentile(v []int, p int) int {
	if len(v) == 0 {
		return 0
	}
	if p < 1 {
		p = 1
	}
	if p > 100 {
		p = 100
	}
	i := (p*len(v) + 99) / 100 // ceil(p/100 * n) - 1
	if i < 1 {
		i = 1
	}
	return v[i-1]
}

func sortInts(v []int) {
	for i := 1; i < len(v); i++ {
		for j := i; j > 0 && v[j] < v[j-1]; j-- {
			v[j], v[j-1] = v[j-1], v[j]
		}
	}
}

// Result 是单节点深检结论（含可选的多轮稳定性画像）。
type Result struct {
	ID        string
	OK        bool
	LatencyMS int
	Stats     Stats
}

// Deep 对 nodes 做协议级检测（单轮，向后兼容的入口）。
func Deep(nodes []model.Node, singboxPath string, basePort, chunkSize int, logf func(string, ...any)) map[string]Result {
	return DeepRounds(nodes, singboxPath, basePort, chunkSize, 1, logf)
}

// DeepRounds 与 Deep 相同，但每个节点连续采样 rounds 轮（同一个核心进程内完成，
// 不重复启动核心）：轮与轮之间小睡，避免把"连续失败"测成"瞬时突发"。
func DeepRounds(nodes []model.Node, singboxPath string, basePort, chunkSize, rounds int, logf func(string, ...any)) map[string]Result {
	if rounds < 1 {
		rounds = 1
	}
	out := deepInner(nodes, singboxPath, basePort, chunkSize, rounds, logf)
	return out
}

func deepInner(nodes []model.Node, singboxPath string, basePort, chunkSize, rounds int, logf func(string, ...any)) map[string]Result {
	out := map[string]Result{}
	if chunkSize <= 0 {
		chunkSize = 32
	}
	workDir, err := os.MkdirTemp("", "novanode-deep-")
	if err != nil {
		logf("深度检测无法创建临时目录: %v", err)
		return out
	}
	defer os.RemoveAll(workDir)

	total := len(nodes)
	for batch, start := 0, 0; start < total; batch, start = batch+1, start+chunkSize {
		end := start + chunkSize
		if end > total {
			end = total
		}
		chunk := nodes[start:end]
		t0 := time.Now()
		for id, r := range deepChunk(batch, chunk, singboxPath, basePort, rounds, workDir) {
			out[id] = r
		}
		logf("深度检测进度: %d/%d（本批 %d 个，耗时 %s）",
			end, total, len(chunk), time.Since(t0).Round(time.Second))
	}
	return out
}

// deepChunk 验证一批节点；配置校验失败时二分拆小批重试，
// 最终单个仍无法生成合法配置的节点记为失败（而非漏检）。
func deepChunk(batch int, chunk []model.Node, singboxPath string, basePort, rounds int, workDir string) map[string]Result {
	out := map[string]Result{}
	res, ok := tryChunk(batch, chunk, singboxPath, basePort, rounds, workDir)
	if ok {
		for id, r := range res {
			out[id] = r
		}
		return out
	}
	if len(chunk) == 1 {
		out[chunk[0].ID] = Result{ID: chunk[0].ID, OK: false}
		return out
	}
	mid := len(chunk) / 2
	for id, r := range deepChunk(batch*10, chunk[:mid], singboxPath, basePort, rounds, workDir) {
		out[id] = r
	}
	for id, r := range deepChunk(batch*10+1, chunk[mid:], singboxPath, basePort, rounds, workDir) {
		out[id] = r
	}
	return out
}

// tryChunk 尝试整批验证；返回 ok=false 表示本批配置无法通过校验（触发二分）。
func tryChunk(batch int, chunk []model.Node, singboxPath string, basePort, rounds int, workDir string) (map[string]Result, bool) {
	out := map[string]Result{}
	var inbounds []any
	var outbounds []any
	var rules []any
	type target struct {
		id   string
		port int
	}
	targets := make([]target, 0, len(chunk))
	for i := range chunk {
		n := chunk[i]
		ob := publish.Outbound(&n)
		if ob == nil {
			out[n.ID] = Result{ID: n.ID, OK: false}
			continue
		}
		tag := "in" + strconv.Itoa(i)
		port := basePort + i
		inbounds = append(inbounds, map[string]any{
			"type": "mixed", "tag": tag, "listen": "127.0.0.1", "listen_port": port,
		})
		outbounds = append(outbounds, ob)
		rules = append(rules, map[string]any{"inbound": []string{tag}, "outbound": n.ID})
		targets = append(targets, target{id: n.ID, port: port})
	}
	if len(targets) == 0 {
		return out, true
	}
	cfg := map[string]any{
		"log":       map[string]any{"level": "warn"},
		"inbounds":  inbounds,
		"outbounds": append(outbounds, map[string]any{"type": "direct", "tag": "direct"}),
		"route":     map[string]any{"rules": rules, "final": "direct"},
	}
	cfgPath := filepath.Join(workDir, fmt.Sprintf("deep_%d.json", batch))
	b, err := json.Marshal(cfg)
	if err != nil {
		return out, false
	}
	if err := os.WriteFile(cfgPath, b, 0o644); err != nil {
		return out, false
	}
	if _, err := exec.Command(singboxPath, "check", "-c", cfgPath).CombinedOutput(); err != nil {
		return out, false // 本批配置无法通过校验，交由上层二分
	}

	cmd := exec.Command(singboxPath, "run", "-c", cfgPath)
	if err := cmd.Start(); err != nil {
		return out, false
	}
	defer func() {
		_ = exec.Command("taskkill", "/F", "/T", "/PID", strconv.Itoa(cmd.Process.Pid)).Run()
		_ = cmd.Process.Kill()
		_, _ = cmd.Process.Wait()
	}()
	time.Sleep(2 * time.Second)

	var mu sync.Mutex
	var wg sync.WaitGroup
	sem := make(chan struct{}, 48)
	for _, t := range targets {
		wg.Add(1)
		sem <- struct{}{}
		go func(t target) {
			defer wg.Done()
			defer func() { <-sem }()
			c, err := net.DialTimeout("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(t.port)), time.Second)
			if err != nil {
				return // 入站未起，视为未检测
			}
			c.Close()
			cl := &http.Client{
				Timeout: 6 * time.Second,
				Transport: &http.Transport{Proxy: func(*http.Request) (*url.URL, error) {
					return url.Parse(fmt.Sprintf("http://127.0.0.1:%d", t.port))
				}},
			}
			// 与客户端 healthLoop 同口径：多目标里 ≥2 通过才算本轮成功。
			// 只测 1 个目标会产出"能到 google 但上不了 YouTube/Facebook"的节点，
			// 这些节点在本地实测里被判可用、进了候选组，却在连接验证时被 2/3 规则
			// 拒掉 —— 实测出现过"池子里有 2 个可用节点却连不上"（2026-09-19）。
			urls := ProbeTargets()
			lats := make([]int, 0, rounds)
			oks := 0
			for r := 0; r < rounds; r++ {
				if r > 0 {
					time.Sleep(roundGap) // 轮间隔：不留间隔测到的是同一瞬时的运气
				}
				var wgN sync.WaitGroup
				var muN sync.Mutex
				okN, sum, n := 0, 0, 0
				for _, u := range urls {
					wgN.Add(1)
					go func(u string) {
						defer wgN.Done()
						t0 := time.Now()
						resp, err := cl.Get(u)
						d := int(time.Since(t0).Milliseconds())
						muN.Lock()
						defer muN.Unlock()
						if err == nil {
							_ = resp.Body.Close()
							if resp.StatusCode == http.StatusNoContent {
								okN++
								sum += d
								n++
							}
						}
					}(u)
				}
				wgN.Wait()
				if n > 0 {
					lats = append(lats, sum/n)
				}
				if PassVerdict(okN, len(urls)) {
					oks++
				}
			}
			st := Summarize(lats, rounds, oks)
			med := st.P50
			if med == 0 {
				med = st.P95
			}
			mu.Lock()
			// 总判定：多轮里过半成功才算可用 —— 偶发一轮成功不许混过闸门。
			out[t.id] = Result{ID: t.id, OK: oks*2 > rounds, LatencyMS: med, Stats: st}
			mu.Unlock()
		}(t)
	}
	wg.Wait()
	return out, true
}
