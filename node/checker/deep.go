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
	"sync"
	"time"

	"novanode/model"
	"novanode/publish"
)

const probeURL = "http://www.gstatic.com/generate_204"

// Deep 对 nodes 做协议级检测：按 chunkSize 分批，每批生成一个
// 多入站/多出站的 sing-box 配置（入站 i 固定路由到出站 i），
// 启动一个核心进程并发验证整批，然后更换下一批。
// 无法生成出站的节点直接记失败；配置无法通过校验的批次记为未检测（留空）。
func Deep(nodes []model.Node, singboxPath string, basePort, chunkSize int, logf func(string, ...any)) map[string]Result {
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
		for id, r := range deepChunk(batch, chunk, singboxPath, basePort, workDir) {
			out[id] = r
		}
		logf("深度检测进度: %d/%d（本批 %d 个，耗时 %s）",
			end, total, len(chunk), time.Since(t0).Round(time.Second))
	}
	return out
}

// deepChunk 验证一批节点；配置校验失败时二分拆小批重试，
// 最终单个仍无法生成合法配置的节点记为失败（而非漏检）。
func deepChunk(batch int, chunk []model.Node, singboxPath string, basePort int, workDir string) map[string]Result {
	out := map[string]Result{}
	res, ok := tryChunk(batch, chunk, singboxPath, basePort, workDir)
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
	for id, r := range deepChunk(batch*10, chunk[:mid], singboxPath, basePort, workDir) {
		out[id] = r
	}
	for id, r := range deepChunk(batch*10+1, chunk[mid:], singboxPath, basePort, workDir) {
		out[id] = r
	}
	return out
}

// tryChunk 尝试整批验证；返回 ok=false 表示本批配置无法通过校验（触发二分）。
func tryChunk(batch int, chunk []model.Node, singboxPath string, basePort int, workDir string) (map[string]Result, bool) {
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
	sem := make(chan struct{}, 16)
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
			t0 := time.Now()
			resp, err := cl.Get(probeURL)
			lat := int(time.Since(t0).Milliseconds())
			mu.Lock()
			defer mu.Unlock()
			if err == nil {
				_ = resp.Body.Close()
				out[t.id] = Result{ID: t.id, OK: resp.StatusCode == http.StatusNoContent, LatencyMS: lat}
			} else {
				out[t.id] = Result{ID: t.id, OK: false}
			}
		}(t)
	}
	wg.Wait()
	return out, true
}
