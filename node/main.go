// NovaLink Node Pipeline 命令行入口（任务书：NovaLink Node Pipeline / node-manager）。
//
// 用法:
//
//	novanode fetch    [-dir 数据目录]   一轮完整流程：获取→解析→去重→合并→粗筛→过期清理→保存→发布
//	novanode check    [-dir 数据目录]   复检到期节点并保存；加 -full 强制全量复检
//	novanode publish  [-dir 数据目录]   仅重新生成发布文件
//	novanode status   [-dir 数据目录]   打印节点池统计
//
// 日志规范（任务书第二十一章）：不打印节点 URI 与任何凭据。
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"

	"novanode/cache"
	"novanode/checker"
	"novanode/dedup"
	"novanode/model"
	"novanode/parser"
	"novanode/publish"
	"novanode/sources"
)

const checkInterval = 6 * time.Hour // 粗筛最小间隔

// pipelineCfg 管线运行参数（data/pipeline.json，缺省自动生成）。
type pipelineCfg struct {
	SingBoxPath  string `json:"singbox_path"`  // 提供则启用协议级深度检测
	Deep         bool   `json:"deep"`
	ChunkSize    int    `json:"chunk_size"`
	BasePort     int    `json:"base_port"`
	MaxLatencyMS int    `json:"max_latency_ms"` // 可用线：超过则降级，连续2轮超线判死
	MaxDeep      int    `json:"max_deep"`       // 单轮深检上限（0=不限）；深检单批约 1 分钟，需控总时长
}

func loadPipeline(dir string) pipelineCfg {
	def := pipelineCfg{
		SingBoxPath: "E:/NovaLink/core/vpn-core/sing-box-1.14.0-windows-amd64/sing-box.exe",
		Deep:        true, ChunkSize: 64, BasePort: 30000, MaxLatencyMS: 800,
		MaxDeep: 6000, // 云端源可达 1.5 万节点，限 6000 个可使单轮深检约 100 分钟可控
	}
	b, err := os.ReadFile(filepath.Join(dir, "pipeline.json"))
	if err != nil {
		if jb, e := json.MarshalIndent(def, "", "  "); e == nil {
			_ = os.WriteFile(filepath.Join(dir, "pipeline.json"), jb, 0o644)
		}
		return def
	}
	cfg := def
	_ = json.Unmarshal(b, &cfg)
	return cfg
}

func checkDue(dir string, pool *model.Pool, deep pipelineCfg) map[string]checker.Result {
	due := []model.Node{}
	nowT := time.Now().UTC()
	for _, n := range pool.Nodes {
		switch n.State {
		case model.StateExpired, model.StateRemoved:
			continue
		}
		// 失效节点已不发布，降频复检（24h），把检测资源留给可用与候选
		interval := checkInterval
		if n.State == model.StateFailed {
			interval = 24 * time.Hour
		}
		if n.LastChecked == "" {
			due = append(due, *n)
			continue
		}
		if t, err := time.Parse(time.RFC3339, n.LastChecked); err == nil && nowT.Sub(t) >= interval {
			due = append(due, *n)
		}
	}
	if len(due) == 0 {
		return nil
	}
	// 两阶段：TCP 快速淘汰（大批量、短超时）-> 协议级深度检测（只测 TCP 活的）
	t0 := time.Now()
	tcpResults := checker.TCP(due, 3*time.Second, 256)
	logf("TCP 快筛: %d 个，存活 %d，耗时 %s",
		len(due), countOK(tcpResults), time.Since(t0).Round(time.Second))
	alive := make([]model.Node, 0, len(tcpResults))
	tcpDead := map[string]bool{}
	for _, n := range due {
		if r, ok := tcpResults[n.ID]; ok && !r.OK {
			tcpDead[n.ID] = true
			continue
		}
		alive = append(alive, n)
	}
	if deep.Deep && deep.SingBoxPath != "" {
		if _, err := os.Stat(deep.SingBoxPath); err == nil {
			// 深检单批约 1 分钟，必须控总量，否则 CI 定时任务跑不完。
			// 优先级：可用/降级（要维持发布）> 候选（NEW/TESTING）> 其他。
			if deep.MaxDeep > 0 && len(alive) > deep.MaxDeep {
				logf("深度检测限流: 存活 %d 个，本轮取优先级最高的 %d 个（其余留到下一轮）",
					len(alive), deep.MaxDeep)
				alive = prioritize(alive, deep.MaxDeep)
			}
			logf("深度检测开始: %d 个节点（协议级真实握手，批 %d）", len(alive), deep.ChunkSize)
			results := checker.Deep(alive, deep.SingBoxPath, deep.BasePort, deep.ChunkSize, logf)
			for id := range tcpResults { // TCP 已死的直接计失败
				if tcpDead[id] {
					if _, covered := results[id]; !covered {
						results[id] = checker.Result{ID: id, OK: false}
					}
				}
			}
			return results
		}
		logf("深度检测不可用（未找到核心 %s），退回 TCP 粗筛", deep.SingBoxPath)
	}
	return tcpResults
}

// prioritize 在候选超过上限时挑出最该检测的节点：
// 已发布状态（可用/降级）优先保活，其次是新节点与候选，
// 最后是失败节点（本来就有 24h 降频），同档内按加入时间新的优先。
func prioritize(nodes []model.Node, limit int) []model.Node {
	rank := func(s string) int {
		switch s {
		case model.StateAvailable, model.StateDegraded:
			return 0 // 正在发布，必须优先保活
		case model.StateNew, model.StateTesting, "":
			return 1 // 新节点/候选，尽快定级
		default:
			return 2 // FAILED 等，降频即可
		}
	}
	out := make([]model.Node, len(nodes))
	copy(out, nodes)
	sort.SliceStable(out, func(i, j int) bool {
		ri, rj := rank(out[i].State), rank(out[j].State)
		if ri != rj {
			return ri < rj
		}
		return out[i].FirstSeen > out[j].FirstSeen // 新的优先
	})
	return out[:limit]
}

func countOK(m map[string]checker.Result) int {
	c := 0
	for _, r := range m {
		if r.OK {
			c++
		}
	}
	return c
}

func main() {
	dir := flag.String("dir", "data", "工作目录（sources.json/pool.json/published）")
	full := flag.Bool("full", false, "check 时强制全量复检（默认只测到期节点）")
	flag.Parse()
	if flag.NArg() < 1 {
		usage()
		os.Exit(2)
	}
	if err := os.MkdirAll(*dir, 0o755); err != nil {
		fail("创建工作目录失败: %v", err)
	}
	cmd := flag.Arg(0)
	switch cmd {
	case "fetch":
		if err := runFetch(*dir); err != nil {
			fail("fetch 失败: %v", err)
		}
	case "check":
		if err := runCheck(*dir, *full); err != nil {
			fail("check 失败: %v", err)
		}
	case "publish":
		if err := runPublish(*dir); err != nil {
			fail("publish 失败: %v", err)
		}
	case "status":
		if err := runStatus(*dir); err != nil {
			fail("status 失败: %v", err)
		}
	default:
		usage()
		os.Exit(2)
	}
}

func runFetch(dir string) error {
	poolPath := filepath.Join(dir, "pool.json")
	// 1. 来源配置
	cfgs, err := loadSources(dir)
	if err != nil {
		return err
	}
	logf("来源配置: %d 个", len(cfgs))

	// 2. 并发获取（来源失败相互隔离，任务书第八章）
	texts, results := sources.FetchAll(cfgs)
	okCnt := 0
	for _, r := range results {
		if r.OK {
			okCnt++
			logf("来源[%s] 获取成功", r.Name)
		} else {
			logf("来源[%s] 获取失败: %s（不影响其他来源）", r.Name, r.Error)
		}
	}
	// 3. 解析 + 格式检查
	bySource := map[string][]model.Node{}
	total := 0
	for name, text := range texts {
		nodes, dropped := parser.ParseLines(text)
		bySource[name] = nodes
		total += len(nodes)
		logf("来源[%s] 解析出 %d 个节点，格式检查丢弃 %d 行", name, len(nodes), dropped)
	}
	// 4. 去重（任务书第十二章）
	candidates := dedup.Merge(bySource)
	logf("去重后候选节点: %d（解析合计 %d）", len(candidates), total)

	// 5. 合并入池
	pool, err := cache.Load(poolPath)
	if err != nil {
		return fmt.Errorf("读取节点池失败: %w", err)
	}
	seen := cache.Seen(candidates)
	added := cache.Merge(pool, candidates, seen)
	logf("节点池合并: 新增 %d，池内共 %d", added, len(pool.Nodes))

	// 6. 深度检测/粗筛（仅测到期节点）
	deep := loadPipeline(dir)
	results2 := checkDue(dir, pool, deep)
	if len(results2) > 0 {
		a, d, f := cache.ApplyCheck(pool, results2, deep.MaxLatencyMS)
		logf("检测完成: %d 个，可用 %d / 较差 %d / 连续失败 %d", len(results2), a, d, f)
	}

	// 7. 过期清理（任务书第十六章）
	expired, removed := cache.Age(pool, seen)
	if expired+removed > 0 {
		logf("过期清理: EXPIRED %d，移出 %d，池内剩余 %d", expired, removed, len(pool.Nodes))
	}

	// 8. 保存 + 发布
	if err := cache.Save(pool, poolPath); err != nil {
		return err
	}
	n, err := publish.WriteAll(filepath.Join(dir, "published"), pool, cache.Publishable(pool))
	if err != nil {
		return err
	}
	logf("发布完成: %d 个可发布节点 -> %s", n, filepath.Join(dir, "published"))
	return nil
}

// runCheck 重跑检测。默认只测到期节点（与 fetch 内部一致，耗时可控）；
// 传 full=true 才强制全量复检（1.5 万节点需 1 小时以上，慎用）。
func runCheck(dir string, full bool) error {
	pool, err := cache.Load(filepath.Join(dir, "pool.json"))
	if err != nil {
		return err
	}
	deep := loadPipeline(dir)
	target := pool
	if full {
		pool2 := pool
		for i := range pool2.Nodes { // 强制全量到期判定：清空 LastChecked 使其全部到期
			pool2.Nodes[i].LastChecked = ""
		}
		target = pool2
		logf("check -full: 强制全量复检 %d 个节点", len(pool2.Nodes))
	} else {
		logf("check: 仅复检到期节点（如需全量请加 -full）")
	}
	results := checkDue(dir, target, deep)
	if len(results) == 0 {
		logf("check: 无到期节点，跳过")
		return nil
	}
	a, d, f := cache.ApplyCheck(pool, results, deep.MaxLatencyMS)
	if err := cache.Save(pool, filepath.Join(dir, "pool.json")); err != nil {
		return err
	}
	logf("检测完成: %d 个，可用 %d / 较差 %d / 连续失败 %d", len(results), a, d, f)
	_, err = publish.WriteAll(filepath.Join(dir, "published"), pool, cache.Publishable(pool))
	return err
}

func runPublish(dir string) error {
	pool, err := cache.Load(filepath.Join(dir, "pool.json"))
	if err != nil {
		return err
	}
	nodes := cache.Publishable(pool)
	publish.SortByLatency(nodes)
	n, err := publish.WriteAll(filepath.Join(dir, "published"), pool, nodes)
	if err != nil {
		return err
	}
	logf("重新发布: %d 个节点", n)
	return nil
}

func runStatus(dir string) error {
	pool, err := cache.Load(filepath.Join(dir, "pool.json"))
	if err != nil {
		return err
	}
	states := map[string]int{}
	protos := map[string]int{}
	for _, n := range pool.Nodes {
		states[n.State]++
		protos[n.Protocol]++
	}
	fmt.Printf("节点池: %s\n共 %d 个节点\n", pool.Updated, len(pool.Nodes))
	fmt.Printf("按状态: ")
	for _, s := range []string{model.StateNew, model.StateAvailable, model.StateDegraded, model.StateFailed, model.StateExpired, model.StateRemoved} {
		fmt.Printf("%s=%d ", s, states[s])
	}
	fmt.Printf("\n按协议: ")
	for _, p := range []string{"ss", "trojan", "vmess", "vless"} {
		fmt.Printf("%s=%d ", p, protos[p])
	}
	fmt.Println()
	pub := cache.Publishable(pool)
	fmt.Printf("可发布: %d 个\n", len(pub))
	return nil
}

// loadSources 读取来源配置；不存在时写入默认种子配置。
func loadSources(dir string) ([]model.SourceConfig, error) {
	path := filepath.Join(dir, "sources.json")
	b, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		cfgs := defaultSources()
		jb, _ := json.Marshal(cfgs)
		if werr := os.WriteFile(path, jb, 0o644); werr != nil {
			return nil, werr
		}
		logf("未找到 sources.json，已生成默认来源配置: %s", path)
		return cfgs, nil
	}
	if err != nil {
		return nil, err
	}
	var cfgs []model.SourceConfig
	if err := json.Unmarshal(b, &cfgs); err != nil {
		return nil, fmt.Errorf("sources.json 格式错误: %w", err)
	}
	return cfgs, nil
}

func defaultSources() []model.SourceConfig {
	return []model.SourceConfig{
		{Name: "本地-Eternity样本", URL: "file:///E:/NovaLink/verify/stage1/eternity_uris.txt", Type: "auto", Enabled: true, Note: "阶段1样本"},
		{Name: "本地-Epodonios-ss", URL: "file:///E:/NovaLink/verify/stage1/ep_ss_uris.txt", Type: "auto", Enabled: true, Note: "阶段1样本"},
		{Name: "本地-Epodonios-trojan", URL: "file:///E:/NovaLink/verify/stage1/ep_trojan_uris.txt", Type: "auto", Enabled: true, Note: "阶段1样本"},
		{Name: "远程-Eternity(jsDelivr)", URL: "https://cdn.jsdelivr.net/gh/mahdibland/V2RayAggregator@master/Eternity.txt", Type: "auto", Enabled: true, Note: "聚合器输出"},
		{Name: "远程-Eternity(raw)", URL: "https://raw.githubusercontent.com/mahdibland/V2RayAggregator/master/Eternity.txt", Type: "auto", Enabled: true, Note: "聚合器输出"},
		{Name: "远程-roosterkid(jsDelivr)", URL: "https://cdn.jsdelivr.net/gh/roosterkid/openproxylist@main/V2RAY_RAW.txt", Type: "auto", Enabled: true, Note: "openproxylist"},
		{Name: "远程-Pawdroid(jsDelivr)", URL: "https://cdn.jsdelivr.net/gh/Pawdroid/Free-servers@main/sub", Type: "auto", Enabled: true, Note: "Free-servers"},
		{Name: "远程-ermaozi(raw)", URL: "https://raw.githubusercontent.com/ermaozi/get_subscribe/main/subscribe/v2ray.txt", Type: "auto", Enabled: true, Note: "抓取+连通性测试"},
		{Name: "远程-Epodonios-ss(raw)", URL: "https://raw.githubusercontent.com/Epodonios/v2ray-configs/main/Splitted-By-Protocol/ss.txt", Type: "auto", Enabled: true, Note: "整文件base64"},
		{Name: "远程-Epodonios-trojan(raw)", URL: "https://raw.githubusercontent.com/Epodonios/v2ray-configs/main/Splitted-By-Protocol/trojan.txt", Type: "auto", Enabled: true, Note: "整文件base64"},
		{Name: "远程-AutoMerge(raw)", URL: "https://raw.githubusercontent.com/chengaopan/AutoMergePublicNodes/master/list.txt", Type: "auto", Enabled: true, Note: "时通时断，验证来源隔离"},
		{Name: "索引-lza6目录", URL: "https://raw.githubusercontent.com/lza6/free-VPN/main/README.md", Type: "index", Enabled: true, Note: "免费节点目录源，自动发现订阅地址（借鉴 ghboost）"},
	}
}

func logf(format string, args ...any) {
	fmt.Printf("[%s] %s\n", time.Now().Format("15:04:05"), fmt.Sprintf(format, args...))
}

func fail(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "错误: "+format+"\n", args...)
	os.Exit(1)
}

func usage() {
	fmt.Fprintln(os.Stderr, "用法: novanode <fetch|check|publish|status> [-dir 工作目录]")
}
