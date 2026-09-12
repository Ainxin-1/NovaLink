// NovaLink Node Pipeline 命令行入口（任务书：NovaLink Node Pipeline / node-manager）。
//
// 用法:
//
//	novanode fetch    [-dir 数据目录]   一轮完整流程：获取→解析→去重→合并→粗筛→过期清理→保存→发布
//	novanode check    [-dir 数据目录]   仅重跑粗筛并保存
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
	SingBoxPath string `json:"singbox_path"` // 提供则启用协议级深度检测
	Deep        bool   `json:"deep"`
	ChunkSize   int    `json:"chunk_size"`
	BasePort    int    `json:"base_port"`
}

func loadPipeline(dir string) pipelineCfg {
	def := pipelineCfg{
		SingBoxPath: "E:/NovaLink/core/vpn-core/sing-box-1.14.0-windows-amd64/sing-box.exe",
		Deep:        true, ChunkSize: 32, BasePort: 30000,
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
		if n.LastChecked == "" {
			due = append(due, *n)
			continue
		}
		if t, err := time.Parse(time.RFC3339, n.LastChecked); err == nil && nowT.Sub(t) >= checkInterval {
			due = append(due, *n)
		}
	}
	if len(due) == 0 {
		return nil
	}
	if deep.Deep && deep.SingBoxPath != "" {
		if _, err := os.Stat(deep.SingBoxPath); err == nil {
			logf("深度检测开始: %d 个节点（协议级真实握手，批 %d 并发 16）", len(due), deep.ChunkSize)
			return checker.Deep(due, deep.SingBoxPath, deep.BasePort, deep.ChunkSize, logf)
		}
		logf("深度检测不可用（未找到核心 %s），退回 TCP 粗筛", deep.SingBoxPath)
	}
	logf("TCP 粗筛开始: %d 个节点（16 并发）", len(due))
	return checker.TCP(due, 5*time.Second, 16)
}

func main() {
	dir := flag.String("dir", "data", "工作目录（sources.json/pool.json/published）")
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
		if err := runCheck(*dir); err != nil {
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
	results2 := checkDue(dir, pool, loadPipeline(dir))
	if len(results2) > 0 {
		a, d, f := cache.ApplyCheck(pool, results2)
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

func runCheck(dir string) error {
	pool, err := cache.Load(filepath.Join(dir, "pool.json"))
	if err != nil {
		return err
	}
	pool2 := pool
	for i := range pool2.Nodes { // 强制全量到期判定：清空 LastChecked 使其全部到期
		pool2.Nodes[i].LastChecked = ""
	}
	results := checkDue(dir, pool2, loadPipeline(dir))
	a, d, f := cache.ApplyCheck(pool, results)
	if err := cache.Save(pool, filepath.Join(dir, "pool.json")); err != nil {
		return err
	}
	logf("粗筛: %d 个，可用 %d / 较差 %d / 连续失败 %d", len(results), a, d, f)
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
