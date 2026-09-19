// NovaLink Client for Windows —— 命令行启动 + 本机 Web 界面（go:embed）。
//
// 用法: novalink.exe [-dir 数据目录] [-listen 监听地址]
// 任务书阶段 3：节点列表 / 节点状态 / 设置 / 首页（状态+连接+断开）。
package main

import (
	_ "net/http/pprof"
	"embed"
	"encoding/json"
	"flag"
	"fmt"
	"io/fs"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"novalink/core"

	"novanode/cache"
	"novanode/model"
)

//go:embed web
var webFS embed.FS

// rollingProbeRound 两轮本机协议级实测之间的间隔。
// 实测内的节点结论 30 分钟后就该重测（LocalProbe.Due 负责去重），
// 间隔太短会与连接抢带宽，太长则拿旧结论选路。
const rollingProbeRound = 15 * time.Minute

type app struct {
	mu       sync.Mutex
	dataDir  string
	settings *core.Settings
	manager  *core.Manager
	logs     []string
	// 设备端协议级实测进度（结果落在 LocalProbe，选路与界面共用）
	checkTotal  int
	checkDone   int
	checkUsable int
	checking    bool
	refreshing  bool
	// 节点池按 (路径,大小,mtime) 缓存：文件已达数十 MB，而界面每 30 秒拉一次
	// /api/nodes、每次连接还要再读一遍 —— 逐请求重解析会把界面直接卡死。
	poolCache *model.Pool
	poolKey   string
}

// loadPool 读取节点池（带缓存）。
func (a *app) loadPool() (*model.Pool, error) {
	path := a.settings.PoolPath
	st, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	key := fmt.Sprintf("%s|%d|%d", path, st.Size(), st.ModTime().UnixNano())
	a.mu.Lock()
	if a.poolCache != nil && a.poolKey == key {
		p := a.poolCache
		a.mu.Unlock()
		return p, nil
	}
	a.mu.Unlock()
	pool, err := cache.Load(path)
	if err != nil {
		return nil, err
	}
	a.mu.Lock()
	a.poolCache, a.poolKey = pool, key
	a.mu.Unlock()
	return pool, nil
}

func main() {
	dir := flag.String("dir", "data", "客户端数据目录")
	listen := flag.String("listen", "", "覆盖设置中的监听地址")
	flag.Parse()

	a := &app{dataDir: *dir}
	if err := os.MkdirAll(*dir, 0o755); err != nil {
		fmt.Fprintln(os.Stderr, "错误:", err)
		os.Exit(1)
	}
	s, err := core.LoadSettings(filepath.Join(*dir, "settings.json"))
	if err != nil {
		fmt.Fprintln(os.Stderr, "读取设置失败:", err)
		os.Exit(1)
	}
	if *listen != "" {
		s.Listen = *listen
	}
	a.settings = s
	a.manager = core.NewManager(s, *dir)
	core.CleanupOrphan(s.ProxyPort, s.SingBoxPath, a.logf)
	a.logf("NovaLink 客户端启动，界面地址 http://%s", s.Listen)

	mux := http.NewServeMux()
	mux.Handle("/debug/pprof/", http.DefaultServeMux)
	mux.HandleFunc("/", a.handleIndex)
	mux.HandleFunc("/api/status", a.handleStatus)
	mux.HandleFunc("/api/nodes", a.handleNodes)
	mux.HandleFunc("/api/connect", a.handleConnect)
	mux.HandleFunc("/api/disconnect", a.handleDisconnect)
	mux.HandleFunc("/api/check", a.handleCheck)
	mux.HandleFunc("/api/refresh", a.handleRefresh)
	mux.HandleFunc("/api/settings", a.handleSettings)
	mux.HandleFunc("/api/log", a.handleLog)

	url := "http://" + s.Listen
	go openBrowser(url)
	// 后台准备分流规则集（国内站与私网直连）。刻意不阻塞启动：
	// 拿不到就退回全域代理，规则一落地，下一次连接自动带上。
	go core.EnsureRuleSets(*dir, a.logf)
	// 启动时后台检查节点池是否过期（超过 6 小时自动拉取云端最新）
	if core.PoolStale(s.PoolPath, 6*time.Hour) {
		go func() {
			a.logf("本地节点池已超过 6 小时未更新，尝试拉取云端最新…")
			src, n, err := a.manager.RefreshPool(a.logf)
			if err != nil {
				a.logf("节点池自动刷新失败: %v", err)
				return
			}
			a.logf("节点池自动刷新完成（来源 %s，%d 个节点）", src, n)
		}()
	}
	// 后台滚动实测：免费节点的时效是小时级（今晚 44841 个候选里国内只活 19 个，
	// 且一小时内结论就会变）。只在点"连接"时测一次，等于拿旧结论做新决策。
	go func() {
		for {
			if pool, err := a.loadPool(); err == nil {
				a.probePool(pool)
			} else {
				a.logf("[LOCAL] 读不到节点池，稍后重试: %v", err)
			}
			time.Sleep(rollingProbeRound)
		}
	}()
	if err := http.ListenAndServe(s.Listen, mux); err != nil {
		a.logf("服务退出: %v", err)
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// ---------- 处理器 ----------

func (a *app) handleIndex(w http.ResponseWriter, r *http.Request) {
	b, err := fs.ReadFile(webFS, "web/index.html")
	if err != nil {
		http.Error(w, "UI 资源缺失", 500)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Write(b)
}

func (a *app) handleStatus(w http.ResponseWriter, r *http.Request) {
	a.mu.Lock()
	ct, cd, cu, checking := a.checkTotal, a.checkDone, a.checkUsable, a.checking
	a.mu.Unlock()
	resp := a.manager.Snapshot()
	resp["check"] = map[string]any{"total": ct, "done": cd, "usable": cu, "running": checking}
	// verified_usable 是**已落盘的本机实测可用数**（跨轮次累计），与 check.usable
	// （本轮新测出多少）不同：结论 30 分钟内不重测，所以刚启动时本轮常常是 0，
	// 而可用节点确实存在 —— 界面上必须显示前者，否则会被误读成"一个都没有"。
	resp["verified_usable"] = a.manager.Probe().UsableCount()
	// 分流状态：随包规则是否已落地（缺了就是全域代理，国内站点也一起绕）
	resp["split_route"] = len(core.LocalRuleSets(a.dataDir)) == 2
	writeJSON(w, resp)
}

func (a *app) handleNodes(w http.ResponseWriter, r *http.Request) {
	pool, err := a.loadPool()
	if err != nil {
		http.Error(w, "读取节点池失败: "+err.Error(), 500)
		return
	}
	search := strings.ToLower(r.URL.Query().Get("search"))
	stateFilter := r.URL.Query().Get("state")
	// 默认视图隐藏失效节点与超慢节点（>800ms 的 DEGRADED 仅在荒年展示）
	hideFailed := stateFilter == "" || stateFilter == "all"
	fastAvailable := 0
	for _, n := range pool.Nodes {
		if n.State == model.StateAvailable && n.LatencyMS < 800 {
			fastAvailable++
		}
	}
	famine := fastAvailable < 3 // 荒年：快节点不足时展示慢节点兜底

	rows := []nodeRow{}
	counts := map[string]int{}
	for _, n := range pool.Nodes {
		counts[n.State]++
		if n.State == model.StateRemoved || n.State == model.StateExpired {
			continue
		}
		// 本机协议级实测过的节点在默认视图里不得被藏起来：CI 把它们的延迟
		// 标成 >800ms（DEGRADED）甚至是 NEW，而它们恰恰是这台机器上唯一能用的。
		verifiedOK, verifiedLat, verifiedSeen := a.manager.Probe().Lookup(n.ID)
		if hideFailed {
			if !verifiedOK {
				if n.State == model.StateFailed {
					continue
				}
				if n.State == model.StateNew && n.FailCount > 0 {
					continue
				}
				// 超慢节点（≥800ms 降级）默认隐藏，荒年（快节点<3）才展示兜底
				if n.State == model.StateDegraded && !famine {
					continue
				}
			}
		} else if stateFilter != "with-failed" && n.State != stateFilter {
			continue
		} else if stateFilter == "with-failed" && n.State == model.StateRemoved {
			continue
		}
		r1 := nodeRow{ID: n.ID, Name: n.Name, Protocol: n.Protocol, Server: n.Server,
			Port: n.Port, State: n.State, Cloud: n.LatencyMS, Latency: n.LatencyMS,
			Sources: len(n.Sources), FailCount: n.FailCount}
		// 2.0 运行时健康度：健康分与冷却状态（仅本机观察过的节点有数据）
		if h, ok := a.manager.HealthOf(n.ID); ok {
			r1.Health = h.HealthScore
			r1.Cool = h.CooldownUntil.After(time.Now())
		} else if a.manager.CooldownActive(n.ID) {
			r1.Cool = true
		}
		// 可达性与延迟一律以**本机协议级实测**为准：池子里的 latency 是
		// GitHub 海外机房测的，对国内线路没有判别力（同一批节点，CI 说 800 个
		// 可用，本机实测只有 19 个通、其中 11 个真能取回外网内容）。
		if d, ok := a.manager.Probe().Describe(n.ID); ok {
			r1.Stat = d
		}
		if verifiedSeen {
			if verifiedOK {
				r1.Reach, r1.Online = "yes", true
				if verifiedLat > 0 {
					r1.Latency = verifiedLat
				}
			} else {
				r1.Reach = "no"
			}
		}
		if search != "" && !strings.Contains(strings.ToLower(n.Name), search) &&
			!strings.Contains(strings.ToLower(n.Server), search) {
			continue
		}
		rows = append(rows, r1)
	}
	sort.Slice(rows, func(i, j int) bool { return nodeLess(rows[i], rows[j]) })
	writeJSON(w, map[string]any{
		"nodes": rows, "total": len(rows), "counts": counts,
		"pool_updated": pool.Updated,
	})
}

// nodeRow 是节点列表接口的行结构。
type nodeRow struct {
	ID        string `json:"id"`
	Name      string `json:"name"`
	Protocol  string `json:"protocol"`
	Server    string `json:"server"`
	Port      int    `json:"port"`
	State     string `json:"state"`
	Latency   int    `json:"latency_ms"`
	Cloud     int    `json:"cloud_latency_ms,omitempty"` // 海外机房测的，仅作参考
	Stat      string `json:"stat,omitempty"`     // 本机实测画像：p50/p95/丢包
	Reach     string `json:"reach"`
	Sources   int    `json:"sources"`
	Online    bool   `json:"online"`
	FailCount int    `json:"fail_count"`
	Health    int    `json:"health,omitempty"` // 本机健康分（0=无数据）
	Cool      bool   `json:"cooldown,omitempty"`
}

func nodeLess(a, b nodeRow) bool {
	if ra, rb := reachRank(a.Reach, a.State), reachRank(b.Reach, b.State); ra != rb {
		return ra < rb
	}
	if a.Latency != b.Latency && a.Latency > 0 && b.Latency > 0 {
		return a.Latency < b.Latency
	}
	if a.Latency == 0 != (b.Latency == 0) {
		return a.Latency != 0
	}
	return a.Name < b.Name
}

// reachRank 客户端视角排序（任务书第十五章：可用优先、延迟优先）。
func reachRank(reach, state string) int {
	switch {
	case reach == "yes":
		return 0
	case state == model.StateAvailable:
		return 1
	case state == model.StateNew:
		return 2
	case state == model.StateDegraded:
		return 3
	case state == model.StateFailed:
		return 4
	default:
		return 9
	}
}

func (a *app) handleConnect(w http.ResponseWriter, r *http.Request) {
	var req struct{ ID string `json:"id"` }
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.ID == "" {
		http.Error(w, "参数错误", 400)
		return
	}
	pool, err := a.loadPool()
	if err != nil {
		http.Error(w, "读取节点池失败", 500)
		return
	}
	for _, n := range pool.Nodes {
		if n.ID == req.ID {
			if n.State == model.StateExpired || n.State == model.StateRemoved {
				http.Error(w, "该节点已淘汰，请选择其他节点", 400)
				return
			}
			// 自动换节点候选池交给 Connect 在后台排序（本机协议级实测 →
			// 健康分 → TCP 预筛），用户点选的节点放组首。HTTP 线程不再等待。
			if err := a.manager.Connect(n, cache.Publishable(pool), a.logf); err != nil {
				http.Error(w, err.Error(), 409)
				return
			}
			writeJSON(w, map[string]any{"ok": true, "phase": core.PhaseConnecting})
			return
		}
	}
	http.Error(w, "节点不存在", 404)
}

func (a *app) handleDisconnect(w http.ResponseWriter, r *http.Request) {
	if err := a.manager.Disconnect(a.logf); err != nil {
		http.Error(w, err.Error(), 409)
		return
	}
	writeJSON(w, map[string]any{"ok": true})
}

// handleCheck 触发设备端协议级实测（后台执行，进度走 /api/status）。
func (a *app) handleCheck(w http.ResponseWriter, r *http.Request) {
	a.mu.Lock()
	if a.checking {
		a.mu.Unlock()
		http.Error(w, "检测正在进行中", 409)
		return
	}
	a.mu.Unlock()
	pool, err := a.loadPool()
	if err != nil {
		http.Error(w, "读取节点池失败", 500)
		return
	}
	go a.probePool(pool)
	writeJSON(w, map[string]any{"ok": true})
}

// probePool 后台把候选扫一遍：真实协议握手 + 经该节点取回外网内容，
// 结论写进 LocalProbe —— 它同时是选路排序和界面"可达"那一栏的依据。
//
// 为什么不再用 TCP 扫描：TCP 活着与能翻墙是两件事（实测 783 个 TCP 存活
// 只有 18 个真通），而全池 TCP 扫一遍要 45 分钟。改成协议级后每批 64 个
// 并发只要 10~50 秒，且结论真正可用于选路。
//
// 候选为什么取 cache.Publishable 而不是全池：试过扫全量精简池 3,000 个
// （2026-09-19 实测），耗时 8m58s，可用节点仍是 11 个 —— 与只扫已发布的
// 800 个（1m52s）结果完全相同，多出来的 2,200 个从未被验证过的尾部节点
// 贡献 0。广度不是瓶颈，所以按 5 倍时间零收益回退。
func (a *app) probePool(pool *model.Pool) {
	cands := cache.Publishable(pool)
	if len(cands) == 0 {
		a.logf("[LOCAL] 无可测候选（池子为空或全部失效），先用「刷新节点池」拉最新")
		return
	}
	a.mu.Lock()
	a.checking, a.checkTotal, a.checkDone, a.checkUsable = true, len(cands), 0, 0
	a.mu.Unlock()
	defer func() { a.mu.Lock(); a.checking = false; a.mu.Unlock() }()
	start := time.Now()
	tested, usable := a.manager.ProbePool(cands, func(done, ok, total int) {
		a.mu.Lock()
		a.checkDone, a.checkUsable = done, ok
		a.mu.Unlock()
	}, a.logf)
	a.logf("[LOCAL] 本机实测完成: 检测 %d 个，可用 %d 个，耗时 %s",
		tested, usable, time.Since(start).Round(time.Second))
}

// handleRefresh 从云端订阅拉取最新节点池（jsDelivr → raw → 代理回退）。
func (a *app) handleRefresh(w http.ResponseWriter, r *http.Request) {
	a.mu.Lock()
	if a.checking || a.refreshing {
		a.mu.Unlock()
		http.Error(w, "检测或更新正在进行中，请稍后再试", 409)
		return
	}
	a.refreshing = true
	a.mu.Unlock()
	defer func() { a.mu.Lock(); a.refreshing = false; a.mu.Unlock() }()
	src, n, err := a.manager.RefreshPool(a.logf)
	if err != nil {
		http.Error(w, err.Error(), 502)
		return
	}
	writeJSON(w, map[string]any{"ok": true, "source": src, "total": n})
}

func (a *app) handleSettings(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet {
		writeJSON(w, a.settings)
		return
	}
	// 以当前设置为基底解码提交内容：整体替换会让界面未提交的字段被清零
	// （早先保存一次设置就可能把 singbox_path / pool_url 抹成空串）。
	s := *a.settings
	if err := json.NewDecoder(r.Body).Decode(&s); err != nil {
		http.Error(w, "参数错误", 400)
		return
	}
	if s.ProxyPort < 1 || s.ProxyPort > 65535 {
		http.Error(w, "代理端口不合法", 400)
		return
	}
	if s.MaxNodeLatencyMS < 0 {
		s.MaxNodeLatencyMS = 0
	}
	oldListen := a.settings.Listen
	a.settings = &s
	if err := core.SaveSettings(filepath.Join(a.dataDir, "settings.json"), &s); err != nil {
		http.Error(w, "保存失败", 500)
		return
	}
	a.manager.SetError("")
	a.manager.SetSettings(&s) // 让运行中的管理器立即用上新值（闸门/端口/核心路径）
	a.logf("设置已保存（监听地址变更需重启客户端生效）")
	if s.Listen != oldListen {
		a.logf("监听地址已改为 %s", s.Listen)
	}
	writeJSON(w, s)
}

func (a *app) handleLog(w http.ResponseWriter, r *http.Request) {
	a.mu.Lock()
	logs := append([]string(nil), a.logs...)
	a.mu.Unlock()
	writeJSON(w, map[string]any{"lines": logs})
}

// ---------- 辅助 ----------

func (a *app) logf(format string, args ...any) {
	line := time.Now().Format("15:04:05") + " " + fmt.Sprintf(format, args...)
	a.mu.Lock()
	a.logs = append(a.logs, line)
	if len(a.logs) > 300 {
		a.logs = a.logs[len(a.logs)-300:]
	}
	a.mu.Unlock()
	fmt.Println(line)
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	_ = json.NewEncoder(w).Encode(v)
}

func openBrowser(url string) {
	_ = exec.Command("rundll32", "url.dll,FileProtocolHandler", url).Start()
}
