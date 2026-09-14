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
	"novanode/checker"
	"novanode/model"
)

//go:embed web
var webFS embed.FS

type app struct {
	mu       sync.Mutex
	dataDir  string
	settings *core.Settings
	manager  *core.Manager
	logs     []string
	// 设备端检测覆盖层（客户端本地观察，不写管线节点池）
	overlay     map[string]overlayEntry
	checkTotal  int
	checkDone   int
	checking    bool
	refreshing  bool
}

type overlayEntry struct {
	OK        bool   `json:"ok"`
	LatencyMS int    `json:"latency_ms"`
	At        string `json:"at"`
}

func main() {
	dir := flag.String("dir", "data", "客户端数据目录")
	listen := flag.String("listen", "", "覆盖设置中的监听地址")
	flag.Parse()

	a := &app{dataDir: *dir, overlay: map[string]overlayEntry{}}
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
	a.loadOverlay()
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
	ct, cd, checking := a.checkTotal, a.checkDone, a.checking
	a.mu.Unlock()
	resp := a.manager.Snapshot()
	resp["check"] = map[string]any{"total": ct, "done": cd, "running": checking}
	writeJSON(w, resp)
}

func (a *app) handleNodes(w http.ResponseWriter, r *http.Request) {
	pool, err := cache.Load(a.settings.PoolPath)
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

	a.mu.Lock()
	overlay := a.overlay
	a.mu.Unlock()

	rows := []nodeRow{}
	counts := map[string]int{}
	for _, n := range pool.Nodes {
		counts[n.State]++
		if n.State == model.StateRemoved || n.State == model.StateExpired {
			continue
		}
		if hideFailed {
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
		} else if stateFilter != "with-failed" && n.State != stateFilter {
			continue
		} else if stateFilter == "with-failed" && n.State == model.StateRemoved {
			continue
		}
		r1 := nodeRow{ID: n.ID, Name: n.Name, Protocol: n.Protocol, Server: n.Server,
			Port: n.Port, State: n.State, Latency: n.LatencyMS, Sources: len(n.Sources),
			FailCount: n.FailCount}
		// 2.0 运行时健康度：健康分与冷却状态（仅本机观察过的节点有数据）
		if h, ok := a.manager.HealthOf(n.ID); ok {
			r1.Health = h.HealthScore
			r1.Cool = h.CooldownUntil.After(time.Now())
		} else if a.manager.CooldownActive(n.ID) {
			r1.Cool = true
		}
		if e, ok := overlay[n.ID]; ok {
			if e.OK {
				r1.Reach, r1.Online = "yes", true
				if e.LatencyMS > 0 && (r1.Latency == 0 || r1.State == model.StateNew) {
					r1.Latency = e.LatencyMS
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
	pool, err := cache.Load(a.settings.PoolPath)
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
			// 自动换节点候选池：按健康分排序（稳定优先，任务书第七节），
			// 用户点选的节点在 Connect 中移到组首
			nodes := cache.Publishable(pool)
			a.manager.SetFailoverPool(nodes)
			if err := a.manager.Connect(n, a.logf); err != nil {
				http.Error(w, err.Error(), 409)
				return
			}
			writeJSON(w, map[string]any{"ok": true})
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

// handleCheck 触发设备端节点检测（后台执行，进度走 /api/status）。
func (a *app) handleCheck(w http.ResponseWriter, r *http.Request) {
	a.mu.Lock()
	if a.checking {
		a.mu.Unlock()
		http.Error(w, "检测正在进行中", 409)
		return
	}
	pool, err := cache.Load(a.settings.PoolPath)
	if err != nil {
		a.mu.Unlock()
		http.Error(w, "读取节点池失败", 500)
		return
	}
	due := []model.Node{}
	for _, n := range pool.Nodes {
		if n.State != model.StateExpired && n.State != model.StateRemoved {
			due = append(due, *n)
		}
	}
	a.checking, a.checkTotal, a.checkDone = true, len(due), 0
	a.mu.Unlock()

	go func() {
		start := time.Now()
		results := checker.TCP(due, 5*time.Second, 16)
		a.mu.Lock()
		for id, r := range results {
			a.overlay[id] = overlayEntry{OK: r.OK, LatencyMS: r.LatencyMS, At: time.Now().UTC().Format(time.RFC3339)}
			a.checkDone++
		}
		a.checking = false
		a.mu.Unlock()
		a.saveOverlay()
		a.logf("设备端检测完成: %d 个节点，耗时 %s", len(due), time.Since(start).Round(time.Second))
	}()
	writeJSON(w, map[string]any{"ok": true, "total": len(due)})
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
	var s core.Settings
	if err := json.NewDecoder(r.Body).Decode(&s); err != nil {
		http.Error(w, "参数错误", 400)
		return
	}
	if s.ProxyPort < 1 || s.ProxyPort > 65535 {
		http.Error(w, "代理端口不合法", 400)
		return
	}
	oldListen := a.settings.Listen
	a.settings = &s
	if err := core.SaveSettings(filepath.Join(a.dataDir, "settings.json"), &s); err != nil {
		http.Error(w, "保存失败", 500)
		return
	}
	a.manager.SetError("")
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

func (a *app) overlayPath() string { return filepath.Join(a.dataDir, "local_check.json") }

func (a *app) loadOverlay() {
	b, err := os.ReadFile(a.overlayPath())
	if err != nil {
		return
	}
	_ = json.Unmarshal(b, &a.overlay)
}

func (a *app) saveOverlay() {
	b, err := json.MarshalIndent(a.overlay, "", " ")
	if err != nil {
		return
	}
	_ = os.WriteFile(a.overlayPath(), b, 0o644)
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	_ = json.NewEncoder(w).Encode(v)
}

func openBrowser(url string) {
	_ = exec.Command("rundll32", "url.dll,FileProtocolHandler", url).Start()
}
