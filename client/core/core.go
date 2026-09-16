package core

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

// 连接阶段（任务书第二十章：未连接/连接中/已连接/连接失败）。
const (
	PhaseDisconnected = "disconnected"
	PhaseConnecting   = "connecting"
	PhaseConnected    = "connected"
	PhaseFailed       = "failed"
)

// 网络分层状态（任务书第九节：区分"进程活着"与"实际可用"）。
const (
	NetHealthy  = "HEALTHY"
	NetDegraded = "DEGRADED"
	NetFailed   = "FAILED"
)

const (
	probeInterval = 1500 * time.Millisecond
	probeTimeout  = 25 * time.Second // 单批连接验证上限
	probeHTTP     = "https://www.gstatic.com/generate_204" // HTTPS 防伪造应答
	batchSize     = 8               // 内核 urltest 组的节点数（任务书第十五章：自动选择最快）
	failoverMax   = 5               // 整批全灭时最多再换 5 批
	clashAPIPort  = 9095            // 内核 clash_api，用于查询/切换当前节点
	infoInterval  = 10 * time.Second

	healthInterval = 20 * time.Second // 健康检查周期
	urltestWait    = 12 * time.Second // 单轮全失败后给内核 urltest 自切的重选窗口
	minHold        = 45 * time.Second // 防抖动最短保持时间（任务书第十三节）
	urltestInterval = "30s"           // 任务书第三节：urltest 测试周期 2m → 30s
)

// probeTargets 多 Probe 目标（任务书第八节）：全部是轻量 204 端点，
// 任一目标都不作为唯一判断依据。
var probeTargets = []string{
	"https://www.gstatic.com/generate_204",
	"https://www.google.com/generate_204",
	"https://cp.cloudflare.com/generate_204",
}

// directTargets 直连探测目标（任务书第十四节）：国内可达轻量 204，
// 用于区分"本地网络断了"与"节点坏了"。
var directTargets = []string{
	"http://connect.rom.miui.com/generate_204",
	"http://wifi.vivo.com.cn/generate_204",
}

// Manager 管理 VPN 核心子进程与连接状态（单连接，串行管理）。
// 连接形态：一个核心进程带一批（≤8）验证可用节点组成 urltest 组，
// 节点失效由内核秒级自动切换；应用层健康检查失败时先让内核重选、
// 再降分冷却并切换组内其他节点，只有整组失败才换批，
// 只有核心进程自身异常才重启核心（任务书第一/十七节）。
type Manager struct {
	mu        sync.Mutex
	settings  *Settings
	phase     string
	node      *model.Node // 当前实际使用的节点（内核选择或手动切换，信息循环刷新）
	since     time.Time
	lastError string
	exitInfo  string
	cmd       *exec.Cmd
	done      chan struct{}
	exitErr   error // 核心进程退出错误（唯一 Wait goroutine 写入）
	stopped   bool  // 主动停止标志：区分主动停止与进程崩溃（任务书第二节）
	dataDir   string
	sysProxy  bool
	pool      []*model.Node // 候选池快照（健康分排序）
	batchIdx  int           // 当前批索引
	gen       int
	tracker   *HealthTracker
	netState  string      // 网络分层状态 HEALTHY/DEGRADED/FAILED
	lastSwitch time.Time  // 最近一次节点切换时间（防抖动）
}

// NewManager 创建核心管理器。
func NewManager(settings *Settings, dataDir string) *Manager {
	return &Manager{
		settings: settings, phase: PhaseDisconnected, dataDir: dataDir,
		tracker: NewHealthTracker(filepath.Join(dataDir, "runtime_health.json")),
	}
}

// SetFailoverPool 设置候选池快照：按健康分排序（任务书第七/十二节，
// 稳定优先于偶尔最快），冷却节点垫底。
func (m *Manager) SetFailoverPool(nodes []*model.Node) {
	m.tracker.SortByScore(nodes)
	m.mu.Lock()
	m.pool, m.batchIdx = nodes, 0
	m.mu.Unlock()
}

// SetFailoverPoolLocal 与 SetFailoverPool 相同，但先做本地 TCP 可达性预筛。
//
// 为什么必须预筛：云端 CI 跑在海外机房，它测出的"低延迟 21ms 可用节点"在国内
// 常常完全不可达。实测某个池子前 48 个节点在国内 TCP 可达率为 0%，而随机/中段
// 采样可达率约 31%-44% —— 即池子头部可能整段是死区。若不预筛，客户端会按
// 顺序开出前几批全部阵亡，首连耗时被拖到几分钟。
//
// 策略：按批扫描（每批 48 个，2.5s 超时），直到收集到 needReachable 个可达节点，
// 或扫描到 probeCap 上限为止。可达的排前面（保持相对顺序），不可达的沉到末尾
// 兜底（万一某节点只是瞬间抖动，后续换批仍能轮到它）。
func (m *Manager) SetFailoverPoolLocal(nodes []*model.Node, logf func(string, ...any)) {
	if len(nodes) == 0 {
		return
	}
	const (
		probeBatch    = 48               // 每批并发探测数
		needReachable = 3 * batchSize    // 凑够 3 批可用即停（够 failover 用）
		probeCap      = 480              // 最多扫 480 个，避免极端池子卡住点击
		probeTimeout  = 2500 * time.Millisecond
	)
	type r struct{ idx int; ok bool }

	reach := map[int]bool{} // 已探明的下标 -> 是否可达
	okN := 0
	scanned := 0
	for scanned < len(nodes) && scanned < probeCap && okN < needReachable {
		end := scanned + probeBatch
		if end > len(nodes) {
			end = len(nodes)
		}
		if end > probeCap {
			end = probeCap
		}
		cnt := end - scanned
		ch := make(chan r, cnt)
		for i := scanned; i < end; i++ {
			go func(i int) {
				nd := nodes[i]
				conn, err := net.DialTimeout("tcp",
					net.JoinHostPort(nd.Server, strconv.Itoa(nd.Port)), probeTimeout)
				if err == nil {
					_ = conn.Close()
					ch <- r{i, true}
					return
				}
				ch <- r{i, false}
			}(i)
		}
		for i := 0; i < cnt; i++ {
			v := <-ch
			reach[v.idx] = v.ok
			if v.ok {
				okN++
			}
		}
		scanned = end
	}
	if logf != nil {
		logf("[POOL] 本地预筛：扫描 %d 个候选，TCP 可达 %d 个（不可达的沉到末尾兜底）",
			scanned, okN)
	}
	// 先按健康分排（稳定优先），再把不可达的稳定沉到末尾：
	// 保证「可达」优先于「不可达」，同等条件下仍按健康分。
	out := make([]*model.Node, len(nodes))
	copy(out, nodes)
	m.tracker.SortByScore(out)
	dead := map[string]bool{}
	for i := 0; i < scanned; i++ {
		if !reach[i] {
			dead[nodes[i].ID] = true
		}
	}
	reachable := make([]*model.Node, 0, len(out))
	unreachable := make([]*model.Node, 0, len(out))
	for _, nd := range out {
		if dead[nd.ID] {
			unreachable = append(unreachable, nd)
		} else {
			reachable = append(reachable, nd)
		}
	}
	final := append(reachable, unreachable...)
	m.mu.Lock()
	m.pool, m.batchIdx = final, 0
	m.mu.Unlock()
}

// Snapshot 返回当前状态快照。
func (m *Manager) Snapshot() map[string]any {
	m.mu.Lock()
	defer m.mu.Unlock()
	node := map[string]any{"id": "", "name": "", "protocol": "", "server": "", "port": 0}
	if m.node != nil {
		node = map[string]any{
			"id": m.node.ID, "name": m.node.Name, "protocol": m.node.Protocol,
			"server": m.node.Server, "port": m.node.Port,
		}
	}
	resp := map[string]any{
		"phase":      m.phase,
		"node":       node,
		"since":      m.since.Format(time.RFC3339),
		"last_error": m.lastError,
		"exit_info":  m.exitInfo,
		"sysproxy":   m.sysProxy,
	}
	// 分层状态（任务书第九节）：进程存活 ≠ 网络可用
	resp["process_alive"] = m.cmd != nil
	resp["network_state"] = m.netState
	if m.node != nil {
		if h, ok := m.tracker.Get(m.node.ID); ok {
			resp["health"] = map[string]any{
				"score":               h.HealthScore,
				"success_count":       h.SuccessCount,
				"failure_count":       h.FailureCount,
				"consecutive_failure": h.ConsecutiveFailure,
				"avg_latency_ms":      int(h.AvgLatency),
				"cooldown_until":      cooldownRFC3339(h.CooldownUntil),
			}
		}
	}
	return resp
}

func cooldownRFC3339(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.Format(time.RFC3339)
}

// SetError 记录非连接类错误信息。
func (m *Manager) SetError(s string) {
	m.mu.Lock()
	m.lastError = s
	m.mu.Unlock()
}

// Connect 发起连接：取候选池当前批（≤8 节点）交给内核 urltest 自动选择。
func (m *Manager) Connect(n *model.Node, logf func(string, ...any)) error {
	m.mu.Lock()
	if m.phase == PhaseConnecting || m.phase == PhaseConnected {
		m.mu.Unlock()
		return fmt.Errorf("已有连接在进行，请先断开")
	}
	if len(m.pool) > 0 && m.pool[0].ID != n.ID {
		for i, c := range m.pool { // 用户点选的节点放组首
			if c.ID == n.ID {
				m.pool[0], m.pool[i] = m.pool[i], m.pool[0]
				break
			}
		}
	}
	m.node = n
	m.phase = PhaseConnecting
	m.lastError = ""
	m.since = time.Now()
	m.gen++
	gen := m.gen
	m.mu.Unlock()
	logf("发起连接: %s/%s:%d（内核将在候选组内自动选择最快节点）", n.Protocol, n.Server, n.Port)
	go m.supervise(gen, logf)
	return nil
}

// supervise 一代连接生命周期的监督者：逐批启动，整批全灭才换下一批。
func (m *Manager) supervise(gen int, logf func(string, ...any)) {
	attempt := 0
	for {
		m.mu.Lock()
		if m.gen != gen || m.phase != PhaseConnecting && m.phase != PhaseConnected {
			m.mu.Unlock()
			return
		}
		bi := m.batchIdx
		pool := m.pool
		m.mu.Unlock()
		batches := chunkPool(pool, batchSize)
		if bi >= len(batches) {
			break
		}
		if attempt >= failoverMax+1 {
			break
		}
		attempt++
		if bi > 0 {
			logf("[SWITCH] 整批全灭，换下一批(%d/%d)：%d 个节点", attempt, failoverMax+1, len(batches[bi]))
		}
		ok := m.runBatch(gen, batches[bi], logf)
		if ok {
			return // 已连接，交给健康监控与信息循环
		}
		m.mu.Lock()
		aborted := m.phase == PhaseDisconnected || m.gen != gen
		m.batchIdx++
		m.mu.Unlock()
		if aborted {
			return
		}
	}
	m.fail("连续 %d 批候选均连接失败，请稍后重试或刷新节点", failoverMax)
	logf("候选批次耗尽")
	m.mu.Lock()
	sp := m.sysProxy
	m.sysProxy = false
	m.mu.Unlock()
	if sp {
		RestoreSystemProxy(filepath.Join(m.dataDir, "sysproxy_backup.json"))
		logf("系统代理已恢复为用户原设置")
	}
}

func chunkPool(nodes []*model.Node, size int) [][]*model.Node {
	var out [][]*model.Node
	for start := 0; start < len(nodes); start += size {
		end := start + size
		if end > len(nodes) {
			end = len(nodes)
		}
		out = append(out, nodes[start:end])
	}
	return out
}

// runBatch 启动一批节点（urltest 组）并阻塞验证：连上返回 true。
func (m *Manager) runBatch(gen int, batch []*model.Node, logf func(string, ...any)) bool {
	port := m.settings.ProxyPort
	cfgPath := filepath.Join(m.dataDir, "core_config.json")

	var obs []any
	var tags []string
	for i := range batch {
		n := *batch[i]
		ob := publish.Outbound(&n)
		if ob == nil {
			continue
		}
		obs = append(obs, ob)
		tags = append(tags, n.ID)
	}
	if len(tags) == 0 {
		logf("本批节点参数均不完整，跳过")
		return false
	}
	cfg := map[string]any{
		"log": map[string]any{"level": "warn"},
		"experimental": map[string]any{
			"clash_api": map[string]any{"external_controller": fmt.Sprintf("127.0.0.1:%d", clashAPIPort)},
		},
		"inbounds": []any{map[string]any{
			"type": "mixed", "tag": "mixed-in", "listen": "127.0.0.1", "listen_port": port,
		}},
		"outbounds": append([]any{
			map[string]any{"type": "selector", "tag": "proxy",
				"outbounds": append([]string{"auto"}, tags...), "default": "auto"},
			map[string]any{"type": "urltest", "tag": "auto", "outbounds": tags,
				"url": probeHTTP, "interval": urltestInterval, "tolerance": 50},
		}, append(obs, map[string]any{"type": "direct", "tag": "direct"})...),
		"route": map[string]any{"final": "proxy"},
	}
	b, err := json.MarshalIndent(cfg, "", "  ")
	if err == nil {
		err = os.WriteFile(cfgPath, b, 0o644)
	}
	if err != nil {
		logf("写配置失败: %v", err)
		return false
	}
	if _, err := os.Stat(m.settings.SingBoxPath); err != nil {
		logf("核心程序不存在: %s", m.settings.SingBoxPath)
		m.fail("核心程序不存在，请在设置中检查路径")
		return false
	}
	if c, err := net.DialTimeout("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(port)), 300*time.Millisecond); err == nil {
		c.Close()
		logf("代理端口 %d 被占用，跳过本批", port)
		m.fail("代理端口 %d 被占用，可能有残留核心进程", port)
		return false
	}
	if out, err := exec.Command(m.settings.SingBoxPath, "check", "-c", cfgPath).CombinedOutput(); err != nil {
		logf("配置校验未通过: %s", firstLine(out))
		return false
	}

	cmd := exec.Command(m.settings.SingBoxPath, "run", "-c", cfgPath)
	done := make(chan struct{})
	if err := cmd.Start(); err != nil {
		// 启动失败（任务书第二节：与运行中崩溃区分）
		logf("[CORE] 启动失败: %v", err)
		return false
	}
	m.mu.Lock()
	m.cmd, m.done, m.node, m.exitErr, m.stopped = cmd, done, batch[0], nil, false
	pid := cmd.Process.Pid
	m.mu.Unlock()
	logf("[CORE] 核心已启动 (PID %d)，urltest 组 %d 个节点（周期 %s）", pid, len(tags), urltestInterval)

	// 任务书第二节：唯一 Wait goroutine。其他模块只允许监听 done。
	go func() {
		err := cmd.Wait()
		m.mu.Lock()
		m.exitErr = err
		m.mu.Unlock()
		close(done)
	}()

	if !m.probe(port) {
		select {
		case <-done:
			logf("[CORE] 核心进程在验证期间退出")
		default:
		}
		m.stopProc(cmd, done, logf)
		return false
	}
	if m.aborted() {
		m.stopProc(cmd, done, logf)
		return false
	}
	m.mu.Lock()
	m.phase = PhaseConnected
	m.since = time.Now()
	m.lastError = ""
	m.netState = ""
	m.lastSwitch = time.Now()
	m.mu.Unlock()
	logf("连接成功（内核自动选择最快节点，组内 %d 个候选）", len(tags))
	go m.exitWatch(done, gen, logf)
	go m.infoLoop(port, batch, tags, logf)
	go m.healthLoop(port, batch, tags, logf)
	if m.settings.AutoSysProxy {
		if err := SetSystemProxy(port, filepath.Join(m.dataDir, "sysproxy_backup.json")); err != nil {
			logf("系统代理设置失败: %v（可手动设置系统代理为 127.0.0.1:%d）", err, port)
		} else {
			m.mu.Lock()
			m.sysProxy = true
			m.mu.Unlock()
			logf("已接管系统代理: 127.0.0.1:%d（断开时自动恢复原状）", port)
		}
	}
	return true
}

// probe 在时限内反复探测本地代理端口与外网连通性（多 Probe，≥1/3 即视为通）。
func (m *Manager) probe(port int) bool {
	deadline := time.Now().Add(probeTimeout)
	for time.Now().Before(deadline) {
		if m.aborted() {
			return false
		}
		time.Sleep(probeInterval)
		conn, err := net.DialTimeout("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(port)), time.Second)
		if err != nil {
			continue
		}
		conn.Close()
		okN, _, state := m.multiProbe(port)
		if okN >= 1 {
			m.mu.Lock()
			m.netState = state
			m.mu.Unlock()
			return true
		}
	}
	return false
}

// multiProbe 并行探测全部 Probe 目标，返回成功数/平均延迟/分层状态
// （任务书第八节：2/3 HEALTHY，1/3 DEGRADED，0/3 FAILED）。
func (m *Manager) multiProbe(port int) (okN int, avgLatency int, state string) {
	client := m.proxyClient(port, 8*time.Second)
	type result struct {
		ok bool
		ms int
	}
	ch := make(chan result, len(probeTargets))
	for _, tgt := range probeTargets {
		go func(t string) {
			start := time.Now()
			resp, err := client.Get(t)
			if err != nil {
				ch <- result{}
				return
			}
			_ = resp.Body.Close()
			ch <- result{resp.StatusCode == http.StatusNoContent, int(time.Since(start).Milliseconds())}
		}(tgt)
	}
	sum, n := 0, 0
	for range probeTargets {
		r := <-ch
		if r.ok {
			okN++
			if r.ms > 0 {
				sum += r.ms
				n++
			}
		}
	}
	if n > 0 {
		avgLatency = sum / n
	}
	switch {
	case okN >= 2:
		state = NetHealthy
	case okN == 1:
		state = NetDegraded
	default:
		state = NetFailed
	}
	return okN, avgLatency, state
}

// localNetOK 直连探测本地互联网是否可达（不走代理，任务书第十四节）。
// 本地断网时不惩罚节点，避免 Wi-Fi 抖动导致好节点被冷却。
func localNetOK() bool {
	client := &http.Client{Timeout: 4 * time.Second}
	for _, t := range directTargets {
		resp, err := client.Get(t)
		if err != nil {
			continue
		}
		_ = resp.Body.Close()
		if resp.StatusCode == http.StatusNoContent || resp.StatusCode == http.StatusOK {
			return true
		}
	}
	return false
}

// healthLoop 连接健康监控（任务书第一/二节重构版）：
//
//	多 Probe → HEALTHY 记成功；DEGRADED 只观察不动作；
//	FAILED 先区分本地断网 → urltest 重选窗口再验证 → 仍失败才降分冷却
//	→ 切换组内下一健康节点（核心不重启）→ 整组失败才换批（重启核心）。
func (m *Manager) healthLoop(port int, batch []*model.Node, tags []string, logf func(string, ...any)) {
	gen := m.currentGen()
	byTag := map[string]*model.Node{}
	for i := range batch {
		byTag[batch[i].ID] = batch[i]
	}
	fails := 0             // 连续确认失败轮数
	tried := map[string]bool{} // 本批内已确认失败（不再选）的节点
	for {
		time.Sleep(healthInterval)
		if m.currentGen() != gen || m.phaseNow() != PhaseConnected {
			return
		}
		// 自愈：其他代理客户端可能关掉系统代理开关（实测 v2rayN 会）
		if m.sysProxyOwned() && !SysProxyEnabled() {
			if err := AssertSystemProxy(port); err == nil {
				logf("检测到系统代理被其他程序关闭，已重新接管")
			}
		}
		curID := m.nodeNowID()
		okN, lat, state := m.multiProbe(port)
		m.mu.Lock()
		m.netState = state
		m.mu.Unlock()

		switch state {
		case NetHealthy:
			if fails > 0 || len(tried) > 0 {
				logf("[HEALTH] node=%s probe=%d/%d latency=%dms recovered", curID, okN, len(probeTargets), lat)
			}
			if curID != "" && lat > 0 {
				m.tracker.RecordSuccess(curID, lat)
			}
			m.tracker.Flush()
			fails = 0
			continue
		case NetDegraded:
			// 1/3 可达：网络可用但质量差。只观察，不惩罚不切换
			// （任务书第十五节：不要因为一次 Probe 失败就换节点）。
			logf("[HEALTH] node=%s probe=%d/%d latency=%dms state=DEGRADED（观察，暂不动作）", curID, okN, len(probeTargets), lat)
			if curID != "" && lat > 0 {
				m.tracker.RecordSuccess(curID, lat)
			}
			m.tracker.Flush()
			fails = 0
			continue
		}

		// FAILED：0/3 全部失败
		if !localNetOK() {
			// 本地网络断了 ≠ 节点坏了（任务书第十四节）
			logf("[HEALTH] 本地网络直连探测不可达，暂不惩罚节点 %s", curID)
			continue
		}
		fails++
		switchNow := false
		h, _ := m.tracker.Get(curID)
		logf("[HEALTH] node=%s probe=0/%d failure=%d consecutive=%d", curID, len(probeTargets), h.FailureCount, h.ConsecutiveFailure)

		if fails == 1 {
			// 第一轮全失败：先判断是否暂时异常——给内核 urltest
			// 一个重选窗口（周期 30s），再验证一次
			logf("[HEALTH] 等待 %s 让内核 urltest 重选，随后复验", urltestWait)
			time.Sleep(urltestWait)
			if m.currentGen() != gen || m.phaseNow() != PhaseConnected {
				return
			}
			okN2, lat2, _ := m.multiProbe(port)
			switch {
			case okN2 >= 2:
				id := m.nodeNowID()
				logf("[HEALTH] node=%s probe=%d/%d latency=%dms recovered（urltest 已自动切换）", id, okN2, len(probeTargets), lat2)
				if id != "" && lat2 > 0 {
					m.tracker.RecordSuccess(id, lat2)
					m.tracker.Flush()
				}
				m.mu.Lock()
				m.netState = NetHealthy
				m.mu.Unlock()
				fails = 0
				continue
			case okN2 == 1:
				fails = 0 // 降级但可用，回观察态
				continue
			}
			// 复验仍 0/3：当前节点确认死亡 → 降分（任务书第一节点流程）
			if curID != "" {
				cooled, dur := m.tracker.RecordFailure(curID)
				logf("[SCORE] node=%s score=%d", curID, m.tracker.Score(curID))
				if cooled {
					logf("[COOLDOWN] node=%s duration=%s reason=consecutive_failure", curID, dur.Round(time.Second))
				}
				tried[curID] = true
				m.tracker.Flush()
			}
			switchNow = true
		} else {
			// 复验轮之后仍全失败：当前节点维持死亡判定，继续切换流程
			switchNow = true
		}

		// 切换组内下一健康节点（核心保持运行，任务书第十七节）
		if switchNow {
			if since := time.Since(m.lastSwitchTime()); since < minHold && fails < 3 {
				logf("[HEALTH] 距上次切换 %s < 最短保持 %s，本轮暂不切换", since.Round(time.Second), minHold)
				continue
			}
			next := pickNext(tags, byTag, tried, m.tracker)
			if next != nil {
				if m.switchSelector(next, "node_failed", logf) {
					fails = 0
					continue
				}
			}
			// 组内已无可切换节点 → 整组失败，换批（只有此时才重启核心）
			m.nextBatch("整组候选均失效", logf)
			return
		}
	}
}

// pickNext 按候选顺序（健康分序）选下一个未冷却、未确认失败的节点。
func pickNext(tags []string, byTag map[string]*model.Node, tried map[string]bool, tracker *HealthTracker) *model.Node {
	for _, tag := range tags {
		if tried[tag] || tracker.InCooldown(tag) {
			continue
		}
		if n, ok := byTag[tag]; ok {
			return n
		}
	}
	return nil
}

// switchSelector 通过 clash_api 把 selector 切到指定节点——核心进程
// 保持运行（任务书第一/六节：不因节点问题重启核心）。
func (m *Manager) switchSelector(next *model.Node, reason string, logf func(string, ...any)) bool {
	client := &http.Client{Timeout: 3 * time.Second}
	body := fmt.Sprintf(`{"name":%q}`, next.ID)
	req, err := http.NewRequest(http.MethodPut, fmt.Sprintf("http://127.0.0.1:%d/proxies/proxy", clashAPIPort), strings.NewReader(body))
	if err == nil {
		req.Header.Set("Content-Type", "application/json")
		var resp *http.Response
		resp, err = client.Do(req)
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode >= 300 {
				err = fmt.Errorf("clash_api 返回 %d", resp.StatusCode)
			}
		}
	}
	if err != nil {
		logf("[SWITCH] 切换到 %s 失败: %v", next.ID, err)
		return false
	}
	n := *next
	m.mu.Lock()
	m.node = &n
	m.lastSwitch = time.Now()
	m.mu.Unlock()
	logf("[SWITCH] -> %s reason=%s（核心保持运行，未重启）", next.ID, reason)
	return true
}

// nextBatch 当前候选组整体失败时更换下一批（唯一允许重启核心的节点路径）。
func (m *Manager) nextBatch(reason string, logf func(string, ...any)) {
	logf("[SWITCH] reason=%s，自动更换下一批", reason)
	m.killProc(logf)
	m.mu.Lock()
	m.phase = PhaseConnecting
	m.batchIdx++
	m.gen++
	gen := m.gen
	m.mu.Unlock()
	go m.supervise(gen, logf)
}

// infoLoop 通过内核 clash_api 跟踪当前选中节点并刷新出口信息；
// 检测到大陆出口时惩罚该节点并切换组内其他节点（不重启核心）。
func (m *Manager) infoLoop(port int, batch []*model.Node, tags []string, logf func(string, ...any)) {
	byTag := map[string]*model.Node{}
	for i := range batch {
		byTag[batch[i].ID] = batch[i]
	}
	client := &http.Client{Timeout: 3 * time.Second}
	tick := 0
	for {
		time.Sleep(infoInterval)
		if m.currentGen() != m.currentGenSnap() || m.phaseNow() != PhaseConnected {
			return
		}
		tick++
		tag := clashCurrentTag(client)
		if n, ok := byTag[tag]; ok {
			nn := *n
			m.mu.Lock()
			m.node = &nn
			m.mu.Unlock()
		}
		if tick%3 == 0 {
			pc := m.proxyClient(port, 10*time.Second)
			if r2, err := pc.Get("http://ip-api.com/line/?fields=query,countryCode"); err == nil {
				buf := make([]byte, 256)
				n, _ := r2.Body.Read(buf)
				r2.Body.Close()
				parts := strings.SplitN(string(buf[:n]), "\n", 2)
				code := strings.TrimSpace(parts[0])
				m.mu.Lock()
				m.exitInfo = strings.TrimSpace(string(buf[:n]))
				m.mu.Unlock()
				// 大陆出口无法用于访问外网（且可能伪造探测），立即更换
				if code == "CN" {
					id := tag
					logf("[HEALTH] node=%s probe=geo reason=mainland_exit", id)
					m.tracker.Penalize(id, 10*time.Minute)
					logf("[SCORE] node=%s score=%d", id, m.tracker.Score(id))
					logf("[COOLDOWN] node=%s duration=10m reason=mainland_exit", id)
					m.tracker.Flush()
					if next := pickNext(tags, byTag, map[string]bool{id: true}, m.tracker); next != nil {
						if m.switchSelector(next, "mainland_exit", logf) {
							continue
						}
					}
					m.nextBatch("mainland_exit_整组不可用", logf)
					return
				}
			}
		}
	}
}

// clashCurrentTag 查询当前实际出站节点 tag：先看 selector（可能是手动
// 切换的节点），selector 指向 auto 时再查 urltest 组当前选中者。
func clashCurrentTag(client *http.Client) string {
	var sel struct {
		Now string `json:"now"`
	}
	if resp, err := client.Get(fmt.Sprintf("http://127.0.0.1:%d/proxies/proxy", clashAPIPort)); err == nil {
		_ = json.NewDecoder(resp.Body).Decode(&sel)
		resp.Body.Close()
	}
	if sel.Now == "" || sel.Now == "auto" {
		var auto struct {
			Now string `json:"now"`
		}
		if resp, err := client.Get(fmt.Sprintf("http://127.0.0.1:%d/proxies/auto", clashAPIPort)); err == nil {
			_ = json.NewDecoder(resp.Body).Decode(&auto)
			resp.Body.Close()
		}
		return auto.Now
	}
	return sel.Now
}

// exitWatch 监控核心进程退出（任务书第二节：只监听 done，绝不调用
// cmd.Wait()）。区分主动停止与异常崩溃，只有崩溃才自动重启核心。
func (m *Manager) exitWatch(done chan struct{}, gen int, logf func(string, ...any)) {
	<-done
	m.mu.Lock()
	mine := m.cmd != nil && m.done == done && m.gen == gen
	stopped := m.stopped
	exitErr := m.exitErr
	phase := m.phase
	if mine {
		m.cmd, m.done = nil, nil
	}
	m.mu.Unlock()
	if !mine || stopped {
		return // 已被新一代取代，或主动停止（正常生命周期）
	}
	if phase != PhaseConnected && phase != PhaseConnecting {
		return
	}
	logf("[CORE] sing-box exited unexpectedly err=%v", exitErr)
	// 核心进程自身崩溃 → 重启当前批（节点没有问题，不换批）
	m.mu.Lock()
	m.phase = PhaseConnecting
	m.gen++
	gen2 := m.gen
	m.mu.Unlock()
	logf("[CORE] restarting reason=process_exit")
	go m.supervise(gen2, logf)
}

// Disconnect 断开连接并恢复用户原系统代理。
func (m *Manager) Disconnect(logf func(string, ...any)) error {
	m.mu.Lock()
	phase := m.phase
	m.mu.Unlock()
	if phase == PhaseDisconnected {
		return fmt.Errorf("当前未连接")
	}
	m.mu.Lock() // 先断代+置状态，监督者与监控自行退出
	m.gen++
	m.phase = PhaseDisconnected
	m.stopped = true // 主动停止：exitWatch 不触发重启
	cmd, done := m.cmd, m.done
	m.cmd, m.done = nil, nil
	sysProxy := m.sysProxy
	m.sysProxy = false
	m.mu.Unlock()
	m.tracker.Flush() // 保存健康统计
	if cmd != nil {
		killTree(cmd)
	}
	if done != nil {
		select {
		case <-done:
		case <-time.After(5 * time.Second):
		}
	}
	if sysProxy {
		RestoreSystemProxy(filepath.Join(m.dataDir, "sysproxy_backup.json"))
		logf("系统代理已恢复为用户原设置")
	}
	logf("已断开，端口 %d 恢复", m.settings.ProxyPort)
	return nil
}

func (m *Manager) stopProc(cmd *exec.Cmd, done chan struct{}, logf func(string, ...any)) {
	m.mu.Lock()
	m.stopped = true
	m.mu.Unlock()
	killTree(cmd)
	select {
	case <-done:
	case <-time.After(5 * time.Second):
	}
	m.mu.Lock()
	if m.cmd == cmd {
		m.cmd, m.done = nil, nil
	}
	m.mu.Unlock()
}

func (m *Manager) killProc(logf func(string, ...any)) {
	m.mu.Lock()
	cmd, done := m.cmd, m.done
	m.stopped = true
	m.mu.Unlock()
	if cmd == nil {
		return
	}
	logf("[CORE] 停止核心 (PID %d)", cmd.Process.Pid)
	killTree(cmd)
	if done != nil {
		select {
		case <-done:
		case <-time.After(5 * time.Second):
		}
	}
	m.mu.Lock()
	if m.cmd == cmd {
		m.cmd, m.done = nil, nil
	}
	m.mu.Unlock()
}

func killTree(cmd *exec.Cmd) {
	if cmd == nil || cmd.Process == nil {
		return
	}
	_ = exec.Command("taskkill", "/F", "/T", "/PID", strconv.Itoa(cmd.Process.Pid)).Run()
	_ = cmd.Process.Kill()
}

func (m *Manager) proxyClient(port int, timeout time.Duration) *http.Client {
	return &http.Client{
		Timeout: timeout,
		Transport: &http.Transport{Proxy: func(*http.Request) (*url.URL, error) {
			return url.Parse(fmt.Sprintf("http://127.0.0.1:%d", port))
		}},
	}
}

func (m *Manager) currentGen() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.gen
}

func (m *Manager) currentGenSnap() int { return m.currentGen() }

func (m *Manager) phaseNow() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.phase
}

func (m *Manager) nodeNowID() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.node == nil {
		return ""
	}
	return m.node.ID
}

func (m *Manager) lastSwitchTime() time.Time {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.lastSwitch
}

func (m *Manager) aborted() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.phase == PhaseDisconnected
}

func (m *Manager) sysProxyOwned() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.sysProxy
}

func (m *Manager) fail(format string, args ...any) {
	m.mu.Lock()
	m.phase = PhaseFailed
	m.lastError = fmt.Sprintf(format, args...)
	m.mu.Unlock()
}

func firstLine(b []byte) string {
	for i := range b {
		if b[i] == '\n' {
			return string(b[:i])
		}
	}
	return string(b)
}
