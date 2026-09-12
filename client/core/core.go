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

const (
	probeInterval   = 1500 * time.Millisecond
	probeTimeout    = 25 * time.Second // 单批连接验证上限
	probeHTTP       = "http://www.gstatic.com/generate_204"
	batchSize       = 8               // 内核 urltest 组的节点数（任务书第十五章：自动选择最快）
	failoverMax     = 5               // 整批全灭时最多再换 5 批
	healthInterval  = 20 * time.Second
	healthFailLimit = 2
	clashAPIPort    = 9095 // 内核 clash_api，用于查询 urltest 当前选中的节点
	infoInterval    = 10 * time.Second
)

// Manager 管理 VPN 核心子进程与连接状态（单连接，串行管理）。
// 连接形态：一个核心进程带一批（≤8）验证可用节点组成 urltest 组，
// 节点失效由内核秒级自动切换；整批全灭时应用层才更换下一批。
type Manager struct {
	mu        sync.Mutex
	settings  *Settings
	phase     string
	node      *model.Node // 当前实际使用的节点（由内核选择，信息循环刷新）
	since     time.Time
	lastError string
	exitInfo  string
	cmd       *exec.Cmd
	done      chan struct{}
	dataDir   string
	sysProxy  bool
	pool      []*model.Node // 候选池快照
	batchIdx  int           // 当前批索引
	gen       int
}

// NewManager 创建核心管理器。
func NewManager(settings *Settings, dataDir string) *Manager {
	return &Manager{settings: settings, phase: PhaseDisconnected, dataDir: dataDir}
}

// SetFailoverPool 设置候选池快照（调用方按可用优先排序）。
func (m *Manager) SetFailoverPool(nodes []*model.Node) {
	m.mu.Lock()
	m.pool, m.batchIdx = nodes, 0
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
	return map[string]any{
		"phase":      m.phase,
		"node":       node,
		"since":      m.since.Format(time.RFC3339),
		"last_error": m.lastError,
		"exit_info":  m.exitInfo,
		"sysproxy":   m.sysProxy,
	}
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
	logf("[调试] supervise 启动 gen=%d", gen)
	attempt := 0
	for {
		m.mu.Lock()
		if m.gen != gen || m.phase != PhaseConnecting && m.phase != PhaseConnected {
			logf("[调试] supervise 退出: m.gen=%d gen=%d phase=%s", m.gen, gen, m.phase)
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
			logf("整批全灭，换下一批(%d/%d)：%d 个节点", attempt, failoverMax+1, len(batches[bi]))
		}
		logf("[调试] supervise 调用 runBatch 批=%d 节点数=%d", bi, len(batches[bi]))
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
				"url": probeHTTP, "interval": "2m", "tolerance": 50},
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
		logf("核心启动失败: %v", err)
		return false
	}
	m.mu.Lock()
	m.cmd, m.done, m.node = cmd, done, batch[0]
	pid := cmd.Process.Pid
	m.mu.Unlock()
	logf("核心已启动 (PID %d)，urltest 组 %d 个节点", pid, len(tags))

	go func() {
		_ = cmd.Wait()
		close(done)
	}()

	if !m.probe(port) {
		select {
		case <-done:
			logf("核心进程提前退出")
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
	m.mu.Unlock()
	logf("连接成功（内核自动选择最快节点，组内 %d 个候选）", len(tags))
	go m.exitWatch(cmd, logf)
	go m.infoLoop(port, batch, logf)
	go m.healthLoop(port, logf)
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

// probe 在时限内反复探测本地代理端口与外网连通性。
func (m *Manager) probe(port int) bool {
	deadline := time.Now().Add(probeTimeout)
	client := m.proxyClient(port, 8*time.Second)
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
		resp, err := client.Get(probeHTTP)
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusNoContent {
				return true
			}
		}
	}
	return false
}

// infoLoop 通过内核 clash_api 跟踪 urltest 当前选中的节点并刷新出口信息。
func (m *Manager) infoLoop(port int, batch []*model.Node, logf func(string, ...any)) {
	byTag := map[string]model.Node{}
	for i := range batch {
		byTag[batch[i].ID] = *batch[i]
	}
	client := &http.Client{Timeout: 3 * time.Second}
	tick := 0
	for {
		time.Sleep(infoInterval)
		if m.currentGen() != m.currentGenSnap() || m.phaseNow() != PhaseConnected {
			return
		}
		tick++
		resp, err := client.Get(fmt.Sprintf("http://127.0.0.1:%d/proxies/auto", clashAPIPort))
		if err == nil {
			var p struct {
				Now string `json:"now"`
			}
			if json.NewDecoder(resp.Body).Decode(&p) == nil && p.Now != "" {
				if n, ok := byTag[p.Now]; ok {
					m.mu.Lock()
					m.node = &n
					m.mu.Unlock()
				}
			}
			resp.Body.Close()
		}
		if tick%3 == 0 {
			pc := m.proxyClient(port, 10*time.Second)
			if r2, err := pc.Get("http://ip-api.com/line/?fields=query,country"); err == nil {
				buf := make([]byte, 256)
				n, _ := r2.Body.Read(buf)
				r2.Body.Close()
				m.mu.Lock()
				m.exitInfo = string(buf[:n])
				m.mu.Unlock()
			}
		}
	}
}

// healthLoop 连接健康监控：整组失效时换下一批候选。
func (m *Manager) healthLoop(port int, logf func(string, ...any)) {
	fails := 0
	client := m.proxyClient(port, 8*time.Second)
	gen := m.currentGen()
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
		resp, err := client.Get(probeHTTP)
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusNoContent {
				fails = 0
				continue
			}
		}
		fails++
		logf("连接健康检查失败（%d/%d）", fails, healthFailLimit)
		if fails < healthFailLimit {
			continue
		}
		logf("整组候选均已失效，自动更换下一批")
		m.killProc(logf)
		m.mu.Lock()
		m.phase = PhaseConnecting
		m.batchIdx++
		m.gen++
		gen2 := m.gen
		m.mu.Unlock()
		go m.supervise(gen2, logf)
		return
	}
}

// exitWatch 监控核心进程意外退出。
func (m *Manager) exitWatch(cmd *exec.Cmd, logf func(string, ...any)) {
	_ = cmd.Wait()
	m.mu.Lock()
	mine := m.cmd == cmd
	phase := m.phase
	m.mu.Unlock()
	if !mine {
		return
	}
	if phase == PhaseConnected || phase == PhaseConnecting {
		logf("核心进程退出，自动更换候选批次")
		m.killProc(logf)
		m.mu.Lock()
		m.phase = PhaseConnecting
		m.batchIdx++
		m.gen++
		gen := m.gen
		m.mu.Unlock()
		go m.supervise(gen, logf)
	}
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
	cmd, done := m.cmd, m.done
	m.cmd, m.done = nil, nil
	sysProxy := m.sysProxy
	m.sysProxy = false
	m.mu.Unlock()
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
	m.mu.Unlock()
	if cmd == nil {
		return
	}
	logf("停止核心 (PID %d)", cmd.Process.Pid)
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
