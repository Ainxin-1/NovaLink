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
	probeTimeout    = 25 * time.Second // 单节点连接验证上限
	probeHTTP       = "http://www.gstatic.com/generate_204"
	failoverMax     = 5               // 任务书第十五章 v1：失败自动换节点，最多连续尝试 5 个
	healthInterval  = 20 * time.Second
	healthFailLimit = 2
)

// Manager 管理 VPN 核心子进程与连接状态（单连接，串行管理）。
type Manager struct {
	mu        sync.Mutex
	settings  *Settings
	phase     string
	node      *model.Node
	since     time.Time
	lastError string
	exitInfo  string
	cmd       *exec.Cmd
	done      chan struct{}
	dataDir   string
	sysProxy  bool          // 已接管系统代理，断开时需恢复
	pool      []*model.Node // 自动换节点的候选（连接时快照，当前节点在最前）
	poolIdx   int
	gen       int // 生命周期代号，防跨代误操作
}

// NewManager 创建核心管理器。
func NewManager(settings *Settings, dataDir string) *Manager {
	return &Manager{settings: settings, phase: PhaseDisconnected, dataDir: dataDir}
}

// SetFailoverPool 设置自动换节点候选列表（当前选中节点须在最前）。
func (m *Manager) SetFailoverPool(nodes []*model.Node) {
	m.mu.Lock()
	m.pool, m.poolIdx = nodes, 0
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

// Connect 发起连接。启动与验证在后台进行，失败时自动换下一个节点。
func (m *Manager) Connect(n *model.Node, logf func(string, ...any)) error {
	m.mu.Lock()
	if m.phase == PhaseConnecting || m.phase == PhaseConnected {
		m.mu.Unlock()
		return fmt.Errorf("已有连接在进行，请先断开")
	}
	m.node = n
	m.phase = PhaseConnecting
	m.lastError = ""
	m.since = time.Now()
	m.gen++ // 新一代生命周期
	gen := m.gen
	m.mu.Unlock()
	logf("发起连接: %s/%s:%d", n.Protocol, n.Server, n.Port)
	go m.supervise(gen, logf)
	return nil
}

// supervise 是一代连接生命周期的监督者：启动当前节点，失败自动换下一个，
// 成功后交给健康监控；健康监控判定失效也回到这里继续换。
func (m *Manager) supervise(gen int, logf func(string, ...any)) {
	attempt := 0
	for {
		m.mu.Lock()
		if m.gen != gen || m.phase != PhaseConnecting && m.phase != PhaseConnected {
			m.mu.Unlock()
			return
		}
		idx := m.poolIdx
		pool := m.pool
		m.mu.Unlock()
		if pool == nil || idx >= len(pool) {
			break
		}
		node := pool[idx]
		if attempt >= failoverMax+1 {
			break
		}
		attempt++
		if idx > 0 {
			logf("自动换节点(%d/%d): %s/%s:%d", attempt, failoverMax+1, node.Protocol, node.Server, node.Port)
		}
		phase, ok := m.runOne(node, logf)
		if !ok {
			// runOne 内部已处理停止；用户主动断开时直接退出
			m.mu.Lock()
			aborted := m.phase == PhaseDisconnected || m.gen != gen
			m.poolIdx++
			m.mu.Unlock()
			if aborted {
				return
			}
			continue
		}
		_ = phase
		return // runOne 内部已进入健康监控或已代际更替
	}
	m.fail("连续 %d 个节点均连接失败，请稍后重试或刷新节点", failoverMax)
	logf("自动换节点耗尽候选")
	// 连接最终失败：必须归还系统代理，否则用户浏览器指向无监听端口
	m.mu.Lock()
	sp := m.sysProxy
	m.sysProxy = false
	m.mu.Unlock()
	if sp {
		RestoreSystemProxy(filepath.Join(m.dataDir, "sysproxy_backup.json"))
		logf("系统代理已恢复为用户原设置")
	}
}

// runOne 启动单个节点并阻塞验证直到：连上（进入健康监控）/失败/用户断开。
func (m *Manager) runOne(node *model.Node, logf func(string, ...any)) (string, bool) {
	port := m.settings.ProxyPort
	cfgPath := filepath.Join(m.dataDir, "core_config.json")
	ob := publish.Outbound(node)
	if ob == nil {
		m.mu.Lock()
		m.lastError = "节点参数不完整"
		m.mu.Unlock()
		return "", false
	}
	cfg := map[string]any{
		"log":       map[string]any{"level": "warn"},
		"inbounds":  []any{map[string]any{"type": "mixed", "tag": "mixed-in", "listen": "127.0.0.1", "listen_port": port}},
		"outbounds": []any{ob, map[string]any{"type": "direct", "tag": "direct"}},
		"route":     map[string]any{"final": node.ID},
	}
	if b, err := json.MarshalIndent(cfg, "", "  "); err == nil {
		if err := os.WriteFile(cfgPath, b, 0o644); err != nil {
			logf("写配置失败: %v", err)
			return "", false
		}
	}
	if _, err := os.Stat(m.settings.SingBoxPath); err != nil {
		m.fail("核心程序不存在，请在设置中检查路径")
		return "", false
	}
	if c, err := net.DialTimeout("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(port)), 300*time.Millisecond); err == nil {
		c.Close()
		m.fail("代理端口 %d 被占用，可能有残留核心进程", port)
		return "", false
	}
	if out, err := exec.Command(m.settings.SingBoxPath, "check", "-c", cfgPath).CombinedOutput(); err != nil {
		logf("配置校验未通过: %s", firstLine(out))
		return "", false
	}

	cmd := exec.Command(m.settings.SingBoxPath, "run", "-c", cfgPath)
	done := make(chan struct{})
	if err := cmd.Start(); err != nil {
		logf("核心启动失败: %v", err)
		return "", false
	}
	m.mu.Lock()
	m.cmd, m.done, m.node = cmd, done, node
	pid := cmd.Process.Pid
	m.mu.Unlock()
	logf("核心已启动 (PID %d)，节点 %s/%s:%d", pid, node.Protocol, node.Server, node.Port)

	go func() {
		_ = cmd.Wait()
		close(done)
	}()

	ok := m.probe(port)
	select {
	case <-done:
		// 进程已退出（被动）
		m.mu.Lock()
		still := m.cmd == cmd
		if still {
			m.cmd, m.done = nil, nil
		}
		m.mu.Unlock()
		logf("核心进程提前退出")
		return "", false
	default:
	}
	if !ok {
		m.stopProc(cmd, done, logf)
		return "", false
	}
	if m.aborted() {
		m.stopProc(cmd, done, logf)
		return "", false
	}
	// 连接成功
	m.mu.Lock()
	m.phase = PhaseConnected
	m.since = time.Now()
	m.lastError = ""
	m.mu.Unlock()
	logf("连接成功: %s (%s:%d)", node.Name, node.Server, node.Port)
	go m.exitWatch(cmd, logf)
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
	go m.fetchExitInfo(port)
	return PhaseConnected, true
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
			continue // 核心可能仍在初始化
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

// healthLoop 连接健康监控：连续失败达到阈值自动换下一个节点。
func (m *Manager) healthLoop(port int, logf func(string, ...any)) {
	fails := 0
	client := m.proxyClient(port, 8*time.Second)
	gen := m.currentGen()
	for {
		time.Sleep(healthInterval)
		if m.currentGen() != gen || m.phaseNow() != PhaseConnected {
			return
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
		logf("当前节点已失效，自动更换")
		m.killProc(logf)
		m.mu.Lock()
		m.phase = PhaseConnecting
		m.poolIdx++
		gen2 := m.gen + 1
		m.gen = gen2
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
		logf("核心进程退出，自动更换节点")
		m.killProc(logf)
		m.mu.Lock()
		m.phase = PhaseConnecting
		m.poolIdx++
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

func (m *Manager) fetchExitInfo(port int) {
	client := m.proxyClient(port, 10*time.Second)
	if r2, err := client.Get("http://ip-api.com/line/?fields=query,country"); err == nil {
		buf := make([]byte, 256)
		n, _ := r2.Body.Read(buf)
		r2.Body.Close()
		m.mu.Lock()
		m.exitInfo = string(buf[:n])
		m.mu.Unlock()
	}
}

func (m *Manager) currentGen() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.gen
}

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
