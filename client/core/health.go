// 节点运行时健康追踪（NovaLink 2.0 稳定性任务书第四/五/六节）。
//
// 客户端本地观察各节点的真实表现：成功/失败计数、连续次数、
// 平均延迟与抖动（EWMA）、最近成功/失败时间、综合健康分、冷却期。
// 数据持久化到客户端数据目录 runtime_health.json，重启后保留历史，
// 实现"稳定优先于偶尔最快"的节点选择。
//
// 注意：这里只做客户端侧统计，不改动 node/ 管线的 pool.json 结构。
package core

import (
	"encoding/json"
	"os"
	"sort"
	"sync"
	"time"

	"novanode/model"
)

// NodeHealth 单节点运行时健康统计（任务书第四节字段定义）。
type NodeHealth struct {
	ID                 string    `json:"id"`
	SuccessCount       int       `json:"success_count"`        // 总成功次数
	FailureCount       int       `json:"failure_count"`        // 总失败次数
	ConsecutiveSuccess int       `json:"consecutive_success"`  // 连续成功次数
	ConsecutiveFailure int       `json:"consecutive_failure"`  // 连续失败次数
	AvgLatency         float64   `json:"avg_latency_ms"`       // 平均延迟（EWMA）
	LatencyJitter      float64   `json:"latency_jitter_ms"`    // 延迟波动（EWMA |Δ|）
	LastFailure        time.Time `json:"last_failure,omitempty"`
	LastSuccess        time.Time `json:"last_success,omitempty"`
	HealthScore        int       `json:"health_score"`      // 综合健康分 0-100
	CooldownUntil      time.Time `json:"cooldown_until,omitempty"`
}

const (
	ewmaAlpha         = 0.3               // 延迟 EWMA 平滑系数
	cooldownBase      = 5 * time.Minute   // 首次冷却时长（任务书第六节示例）
	cooldownMax       = 30 * time.Minute  // 冷却上限（反复失败逐次翻倍）
	cooldownThreshold = 2                 // 连续失败达到该次数进入冷却
	neutralScore      = 55                // 无数据节点的中性分
)

// HealthTracker 全体节点的健康统计表（并发安全）。
type HealthTracker struct {
	mu    sync.Mutex
	nodes map[string]*NodeHealth
	path  string // 持久化文件路径
	dirty bool
}

// NewHealthTracker 创建并从磁盘恢复健康追踪器。
func NewHealthTracker(path string) *HealthTracker {
	t := &HealthTracker{nodes: map[string]*NodeHealth{}, path: path}
	if b, err := os.ReadFile(path); err == nil {
		var list []*NodeHealth
		if json.Unmarshal(b, &list) == nil {
			for _, h := range list {
				if h != nil && h.ID != "" {
					t.nodes[h.ID] = h
				}
			}
		}
	}
	return t
}

// RecordSuccess 记录一次成功（含延迟），清除冷却并重算健康分。
func (t *HealthTracker) RecordSuccess(id string, latencyMS int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	h := t.get(id)
	h.SuccessCount++
	h.ConsecutiveSuccess++
	h.ConsecutiveFailure = 0
	now := time.Now()
	h.LastSuccess = now
	if h.CooldownUntil.After(now) {
		h.CooldownUntil = time.Time{} // 恢复即重新加入候选池（任务书第六节）
	}
	if latencyMS > 0 {
		if h.AvgLatency <= 0 {
			h.AvgLatency = float64(latencyMS)
		} else {
			j := float64(latencyMS) - h.AvgLatency
			if j < 0 {
				j = -j
			}
			h.LatencyJitter = ewmaAlpha*j + (1-ewmaAlpha)*h.LatencyJitter
			h.AvgLatency = ewmaAlpha*float64(latencyMS) + (1-ewmaAlpha)*h.AvgLatency
		}
	}
	h.HealthScore = computeScore(h)
	t.dirty = true
}

// RecordFailure 记录一次失败，重算健康分；连续失败达到阈值时进入冷却
// 并返回 true（任务书第六节：降分 → Cooldown）。
func (t *HealthTracker) RecordFailure(id string) (cooled bool, duration time.Duration) {
	t.mu.Lock()
	defer t.mu.Unlock()
	h := t.get(id)
	h.FailureCount++
	h.ConsecutiveFailure++
	h.ConsecutiveSuccess = 0
	h.LastFailure = time.Now()
	if h.ConsecutiveFailure >= cooldownThreshold {
		// 反复失败冷却翻倍：5m → 10m → 20m → 30m(封顶)；逐次翻倍避免大数移位溢出
		duration = cooldownBase
		for i := 0; i < h.ConsecutiveFailure-cooldownThreshold && duration < cooldownMax; i++ {
			duration *= 2
		}
		if duration > cooldownMax {
			duration = cooldownMax
		}
		h.CooldownUntil = time.Now().Add(duration)
		cooled = true
	}
	h.HealthScore = computeScore(h)
	t.dirty = true
	return cooled, duration
}

// Penalize 立即冷却惩罚（用于大陆出口等确定性违规，任务书第六节
// "不删除节点，冷却后到期重新检测，恢复则重回候选池"）。
func (t *HealthTracker) Penalize(id string, d time.Duration) {
	t.mu.Lock()
	defer t.mu.Unlock()
	h := t.get(id)
	h.FailureCount++
	h.ConsecutiveFailure++
	h.ConsecutiveSuccess = 0
	h.LastFailure = time.Now()
	h.CooldownUntil = time.Now().Add(d)
	h.HealthScore = computeScore(h)
	t.dirty = true
}

// Get 返回节点健康副本；无记录时第二返回值为 false。
func (t *HealthTracker) Get(id string) (NodeHealth, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if h, ok := t.nodes[id]; ok {
		return *h, true
	}
	return NodeHealth{}, false
}

// Score 返回节点健康分；无数据返回中性分。
func (t *HealthTracker) Score(id string) int {
	t.mu.Lock()
	defer t.mu.Unlock()
	if h, ok := t.nodes[id]; ok {
		return h.HealthScore
	}
	return neutralScore
}

// InCooldown 节点是否处于冷却期。
func (t *HealthTracker) InCooldown(id string) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if h, ok := t.nodes[id]; ok {
		return h.CooldownUntil.After(time.Now())
	}
	return false
}

// SortByScore 健康分优先排序（任务书第七/十二节）：
// 非冷却节点按分数降序在前，冷却节点垫底；原地排序。
func (t *HealthTracker) SortByScore(nodes []*model.Node) {
	t.mu.Lock()
	scores := make(map[string]int, len(nodes))
	for _, n := range nodes {
		if h, ok := t.nodes[n.ID]; ok {
			scores[n.ID] = h.HealthScore
		} else {
			scores[n.ID] = neutralScore
		}
	}
	t.mu.Unlock()
	sort.SliceStable(nodes, func(i, j int) bool {
		a, b := nodes[i], nodes[j]
		ca, cb := t.InCooldown(a.ID), t.InCooldown(b.ID)
		if ca != cb {
			return !ca // 冷却垫底
		}
		if scores[a.ID] != scores[b.ID] {
			return scores[a.ID] > scores[b.ID]
		}
		// 同分再看延迟：低的优先（仅作次级参考）
		return a.LatencyMS < b.LatencyMS
	})
}

// Snapshot 返回全体统计副本（供状态接口展示）。
func (t *HealthTracker) Snapshot() []NodeHealth {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make([]NodeHealth, 0, len(t.nodes))
	for _, h := range t.nodes {
		out = append(out, *h)
	}
	return out
}

// Flush 将统计落盘（脏时才写）。
func (t *HealthTracker) Flush() {
	t.mu.Lock()
	if !t.dirty {
		t.mu.Unlock()
		return
	}
	list := make([]*NodeHealth, 0, len(t.nodes))
	for _, h := range t.nodes {
		list = append(list, h)
	}
	t.dirty = false
	t.mu.Unlock()
	b, err := json.MarshalIndent(list, "", "  ")
	if err != nil {
		return
	}
	_ = os.WriteFile(t.path, b, 0o644)
}

func (t *HealthTracker) get(id string) *NodeHealth {
	h, ok := t.nodes[id]
	if !ok {
		h = &NodeHealth{ID: id, HealthScore: neutralScore}
		t.nodes[id] = h
	}
	return h
}

// computeScore 综合健康分（任务书第五节，稳定优先于偶尔最快）：
//
//	score = 基础 35
//	       + 成功率奖励（最高 +30）
//	       + 连续稳定奖励（最高 +10）
//	       + 延迟奖励（最高 +15）
//	       - 抖动惩罚（最高 -10）
//	       - 失败惩罚（连续失败 + 最近失败加重）
//
// 结果 clamp 到 [0, 100]。
func computeScore(h *NodeHealth) int {
	score := 35
	// 成功率：样本不足(<3)按中性 0.5 计，避免新节点被误判
	total := h.SuccessCount + h.FailureCount
	rate := 0.5
	if total >= 3 {
		rate = float64(h.SuccessCount) / float64(total)
	}
	score += int(rate * 30)
	// 连续稳定：连续成功 10 次拿满
	stab := h.ConsecutiveSuccess
	if stab > 10 {
		stab = 10
	}
	score += stab
	// 延迟奖励
	switch {
	case h.AvgLatency <= 0:
		score += 7 // 无数据中性
	case h.AvgLatency < 200:
		score += 15
	case h.AvgLatency < 500:
		score += 10
	case h.AvgLatency < 800:
		score += 5
	}
	// 抖动惩罚
	if jit := int(h.LatencyJitter / 10); jit > 10 {
		score -= 10
	} else {
		score -= jit
	}
	// 失败惩罚：连续失败每次 -12
	score -= h.ConsecutiveFailure * 12
	// 最近 10 分钟内失败过，再扣 8
	if !h.LastFailure.IsZero() && time.Since(h.LastFailure) < 10*time.Minute {
		score -= 8
	}
	if score < 0 {
		score = 0
	}
	if score > 100 {
		score = 100
	}
	return score
}
