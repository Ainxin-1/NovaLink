// 健康追踪核心逻辑单元测试（NovaLink 2.0 稳定性任务书第四/五/六节）。
package core

import (
	"path/filepath"
	"testing"
	"time"

	"novanode/model"
)

func newTestTracker(t *testing.T) *HealthTracker {
	return NewHealthTracker(filepath.Join(t.TempDir(), "runtime_health.json"))
}

// 冷却阶梯：连续失败 1 次不冷却，2 次 → 5m，之后翻倍封顶 30m。
func TestCooldownEscalation(t *testing.T) {
	tr := newTestTracker(t)
	if cooled, _ := tr.RecordFailure("a"); cooled {
		t.Fatal("第一次失败不应进入冷却")
	}
	if tr.InCooldown("a") {
		t.Fatal("连续失败 1 次不应冷却")
	}
	cooled, dur := tr.RecordFailure("a")
	if !cooled || dur != 5*time.Minute {
		t.Fatalf("连续失败 2 次应冷却 5m，got cooled=%v dur=%v", cooled, dur)
	}
	tr.RecordFailure("a")
	if _, dur = tr.RecordFailure("a"); dur != 20*time.Minute {
		t.Fatalf("连续失败 4 次应冷却 20m，got %v", dur)
	}
	// 大量失败后封顶 30m
	for i := 0; i < 20; i++ {
		cooled, dur = tr.RecordFailure("a")
	}
	if dur != cooldownMax {
		t.Fatalf("冷却应封顶 30m，got %v", dur)
	}
}

// 成功一次即恢复：冷却清除、连续失败清零。
func TestRecoveryClearsCooldown(t *testing.T) {
	tr := newTestTracker(t)
	tr.RecordFailure("a")
	tr.RecordFailure("a")
	if !tr.InCooldown("a") {
		t.Fatal("前置：节点应处于冷却")
	}
	tr.RecordSuccess("a", 120)
	if tr.InCooldown("a") {
		t.Fatal("恢复后不应继续冷却")
	}
	h, _ := tr.Get("a")
	if h.ConsecutiveFailure != 0 || h.ConsecutiveSuccess != 1 {
		t.Fatalf("恢复后计数错误: %+v", h)
	}
}

// 稳定优先于偶尔最快（任务书第五节核心目标）。
func TestStableBeatsOccasionalFast(t *testing.T) {
	tr := newTestTracker(t)
	// 节点 S：连续稳定、低延迟、很少失败
	for i := 0; i < 12; i++ {
		tr.RecordSuccess("stable", 100)
	}
	// 节点 F：延迟更低但反复失败
	tr.RecordSuccess("flappy", 40)
	tr.RecordFailure("flappy")
	tr.RecordSuccess("flappy", 40)
	tr.RecordFailure("flappy")
	tr.RecordSuccess("flappy", 40)
	tr.RecordFailure("flappy")
	tr.RecordFailure("flappy")
	if tr.Score("flappy") >= tr.Score("stable") {
		t.Fatalf("稳定节点(%d)应优于低延迟常失败节点(%d)", tr.Score("stable"), tr.Score("flappy"))
	}
}

// 排序：健康分降序，冷却节点垫底，无数据中性。
func TestSortByScore(t *testing.T) {
	tr := newTestTracker(t)
	tr.RecordSuccess("good", 100)
	tr.RecordSuccess("good", 110)
	tr.RecordSuccess("good", 90)
	tr.RecordSuccess("mid", 300)
	tr.RecordFailure("bad")
	tr.RecordFailure("bad") // 进入冷却
	nodes := []*model.Node{
		{ID: "bad"}, {ID: "unknown"}, {ID: "mid"}, {ID: "good"},
	}
	tr.SortByScore(nodes)
	got := []string{nodes[0].ID, nodes[1].ID, nodes[2].ID, nodes[3].ID}
	if got[0] != "good" || got[1] != "mid" || got[2] != "unknown" || got[3] != "bad" {
		t.Fatalf("排序不符合 稳定>中性>冷却: %v", got)
	}
}

// 大陆出口即时惩罚：直接进入冷却。
func TestPenalize(t *testing.T) {
	tr := newTestTracker(t)
	tr.Penalize("cn", 10*time.Minute)
	if !tr.InCooldown("cn") {
		t.Fatal("Penalize 后应处于冷却")
	}
	h, _ := tr.Get("cn")
	if h.FailureCount != 1 || h.ConsecutiveFailure != 1 {
		t.Fatalf("Penalize 计数错误: %+v", h)
	}
}

// 延迟 EWMA 与抖动统计。
func TestLatencyEWMA(t *testing.T) {
	tr := newTestTracker(t)
	tr.RecordSuccess("a", 100)
	tr.RecordSuccess("a", 100)
	tr.RecordSuccess("a", 100)
	tr.RecordSuccess("a", 200) // 抖动
	h, _ := tr.Get("a")
	if h.AvgLatency <= 100 || h.AvgLatency >= 200 {
		t.Fatalf("EWMA 应介于 100~200，got %v", h.AvgLatency)
	}
	if h.LatencyJitter <= 0 {
		t.Fatalf("应统计到抖动，got %v", h.LatencyJitter)
	}
}

// 持久化往返：重新加载后统计仍在。
func TestPersistence(t *testing.T) {
	path := filepath.Join(t.TempDir(), "runtime_health.json")
	tr := NewHealthTracker(path)
	tr.RecordSuccess("a", 150)
	tr.RecordSuccess("a", 150)
	tr.Flush()
	tr2 := NewHealthTracker(path)
	h, ok := tr2.Get("a")
	if !ok || h.SuccessCount != 2 {
		t.Fatalf("持久化往返失败: ok=%v h=%+v", ok, h)
	}
}
