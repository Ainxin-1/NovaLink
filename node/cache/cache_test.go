package cache

import (
	"testing"
	"time"

	"novanode/checker"
	"novanode/model"
)

func mkNode(id, state string, failCount int, lastSuccess string) *model.Node {
	return &model.Node{
		ID:          id,
		State:       state,
		FailCount:   failCount,
		LastSuccess: lastSuccess,
		LastChecked: time.Now().UTC().Format(time.RFC3339),
		LastUpdated: time.Now().UTC().Format(time.RFC3339),
	}
}

// NEW 节点从未成功过时，连续失败到 NewFailLimit 就该判 FAILED，
// 不能像旧规则那样永远卡在 NEW（那会积压上万个僵尸节点）。
func TestNewNodeFailsFast(t *testing.T) {
	p := &model.Pool{Nodes: []*model.Node{
		mkNode("new1", model.StateNew, 0, ""),
	}}
	// 第一次失败：NEW 未成功过，阈值=NewFailLimit=2，还不到
	res := map[string]checker.Result{"new1": {ID: "new1", OK: false}}
	_, _, failed := ApplyCheck(p, res, 800)
	if failed != 0 {
		t.Fatalf("第1次失败不应判死，实际 failed=%d", failed)
	}
	if p.Nodes[0].State == model.StateFailed {
		t.Fatal("第1次失败不应进 FAILED")
	}
	// 第二次失败：达到 NewFailLimit=2 -> FAILED
	ApplyCheck(p, res, 800)
	if p.Nodes[0].State != model.StateFailed {
		t.Fatalf("第2次失败应进 FAILED，实际 %s (fail=%d)",
			p.Nodes[0].State, p.Nodes[0].FailCount)
	}
}

// 曾经成功过的节点容错更高：要连续失败 FailLimit(3) 次才判死。
func TestProvenNodeToleratesMore(t *testing.T) {
	ok := time.Now().UTC().Add(-time.Hour).Format(time.RFC3339)
	p := &model.Pool{Nodes: []*model.Node{
		mkNode("proven", model.StateAvailable, 0, ok),
	}}
	res := map[string]checker.Result{"proven": {ID: "proven", OK: false}}
	for i := 1; i <= 2; i++ {
		ApplyCheck(p, res, 800)
		if p.Nodes[0].State == model.StateFailed {
			t.Fatalf("第%d次失败不该判死（曾成功过的节点阈值是 %d）", i, FailLimit)
		}
	}
	ApplyCheck(p, res, 800)
	if p.Nodes[0].State != model.StateFailed {
		t.Fatalf("第3次失败应判死，实际 %s", p.Nodes[0].State)
	}
}

// 僵尸节点应被 prune 清除；从未成功但失败次数未达上限的节点要保留（还在观察期）。
func TestPruneZombies(t *testing.T) {
	p := &model.Pool{Nodes: []*model.Node{
		mkNode("zombie1", model.StateNew, NewFailLimit, ""),      // 僵尸：从未成功 + 失败到上限
		mkNode("zombie2", model.StateFailed, NewFailLimit+5, ""), // 僵尸：同上
		mkNode("watch", model.StateNew, 1, ""),                   // 观察期：只失败 1 次
		mkNode("good", model.StateAvailable, 0, time.Now().UTC().Format(time.RFC3339)),
	}}
	if got := ZombieCount(p); got != 2 {
		t.Fatalf("僵尸数应为 2，实际 %d", got)
	}
	left := PruneZombies(p)
	if left != 2 {
		t.Fatalf("清理后应剩 2 个，实际 %d", left)
	}
	ids := map[string]bool{}
	for _, n := range p.Nodes {
		ids[n.ID] = true
	}
	if ids["zombie1"] || ids["zombie2"] {
		t.Fatal("僵尸节点应被清除")
	}
	if !ids["watch"] || !ids["good"] {
		t.Fatal("观察期与可用节点不应被清除")
	}
	if ZombieCount(p) != 0 {
		t.Fatal("清理后僵尸数应为 0（幂等）")
	}
}

// 从未成功过的 NEW 节点不该被发布（旧规则会全发，导致订阅塞满没验证的节点）。
func TestPublishableRejectsUnproven(t *testing.T) {
	nowS := time.Now().UTC().Format(time.RFC3339)
	p := &model.Pool{Nodes: []*model.Node{
		mkNode("avail", model.StateAvailable, 0, nowS),
		mkNode("unproven", model.StateNew, 0, ""), // 从未成功 -> 不发布
		mkNode("proven", model.StateNew, 0, nowS), // 成功过 -> 可发布
		mkNode("dead", model.StateFailed, 3, ""),  // FAILED -> 不发布
	}}
	got := Publishable(p)
	ids := map[string]bool{}
	for _, n := range got {
		ids[n.ID] = true
	}
	if !ids["avail"] {
		t.Error("AVAILABLE 应发布")
	}
	if !ids["proven"] {
		t.Error("成功过的 NEW 应发布")
	}
	if ids["unproven"] {
		t.Error("从未成功过的 NEW 不应发布")
	}
	if ids["dead"] {
		t.Error("FAILED 不应发布")
	}
}

// AVAILABLE 超过 KeepAvailable 上限时，应把延迟差的那批降级为 DEGRADED。
func TestTrimAvailableKeepsFastest(t *testing.T) {
	nodes := []*model.Node{}
	for i := 0; i < KeepAvailable+50; i++ {
		n := mkNode("n"+string(rune('a'+i%26))+string(rune('0'+i/26%10))+string(rune('0'+i/260)),
			model.StateAvailable, 0, time.Now().UTC().Format(time.RFC3339))
		n.LatencyMS = 100 + i // 越靠后越慢
		nodes = append(nodes, n)
	}
	p := &model.Pool{Nodes: nodes}
	trimAvailable(p)

	av, dg := 0, 0
	maxLatInAv := 0
	for _, n := range p.Nodes {
		switch n.State {
		case model.StateAvailable:
			av++
			if n.LatencyMS > maxLatInAv {
				maxLatInAv = n.LatencyMS
			}
		case model.StateDegraded:
			dg++
		}
	}
	if av != KeepAvailable {
		t.Fatalf("AVAILABLE 应压到 %d，实际 %d", KeepAvailable, av)
	}
	if dg != 50 {
		t.Fatalf("应降级 50 个，实际 %d", dg)
	}
	// 保留的应该是延迟最优的那批
	if maxLatInAv > 100+KeepAvailable {
		t.Fatalf("保留的延迟不应超过 %d，实际最大 %d", 100+KeepAvailable, maxLatInAv)
	}
}

// FAILED 且久无成功记录（超过 DeadDays）的节点，即使来源仍在出现也应清出。
func TestAgeRemovesStaleFailed(t *testing.T) {
	old := time.Now().UTC().Add(-(DeadDays + 1) * 24 * time.Hour).Format(time.RFC3339)
	fresh := time.Now().UTC().Format(time.RFC3339)
	p := &model.Pool{Nodes: []*model.Node{
		mkNode("stale", model.StateFailed, 3, old),
		mkNode("recent", model.StateFailed, 3, fresh),
	}}
	// 两个都还在来源里出现
	seen := map[string]bool{"stale": true, "recent": true}
	_, removed := Age(p, seen)
	if removed != 1 {
		t.Fatalf("应移除 1 个（久无成功的 FAILED），实际 %d", removed)
	}
	if len(p.Nodes) != 1 || p.Nodes[0].ID != "recent" {
		t.Fatalf("应保留 recent，实际剩 %v", p.Nodes)
	}
}
