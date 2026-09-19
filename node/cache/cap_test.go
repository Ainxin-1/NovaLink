package cache

import (
	"testing"

	"novanode/model"
)

func node(id, state, firstSeen, lastSuccess string) *model.Node {
	return &model.Node{ID: id, State: state, FirstSeen: firstSeen, LastSuccess: lastSuccess}
}

// TestCapKeepsValuableNodes 池子封顶必须按价值淘汰：验证过的、成功过的、新面孔
// 都要比"判死的旧节点"和"已过期"更值得留下。
func TestCapKeepsValuableNodes(t *testing.T) {
	p := &model.Pool{Nodes: []*model.Node{
		node("dead-old", model.StateFailed, "2026-01-01T00:00:00Z", ""),
		node("expired", model.StateExpired, "2026-01-02T00:00:00Z", ""),
		node("new-stale", model.StateNew, "2026-09-01T00:00:00Z", ""),
		node("new-fresh", model.StateNew, "2026-09-19T00:00:00Z", ""),
		node("proven", model.StateNew, "2026-09-01T00:00:00Z", "2026-09-18T00:00:00Z"),
		node("avail", model.StateAvailable, "2026-09-01T00:00:00Z", "2026-09-19T00:00:00Z"),
	}}
	dropped := Cap(p, 3)
	if dropped != 3 {
		t.Fatalf("淘汰 %d 个，期望 3", dropped)
	}
	left := map[string]bool{}
	for _, n := range p.Nodes {
		left[n.ID] = true
	}
	for _, want := range []string{"avail", "proven", "new-fresh"} {
		if !left[want] {
			t.Errorf("%s 应保留（价值高于被淘汰者），实得 %v", want, left)
		}
	}
	for _, no := range []string{"dead-old", "expired", "new-stale"} {
		if left[no] {
			t.Errorf("%s 应被淘汰", no)
		}
	}
}

// TestCapNoopUnderLimit 未超上限时不得动池子（包括不得重排出副作用）。
func TestCapNoopUnderLimit(t *testing.T) {
	p := &model.Pool{Nodes: []*model.Node{node("a", model.StateFailed, "", ""), node("b", model.StateNew, "", "")}}
	if got := Cap(p, 10); got != 0 {
		t.Errorf("未超上限却淘汰了 %d 个", got)
	}
	if len(p.Nodes) != 2 || p.Nodes[0].ID != "a" {
		t.Errorf("池子被改动: %+v", p.Nodes)
	}
	if Cap(p, 0) != 0 {
		t.Errorf("maxNodes=0 应表示不限")
	}
}
