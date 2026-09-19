package publish

import (
	"fmt"
	"testing"

	"novanode/model"
)

func poolNode(id, state, firstSeen string, lat int) *model.Node {
	return &model.Node{ID: id, State: state, FirstSeen: firstSeen, LatencyMS: lat,
		Name: id, Protocol: "vless", Server: "203.0.113.9", Port: 443}
}

// id16 生成互不相同的 16 位假指纹，避免切片越界与 id 撞车之类的测试噪声。
func id16(n int) string { return fmt.Sprintf("%016x", n) }

// TestSlimPoolBoundsAndExcludesDead 精简池必须：封顶、不含判死/过期节点、
// 且把已发布节点全部留住（客户端下载的就是这份，几十 MB 的全量池它解析不动）。
func TestSlimPoolBoundsAndExcludesDead(t *testing.T) {
	pool := &model.Pool{Updated: "2026-09-19T00:00:00Z"}
	for i := 0; i < 50; i++ {
		id := id16(i)
		switch i % 5 {
		case 0:
			pool.Nodes = append(pool.Nodes, poolNode(id, model.StateAvailable, "2026-09-19T00:00:00Z", 300+i))
		case 1:
			pool.Nodes = append(pool.Nodes, poolNode(id, model.StateNew, "2026-09-19T00:00:00Z", 0))
		case 2:
			pool.Nodes = append(pool.Nodes, poolNode(id, model.StateFailed, "2026-09-18T00:00:00Z", 0))
		case 3:
			pool.Nodes = append(pool.Nodes, poolNode(id, model.StateExpired, "2026-09-01T00:00:00Z", 0))
		default:
			pool.Nodes = append(pool.Nodes, poolNode(id, model.StateDegraded, "2026-09-17T00:00:00Z", 1200))
		}
	}
	published := []*model.Node{}
	for _, n := range pool.Nodes {
		if n.State == model.StateAvailable || n.State == model.StateDegraded {
			published = append(published, n)
		}
	}
	slim := SlimPool(pool, published, 12)
	if len(slim.Nodes) != 12 {
		t.Fatalf("精简池 %d 个，期望封顶 12", len(slim.Nodes))
	}
	if slim.Updated != pool.Updated {
		t.Errorf("Updated 未带过来: %q", slim.Updated)
	}
	for _, n := range slim.Nodes {
		switch n.State {
		case model.StateFailed, model.StateExpired, model.StateRemoved:
			t.Errorf("不该出现 %s 节点 %s", n.State, n.ID)
		}
	}
	// 已发布的一旦进入就必须全在（12 > published 数量时）
	if len(published) <= 12 {
		in := map[string]bool{}
		for _, n := range slim.Nodes {
			in[n.ID] = true
		}
		for _, n := range published {
			if !in[n.ID] {
				t.Errorf("已发布节点 %s 被挤出精简池", n.ID)
			}
		}
	}
}

// TestSlimPoolPrefersFreshWhenCutting 名额不够时，更新的候选要赢过旧的
// （免费节点时效以小时计，旧的几乎必然已死）。
func TestSlimPoolPrefersFreshWhenCutting(t *testing.T) {
	pool := &model.Pool{Nodes: []*model.Node{
		poolNode("aaaaaaaaaaaaaaaa", model.StateNew, "2026-09-10T00:00:00Z", 0),
		poolNode("bbbbbbbbbbbbbbbb", model.StateNew, "2026-09-19T00:00:00Z", 0),
		poolNode("cccccccccccccccc", model.StateNew, "2026-09-15T00:00:00Z", 0),
	}}
	slim := SlimPool(pool, nil, 2)
	in := map[string]bool{}
	for _, n := range slim.Nodes {
		in[n.ID] = true
	}
	if !in["bbbbbbbbbbbbbbbb"] || !in["cccccccccccccccc"] {
		t.Errorf("应保留最新的两个，实得 %v", in)
	}
	if in["aaaaaaaaaaaaaaaa"] {
		t.Errorf("最旧的节点不该占名额")
	}
}
