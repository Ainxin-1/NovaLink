package core

import (
	"fmt"
	"novanode/model"
	"path/filepath"
	"testing"
	"time"
)

func pn(id string) *model.Node {
	return &model.Node{ID: id, Name: id, Protocol: "vless", Server: "203.0.113.9", Port: 443, State: model.StateAvailable}
}

// TestGateOrdersByMeasuredLatency 闸门把超线节点排到合格节点之后；
// 未达保底数（3）时才用次快的实测可用节点补位，未测节点永不补位。
func TestGateOrdersByMeasuredLatency(t *testing.T) {
	p := NewLocalProbe(filepath.Join(t.TempDir(), "probe.json"))
	fast := pn("fast")
	slow := pn("slow")
	mid := pn("mid")
	unknown := pn("unknown")
	now := time.Now()
	p.rec = map[string]probeRecord{
		"fast": {OK: true, LatencyMS: 450, At: now},
		"mid":  {OK: true, LatencyMS: 790, At: now},
		"slow": {OK: true, LatencyMS: 6768, At: now},
		"dead": {OK: false, At: now},
	}
	nodes := []*model.Node{slow, mid, fast, unknown, pn("dead")}
	kept, over := p.Gate(nodes, 800)
	if over != 1 {
		t.Errorf("应记 1 个超线，实得 %d", over)
	}
	if len(kept) < 2 || kept[0].ID != "fast" || kept[1].ID != "mid" {
		t.Errorf("前两名必须是实测最快的两个，实得 %v", ids(kept))
	}
	for _, n := range kept {
		if n.ID == "unknown" || n.ID == "dead" {
			t.Errorf("未测/实测失败节点不得补位: %v", ids(kept))
		}
	}
	if len(kept) != 3 || kept[2].ID != "slow" {
		t.Errorf("不足保底数时应补次快的实测可用节点，实得 %v", ids(kept))
	}

	// 合格节点已够一个组批（batchSize）：超线的就该留在门外
	p.rec = map[string]probeRecord{}
	qualified := []*model.Node{}
	for i := 0; i < batchSize; i++ {
		id := fmt.Sprintf("ok%d", i)
		p.rec[id] = probeRecord{OK: true, LatencyMS: 300 + i*10, At: now}
		qualified = append(qualified, pn(id))
	}
	p.rec["slower"] = probeRecord{OK: true, LatencyMS: 6768, At: now}
	qualified = append(qualified, pn("slower"))
	full, over := p.Gate(qualified, 800)
	if over != 1 {
		t.Errorf("over = %d，期望 1", over)
	}
	if len(full) != batchSize {
		t.Errorf("合格数够一批时 kept 应恰为 %d 个，实得 %d", batchSize, len(full))
	}
	for _, n := range full {
		if n.ID == "slower" {
			t.Errorf("已有 %d 个实测合格节点，6768ms 的仍进了组: %v", batchSize, ids(full))
		}
	}
	// 不够一批时才按升序补次快的（保底 = 组批大小，避免组内无备选）
	short := []*model.Node{}
	for i := 0; i < 3; i++ {
		id := fmt.Sprintf("f%d", i)
		p.rec[id] = probeRecord{OK: true, LatencyMS: 400 + i, At: now}
		short = append(short, pn(id))
	}
	p.rec["z1"] = probeRecord{OK: true, LatencyMS: 1200, At: now}
	p.rec["z2"] = probeRecord{OK: true, LatencyMS: 900, At: now}
	short = append(short, pn("z2"), pn("z1"))
	got, _ := p.Gate(short, 800)
	// 只有 5 个实测可用节点，全用上也凑不满一批：此时应一个不剩地按升序进组
	if len(got) != 5 {
		t.Errorf("应把 5 个实测可用节点全数纳入，实得 %d: %v", len(got), ids(got))
	}
	if got[3].ID != "z2" || got[4].ID != "z1" {
		t.Errorf("补齐部分应按延迟升序，实得 %v", ids(got))
	}
}

// TestGateNeedsEnoughFastBeforeDroppingUnknown 闸门下不再拿未测节点补位
// （未测 = 延迟未知，补位等于把"不要高延迟"变成赌运气）；
// 只有"一个合格的都不剩"时才退回最快的，并且要能被调用方识别出来。
func TestGateNeedsEnoughFastBeforeDroppingUnknown(t *testing.T) {
	p := NewLocalProbe(filepath.Join(t.TempDir(), "probe.json"))
	now := time.Now()
	p.rec = map[string]probeRecord{"a": {OK: true, LatencyMS: 500, At: now}}
	kept, over := p.Gate([]*model.Node{pn("a"), pn("never-measured")}, 800)
	if over != 0 {
		t.Errorf("无超线节点却报了 %d", over)
	}
	if len(kept) != 1 || kept[0].ID != "a" {
		t.Errorf("未测节点不得补位，实得 %v", ids(kept))
	}
	// 一个合格都没有时由保底补次快的实测可用节点，且顺序仍按延迟升序
	p.rec = map[string]probeRecord{
		"x": {OK: true, LatencyMS: 1200, At: now},
		"y": {OK: true, LatencyMS: 900, At: now},
	}
	kept, over = p.Gate([]*model.Node{pn("x"), pn("y")}, 800)
	if len(kept) != 2 || kept[0].ID != "y" {
		t.Errorf("应退回最快优先，实得 %v", ids(kept))
	}
	if over != 2 {
		t.Errorf("over = %d，期望 2", over)
	}
	if best, worst, _ := p.KeptSpread(kept); best != 900 || worst != 1200 {
		t.Errorf("KeptSpread = %d~%d，期望 900~1200", best, worst)
	}
}

// TestGateDisabledWhenZero 闸门设 0 = 不筛（用户可在设置里关掉）。
func TestGateDisabledWhenZero(t *testing.T) {
	p := NewLocalProbe(filepath.Join(t.TempDir(), "probe.json"))
	p.rec = map[string]probeRecord{"slow": {OK: true, LatencyMS: 9000, At: time.Now()}}
	kept, over := p.Gate([]*model.Node{pn("slow")}, 0)
	if len(kept) != 1 || over != 0 {
		t.Errorf("maxMS=0 应原样返回，实得 %v / over=%d", ids(kept), over)
	}
}
func ids(ns []*model.Node) []string {
	out := []string{}
	for _, n := range ns {
		out = append(out, n.ID)
	}
	return out
}
