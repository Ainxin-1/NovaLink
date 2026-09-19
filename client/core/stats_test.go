package core

import (
	"path/filepath"
	"testing"
	"time"

	"novanode/model"
)

// TestRankPrefersLowTailNotLowMean 排序键必须是尾部延迟（P95）而不是均值。
//
// 对应筛选方案里那个例子：B 大多时候 450ms 但偶发 6s，A 稳定在 550~700ms。
// 只看单次均值会把 B 排在 A 前面，而用户体感的卡顿恰恰来自 B 那一次尖峰。
func TestRankPrefersLowTailNotLowMean(t *testing.T) {
	now := time.Now()
	p := NewLocalProbe(filepath.Join(t.TempDir(), "probe.json"))
	p.rec = map[string]probeRecord{
		"A": {OK: true, LatencyMS: 620, P50: 620, P95: 700, Samples: 5, OKs: 5, At: now},
		"B": {OK: true, LatencyMS: 1556, P50: 460, P95: 6000, Samples: 5, OKs: 5, At: now},
	}
	nodes := []*model.Node{pn("B"), pn("A")}
	got := p.Rank(nodes)
	if got[0].ID != "A" {
		t.Errorf("应按 P95 排序，A(P95=700) 应在 B(P95=6000) 前，实得 %v", ids(got))
	}
	// 闸门看的是尾部：B 均值(450)虽低于阈值，仍被记为超线；
	// 但候选只剩 2 个、不足组批保底数（batchSize）时会按次快补进来 ——
	// 这是今晚实测换来的规则：组内没备选就等于单点，单点必挂。
	kept, over := p.Gate(nodes, 2000)
	if over != 1 {
		t.Errorf("B 应因 P95=6000 被记为超线，over=%d", over)
	}
	if len(kept) != 2 || kept[0].ID != "A" || kept[1].ID != "B" {
		t.Errorf("应 A 在前、B 作保底补入，实得 %v", ids(kept))
	}
	// 合格数够满一个组批（batchSize=8）时，超线节点才进不来
	extra := []struct {
		id  string
		p95 int
	}{{"C", 820}, {"D", 950}, {"E", 1100}, {"F", 1300}, {"G", 1400}, {"H", 1500}, {"I", 1600}}
	for _, e := range extra {
		p.rec[e.id] = probeRecord{OK: true, LatencyMS: e.p95 - 40, P50: e.p95 - 40,
			P95: e.p95, Samples: 5, OKs: 5, At: now}
	}
	full := []*model.Node{pn("A"), pn("B")}
	for _, e := range extra {
		full = append(full, pn(e.id))
	}
	kept, over = p.Gate(full, 2000)
	if over != 1 {
		t.Errorf("over = %d，期望 1", over)
	}
	if len(kept) != batchSize {
		t.Errorf("够一批时应只留合格的 %d 个，实得 %d: %v", batchSize, len(kept), ids(kept))
	}
	for _, n := range kept {
		if n.ID == "B" {
			t.Errorf("B(P95=6000) 不该在候选够用时进组: %v", ids(kept))
		}
	}
}

// TestSortKeyFallsBackToMean 没有画像字段的旧记录（升级前的 local_probe.json）
// 必须仍能排序与判闸门，不能因为 P95=0 而被当成 0ms 抢到最前。
func TestSortKeyFallsBackToMean(t *testing.T) {
	now := time.Now()
	old := probeRecord{OK: true, LatencyMS: 500, At: now}
	if got := old.sortKey(); got != 500 {
		t.Errorf("旧记录 sortKey = %d，期望退回均值 500", got)
	}
	withStats := probeRecord{OK: true, LatencyMS: 450, P95: 3000, At: now}
	if got := withStats.sortKey(); got != 3000 {
		t.Errorf("有画像时 sortKey = %d，期望用 P95 3000", got)
	}
	p := NewLocalProbe(filepath.Join(t.TempDir(), "probe.json"))
	p.rec = map[string]probeRecord{
		"old":  old,
		"tail": {OK: true, LatencyMS: 100, P95: 200, At: now},
	}
	got := p.Rank([]*model.Node{pn("old"), pn("tail")})
	if got[0].ID != "tail" {
		t.Errorf("tail(P95=200) 应排在旧记录(均值500)之前，实得 %v", ids(got))
	}
}

// TestProbeRecordPersistsStats 画像必须能存能读，否则每次重启都退回单样本。
func TestProbeRecordPersistsStats(t *testing.T) {
	path := filepath.Join(t.TempDir(), "probe.json")
	p := NewLocalProbe(path)
	now := time.Now()
	p.rec = map[string]probeRecord{
		"x": {OK: true, LatencyMS: 600, P50: 600, P95: 900, Jitter: 300,
			Samples: 5, OKs: 4, Loss: 0.2, At: now},
	}
	if err := p.save(); err != nil {
		t.Fatal(err)
	}
	back := NewLocalProbe(path)
	r, seen := back.record("x")
	if !seen {
		t.Fatal("记录未读回")
	}
	if r.P95 != 900 || r.P50 != 600 || r.Samples != 5 || r.OKs != 4 || r.Loss != 0.2 {
		t.Errorf("画像字段丢失或错位: %+v", r)
	}
	if d := r.desc(); !contains(d, "p95=900") {
		t.Errorf("desc = %q，应含 p95=900", d)
	}
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
