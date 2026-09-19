// 本地协议级测活（客户端自己测、自己存、自己排）。
//
// 为什么必须有这一层：管线里的 latency 是 GitHub 海外机房测出来的。
// 2026-09-19 同一条国内线路实测对照 —— CI 标 800 个 AVAILABLE，
// 本机协议级能在 800ms 内取回外网内容的：**0 个**（仅 18 个勉强通，916~5763ms）。
// 海外机房到任何目标都通，那个数字对国内线路没有判别力，
// 可排序、发布、界面展示全都用它。唯一可信的度量点就是用户这台机器，
// 所以客户端必须自己发起真实握手，并把结果作为选路依据。
package core

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"novanode/checker"
	"novanode/model"
)

const (
	probeFresh    = 10 * time.Minute // 该间隔内不重复测同一节点（实测失效尺度是十几分钟）
	probeValidFor = 6 * time.Hour    // 超过即不再采信（免费节点时效以小时计）
	probeBasePort = 34000            // 与管线(30000)和自身代理端口错开
	probeChunk    = 64               // 一批 = 一个核心进程带 64 个入站/出站对
)

type probeRecord struct {
	OK        bool `json:"ok"`
	LatencyMS int  `json:"latency_ms,omitempty"`
	// 多轮采样画像（旧文件里没有这些字段，读入时为 0，排序自动退回均值）：
	// 单次均值会骗人 —— 实测见过均值 450ms、P95 6s 的节点。
	Samples int       `json:"samples,omitempty"`
	OKs     int       `json:"oks,omitempty"`
	P50     int       `json:"p50_ms,omitempty"`
	P95     int       `json:"p95_ms,omitempty"`
	Jitter  int       `json:"jitter_ms,omitempty"`
	Loss    float64   `json:"loss,omitempty"`
	At      time.Time `json:"at"`
}

// sortKey 是排序与闸门共用的延迟键：有画像时用 P95（尾部），没有时退回均值。
//
// 为什么是 P95 而不是平均：免费节点的常见失败形态是"大部分请求快、偶发一次超时"，
// 平均值把它藏起来了；用户体感的卡顿正是那一次。
func (r probeRecord) sortKey() int {
	if r.P95 > 0 {
		return r.P95
	}
	return r.LatencyMS
}

// desc 给界面用的一行摘要。
func (r probeRecord) desc() string {
	if r.Samples == 0 {
		return fmt.Sprintf("%dms", r.sortKey())
	}
	return fmt.Sprintf("p50=%d p95=%d 丢包=%.0f%% (%d/%d轮)",
		r.P50, r.P95, r.Loss*100, r.OKs, r.Samples)
}

// LocalProbe 保存本机对各节点的协议级实测结果（落盘，重启不丢）。
type LocalProbe struct {
	mu   sync.Mutex
	path string
	rec  map[string]probeRecord
}

// NewLocalProbe 读取（或初始化）实测记录。文件损坏时按空处理而不是退出。
func NewLocalProbe(path string) *LocalProbe {
	p := &LocalProbe{path: path, rec: map[string]probeRecord{}}
	b, err := os.ReadFile(path)
	if err != nil {
		return p
	}
	var stored map[string]probeRecord
	if json.Unmarshal(b, &stored) == nil {
		for id, r := range stored {
			if time.Since(r.At) <= probeValidFor {
				p.rec[id] = r
			}
		}
	}
	return p
}

// apply 合并一轮检测结果并落盘。
func (p *LocalProbe) apply(res map[string]checker.Result) int {
	now := time.Now()
	p.mu.Lock()
	for id, r := range res {
		p.rec[id] = probeRecord{
			OK: r.OK, LatencyMS: r.LatencyMS, At: now,
			Samples: r.Stats.Samples, OKs: r.Stats.OKs,
			P50: r.Stats.P50, P95: r.Stats.P95, Jitter: r.Stats.Jitter, Loss: r.Stats.Loss,
		}
	}
	n := len(res)
	p.mu.Unlock()
	if n > 0 {
		p.save()
	}
	return n
}

// Scan 对 nodes 跑一轮真实协议握手（每批 probeChunk 个入站并发验证），
// 每批内对每个节点连续采样 samples 轮（同一个核心进程，不重复启停）。
// 返回（实际检测数, 其中可用数）。sing-box 缺失时返回 0，调用方降级。
func (p *LocalProbe) Scan(nodes []*model.Node, singboxPath string, samples int, logf func(string, ...any)) (int, int) {
	if len(nodes) == 0 {
		return 0, 0
	}
	if _, err := os.Stat(singboxPath); err != nil {
		if logf != nil {
			logf("[LOCAL] 未找到核心 %s，跳过本地协议级测活", singboxPath)
		}
		return 0, 0
	}
	if samples < 1 {
		samples = 1
	}
	vals := make([]model.Node, 0, len(nodes))
	for _, n := range nodes {
		vals = append(vals, *n)
	}
	res := checker.DeepRounds(vals, singboxPath, probeBasePort, probeChunk, samples, logf)
	ok := 0
	for _, r := range res {
		if r.OK {
			ok++
		}
	}
	p.apply(res)
	return len(res), ok
}

func (p *LocalProbe) save() error {
	p.mu.Lock()
	b, err := json.MarshalIndent(p.rec, "", "  ")
	p.mu.Unlock()
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(p.path), 0o755); err != nil {
		return err
	}
	tmp := p.path + ".tmp"
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, p.path)
}

func (p *LocalProbe) record(id string) (probeRecord, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	r, ok := p.rec[id]
	if !ok || time.Since(r.At) > probeValidFor {
		return probeRecord{}, false
	}
	return r, true
}

// UsableCount 返回本机实测确认可用的节点数。
func (p *LocalProbe) UsableCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	n := 0
	for _, r := range p.rec {
		if r.OK && time.Since(r.At) <= probeValidFor {
			n++
		}
	}
	return n
}

// ScanPool 分批对候选做协议级实测（每批一次真实握手验证），进度回调用于界面。
//
// 分批是必须的：一批就是一个核心进程带 64 个入站/出站对，
// 一次全量会把进程句柄与临时端口打爆，而且中途无法向界面汇报进度。
func (p *LocalProbe) ScanPool(nodes []*model.Node, singboxPath string, samples int,
	progress func(done, usable, total int), logf func(string, ...any)) (int, int) {
	due := p.Due(nodes, len(nodes))
	total := len(due)
	tested, usable := 0, 0
	for off := 0; off < total; off += probeChunk {
		end := off + probeChunk
		if end > total {
			end = total
		}
		n, ok := p.Scan(due[off:end], singboxPath, samples, logf)
		tested += n
		usable += ok
		if progress != nil {
			progress(tested, usable, total)
		}
	}
	return tested, usable
}

// Lookup 返回某节点的本机实测结论（是否存在、是否可用、延迟）。
func (p *LocalProbe) Lookup(id string) (ok bool, latencyMS int, seen bool) {
	r, seen := p.record(id)
	if !seen {
		return false, 0, false
	}
	return r.OK, r.sortKey(), true
}

// Due 从候选里挑出"该重测"的节点：没测过、或结果已超过 probeFresh 的。
// 已知可用的也要隔 probeFresh 复测 —— 免费节点是分钟级失效的，
// 只在连接前测一次等于拿旧结论做新决策。
func (p *LocalProbe) Due(nodes []*model.Node, capN int) []*model.Node {
	out := []*model.Node{}
	for _, n := range nodes {
		if len(out) >= capN {
			break
		}
		r, seen := p.record(n.ID)
		if !seen || time.Since(r.At) > probeFresh {
			out = append(out, n)
		}
	}
	return out
}

// Rank 按本机实测重排候选，且是稳定排序：
// 实测可用的按延迟升序置顶；没测过的保持原有次序居中；
// 实测不可用的（TTL 内）沉底但不丢弃（免费节点的失败常常是几分钟的抖动）。
func (p *LocalProbe) Rank(nodes []*model.Node) []*model.Node {
	type bucket struct {
		node *model.Node
		rank int // 0=实测可用 1=未测 2=实测不可用
		lat  int
		seq  int
	}
	items := make([]bucket, len(nodes))
	for i, n := range nodes {
		b := bucket{node: n, rank: 1, seq: i}
		if r, ok := p.record(n.ID); ok {
			if r.OK {
				b.rank, b.lat = 0, r.sortKey()
			} else {
				b.rank = 2
			}
		}
		items[i] = b
	}
	sort.SliceStable(items, func(i, j int) bool {
		if items[i].rank != items[j].rank {
			return items[i].rank < items[j].rank
		}
		if items[i].rank == 0 && items[i].lat != items[j].lat {
			return items[i].lat < items[j].lat
		}
		return items[i].seq < items[j].seq
	})
	out := make([]*model.Node, len(items))
	for i := range items {
		out[i] = items[i].node
	}
	return out
}

// minGroupRedundancy 候选组的最少节点数。
//
// 为什么保底要等于组批大小：2026-09-19 实测，严格 800ms 闸门下只剩 1 个合格节点，
// 核心起来 27 秒后那唯一一个失效 → 组内无备选 → 直接连不上；而不设闸门时
// 不设闸门时同一时刻有 11 个可用节点、能正常出网。所以"不要高延迟"只能是偏好，
// 不能是把备选砍光的硬过滤 —— 组内没备选就等于单点，单点必挂。
const minGroupRedundancy = batchSize

// Gate 用延迟闸门切候选。
//
// 返回 kept（优先实测可用且 ≤maxMS，按延迟升序；不足 minGroupRedundancy 个
// 时用次快的实测可用节点补到该数）与 over（因超线被让位的个数）。
// maxMS<=0 表示不设闸门。补进来的一定是**本机实测可用**的，未测节点不补位
// （未测=延迟未知，拿它补位等于把"不要高延迟"变成赌运气）。
//
// 实测分布决定这里的取舍（同一台机器同一线路，11 个可用节点协议级延迟）：
//
//	594 731 740 | 876 908 | 1553 1582 | 2012 2096 2132 | 3394 ms
func (p *LocalProbe) Gate(nodes []*model.Node, maxMS int) (kept []*model.Node, over int) {
	if maxMS <= 0 {
		return nodes, 0
	}
	fast, slow := []*model.Node{}, []*model.Node{}
	for _, n := range nodes {
		r, seen := p.record(n.ID)
		switch {
		case !seen || !r.OK:
			continue // 未测/实测失败：不参与组批，交给 Rank 与 TCP 预筛兜底
		case r.sortKey() > maxMS:
			over++
			slow = append(slow, n)
		default:
			fast = append(fast, n)
		}
	}
	byLat := func(ns []*model.Node) {
		sort.SliceStable(ns, func(i, j int) bool {
			li, _ := p.record(ns[i].ID)
			lj, _ := p.record(ns[j].ID)
			return li.LatencyMS < lj.LatencyMS
		})
	}
	byLat(fast)
	byLat(slow)
	kept = fast
	for i := 0; i < len(slow) && len(kept) < minGroupRedundancy; i++ {
		kept = append(kept, slow[i]) // 保底：拿次快且实测可用的，别把组批掏空
	}
	if len(kept) == 0 {
		return nodes, over
	}
	return kept, over
}

// KeptSpread 返回kept里最快/最慢的实测延迟，用于把闸门效果说清楚。
func (p *LocalProbe) KeptSpread(nodes []*model.Node) (best, worst, qualified int) {
	best, worst = 0, 0
	for _, n := range nodes {
		r, seen := p.record(n.ID)
		if !seen || !r.OK {
			continue
		}
		if best == 0 || r.LatencyMS < best {
			best = r.LatencyMS
		}
		if r.LatencyMS > worst {
			worst = r.LatencyMS
		}
	}
	return best, worst, len(nodes)
}

// GoodWithin 返回"在 within 之内被实测判为可用"的节点数与其中最快延迟。
//
// 与 KnownGood 的区别就是这个 within：免费节点的失效尺度是十几分钟
// （实测：15:12 那轮 11 个可用，到 16:03 直接连败两批），所以"多久之内的
// 结论才算数"必须能单独问，不能一律按 6 小时的有效期。
func (p *LocalProbe) GoodWithin(nodes []*model.Node, within time.Duration) (int, int) {
	n, best := 0, 0
	for _, nd := range nodes {
		r, seen := p.record(nd.ID)
		if !seen || !r.OK || time.Since(r.At) > within {
			continue
		}
		n++
		if r.LatencyMS > 0 && (best == 0 || r.LatencyMS < best) {
			best = r.LatencyMS
		}
	}
	return n, best
}

// KnownGood 返回候选里本机实测可用的数量与最快一个的延迟（用于状态展示）。
func (p *LocalProbe) KnownGood(nodes []*model.Node) (int, int) {
	good, best := 0, 0
	for _, n := range nodes {
		if r, ok := p.record(n.ID); ok && r.OK {
			good++
			if best == 0 || r.LatencyMS < best {
				best = r.LatencyMS
			}
		}
	}
	return good, best
}

// Describe 返回某节点本机实测画像的一行摘要（无记录时 seen=false）。
func (p *LocalProbe) Describe(id string) (string, bool) {
	r, seen := p.record(id)
	if !seen {
		return "", false
	}
	return r.desc(), true
}
