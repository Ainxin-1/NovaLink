// Package cache 维护节点池的持久化、合并与状态机（任务书第十一/十三/十六章）。
package cache

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"time"

	"novanode/checker"
	"novanode/model"
)

// 淘汰阈值（任务书第十三章：连续失败降级、长期失败淘汰）。
const (
	ExpireDays = 7  // 超过 N 天未在来源出现 -> EXPIRED
	RemoveDays = 14 // EXPIRED 超过 N 天 -> REMOVED 并出池
	FailLimit  = 3  // 连续检测失败次数 -> FAILED

	// NewFailLimit：NEW 节点（从未成功过）连续失败到该次数即判 FAILED。
	// 免费节点时效性极强，一个新节点连 2 次都握不上手，基本可以认定是死节点，
	// 没必要让它一直以 NEW 状态占着池子（旧规则下这类节点永不淘汰，实测积压 1.8 万个）。
	NewFailLimit = 2

	// DeadDays：FAILED 节点持续该天数仍无任何成功记录 -> 直接出池。
	// 旧规则只按「来源是否还在出现」判断，导致 FAILED 节点能赖满 21 天。
	DeadDays = 3

	// KeepAvailable：池内保留的 AVAILABLE 节点上限。
	// 免费节点池里真正可用的通常只有几百到一千出头，超出的按延迟淘汰，
	// 既避免池子无限膨胀，也保证发布列表全是快节点。
	KeepAvailable = 800
)

// Load 从磁盘读取节点池；文件不存在时返回空池。
func Load(path string) (*model.Pool, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return &model.Pool{Updated: now()}, nil
		}
		return nil, err
	}
	p := &model.Pool{}
	if err := json.Unmarshal(b, p); err != nil {
		return nil, err
	}
	return p, nil
}

// Save 原子化保存节点池（先写临时文件再替换）。
func Save(p *model.Pool, path string) error {
	p.Updated = now()
	b, err := json.MarshalIndent(p, "", "  ")
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	_ = os.Remove(path)
	return os.Rename(tmp, path)
}

// Merge 将去重后的候选节点合并进池（任务书第十章：更新成功才替换/合并）。
// 已存在节点：刷新 LastUpdated、补充来源、保留状态与检测历史。
// 新节点：状态 NEW。本轮未出现的节点不做任何改动（来源可能暂时波动）。
func Merge(p *model.Pool, candidates []model.Node, seen map[string]bool) (added int) {
	byID := map[string]*model.Node{}
	for _, n := range p.Nodes {
		byID[n.ID] = n
	}
	for i := range candidates {
		c := candidates[i]
		if old, ok := byID[c.ID]; ok {
			old.LastUpdated = now()
			old.Params = c.Params // 以最新抓取的参数为准
			old.Name = c.Name
			for _, s := range c.Sources {
				if !hasSource(old.Sources, s) {
					old.Sources = append(old.Sources, s)
				}
			}
			continue
		}
		n := c
		n.FirstSeen, n.LastUpdated, n.State = now(), now(), model.StateNew
		p.Nodes = append(p.Nodes, &n)
		byID[n.ID] = &n
		added++
	}
	return added
}

// Age 执行过期清理（任务书第十六章：缓存不能无限保存失效节点）。
// 三条清理规则：
//  1. 长期未在来源出现 -> EXPIRED；EXPIRED 持续过久 -> REMOVED 并移出池
//  2. FAILED 且持续 DeadDays 无任何成功记录 -> 直接出池（不让死节点赖满 21 天）
//  3. AVAILABLE 超过 KeepAvailable 上限 -> 按延迟从差到好淘汰，控制池子规模
func Age(p *model.Pool, seen map[string]bool) (expired, removed int) {
	keep := p.Nodes[:0]
	nowT := time.Now().UTC()
	for _, n := range p.Nodes {
		if seen[n.ID] {
			if n.State == model.StateExpired { // 来源重新出现则复活观察
				n.State = model.StateNew
			}
			// 规则 2：来源还在出现，但连续失败且久无成功记录 -> 清出
			if n.State == model.StateFailed && deadTooLong(n, nowT) {
				removed++
				continue
			}
			keep = append(keep, n)
			continue
		}
		upd, err := time.Parse(time.RFC3339, n.LastUpdated)
		if err != nil {
			upd = nowT
		}
		age := nowT.Sub(upd)
		switch {
		case age > (RemoveDays+ExpireDays)*24*time.Hour || n.State == model.StateRemoved:
			removed++
			continue // 移出池
		case age > ExpireDays*24*time.Hour:
			if n.State != model.StateExpired {
				n.State = model.StateExpired
				expired++
			}
		}
		keep = append(keep, n)
	}
	p.Nodes = keep
	removed += trimAvailable(p)
	return expired, removed
}

// Cap 把池子规模压到 maxNodes 以内，返回被移出的节点数。
//
// 为什么必须压：来源一次全量就能吐出 4.4 万个候选（2026-09-19 实测），
// 而池子是全量提交进 git 并被客户端整个下载解析的 —— 不封顶的话
// pool.json 每两小时膨胀一次，仓库与客户端都会被拖死。
//
// 淘汰顺序按"这台机器上还能不能指望它"：国内实测通过率约 0.05% 且
// 各协议/各 IP 段没有显著差别，所以价值只体现在**有没有成功记录**与
// **有多新**上，而不是协议或来源。
func Cap(p *model.Pool, maxNodes int) int {
	if maxNodes <= 0 || len(p.Nodes) <= maxNodes {
		return 0
	}
	score := func(n *model.Node) int {
		switch {
		case n.State == model.StateAvailable || n.State == model.StateDegraded || n.State == model.StateTesting:
			return 4 // 验证过的资产，最后才动
		case n.LastSuccess != "":
			return 3 // 历史上成功过，值得再试
		case n.State == model.StateNew:
			return 2 // 新面孔：免费节点时效性强，保留
		case n.State == model.StateFailed:
			return 1 // 连续失败
		default:
			return 0 // EXPIRED / REMOVED
		}
	}
	sort.SliceStable(p.Nodes, func(i, j int) bool {
		si, sj := score(p.Nodes[i]), score(p.Nodes[j])
		if si != sj {
			return si > sj
		}
		// 同档内取更新的（first_seen 大 = 更可能还活着）
		return p.Nodes[i].FirstSeen > p.Nodes[j].FirstSeen
	})
	dropped := len(p.Nodes) - maxNodes
	p.Nodes = p.Nodes[:maxNodes]
	return dropped
}

// deadTooLong 判断 FAILED 节点是否已持续太久没有任何成功记录。
func deadTooLong(n *model.Node, nowT time.Time) bool {
	ref := n.LastSuccess
	if ref == "" {
		ref = n.LastChecked
	}
	if ref == "" {
		ref = n.LastUpdated
	}
	t, err := time.Parse(time.RFC3339, ref)
	if err != nil {
		return false // 时间戳不合法时不误删
	}
	return nowT.Sub(t) > DeadDays*24*time.Hour
}

// trimAvailable 把 AVAILABLE 数量压到 KeepAvailable 以内：
// 保留延迟最优的那批，其余降级为 DEGRADED（不删除，仍可作为兜底候选）。
// 返回被真正移出池的节点数（本函数不物理删除，恒为 0）。
func trimAvailable(p *model.Pool) int {
	av := []*model.Node{}
	for _, n := range p.Nodes {
		if n.State == model.StateAvailable {
			av = append(av, n)
		}
	}
	if len(av) <= KeepAvailable {
		return 0
	}
	sort.Slice(av, func(i, j int) bool { return av[i].LatencyMS < av[j].LatencyMS })
	for i := KeepAvailable; i < len(av); i++ {
		av[i].State = model.StateDegraded
	}
	return 0
}

// ApplyCheck 将检测结果写回状态机（任务书第十三章：不因一次失败立即删除）。
// maxLatencyMS 是可用线：延迟超过即降级，连续 2 轮超线判死（沉降为 FAILED，
// 之后走既有的 24h 复检 / 物理清理规则，不直接抹除）。
//
// NEW 节点（从未成功过）用更严的阈值 NewFailLimit：连不上就尽快判死出池，
// 避免「从不发布、也从不淘汰」的僵尸节点堆积。
func ApplyCheck(p *model.Pool, results map[string]checker.Result, maxLatencyMS int) (ok, degraded, failed int) {
	nowS := now()
	for _, n := range p.Nodes {
		switch n.State {
		case model.StateExpired, model.StateRemoved:
			continue
		}
		r, checked := results[n.ID]
		if !checked {
			continue
		}
		n.LastChecked = nowS
		if r.OK {
			n.LatencyMS = r.LatencyMS
			if r.LatencyMS >= maxLatencyMS {
				// 慢而活：降级并累计慢轮数，连续 2 轮超线判死
				n.SlowCount++
				n.LastSuccess = nowS
				n.FailCount = 0
				if n.SlowCount >= 2 {
					n.State = model.StateFailed
					failed++
				} else {
					n.State = model.StateDegraded
					degraded++
				}
			} else {
				n.SlowCount = 0
				n.FailCount = 0
				n.LastSuccess = nowS
				n.State = model.StateAvailable
				ok++
			}
			continue
		}
		// 失败：NEW 从未成功过的用更严阈值
		n.FailCount++
		limit := FailLimit
		if n.LastSuccess == "" {
			limit = NewFailLimit
		}
		switch {
		case n.FailCount >= limit:
			n.State = model.StateFailed
			failed++
		case n.State == model.StateAvailable || n.State == model.StateDegraded:
			n.State = model.StateDegraded
			degraded++
		}
	}
	return ok, degraded, failed
}

// fastAvailable 统计延迟达标的可用节点数（荒年判定用）。
func fastAvailable(p *model.Pool, maxLatencyMS int) int {
	c := 0
	for _, n := range p.Nodes {
		if n.State == model.StateAvailable && n.LatencyMS < maxLatencyMS {
			c++
		}
	}
	return c
}

// Publishable 返回可发布节点（只发真正验证过能用的）。
// 规则：
//   - AVAILABLE（延迟达标、检测通过）恒发布
//   - DEGRADED 仅在「荒年」（达标可用 < 3 个）时兜底发布，且要求它成功过
//   - NEW 必须有成功记录才算数；从未成功过的 NEW 不发布（它们很快会被判 FAILED 出池）
//
// 旧规则把「从未失败过的 NEW」全部发布，导致订阅里塞进上万个没验证过的节点。
func Publishable(p *model.Pool) []*model.Node {
	fast := fastAvailable(p, 800)
	out := []*model.Node{}
	for _, n := range p.Nodes {
		switch n.State {
		case model.StateAvailable:
			out = append(out, n)
		case model.StateDegraded:
			if fast < 3 && n.LastSuccess != "" {
				out = append(out, n)
			}
		case model.StateNew:
			if n.FailCount == 0 && n.LastSuccess != "" {
				out = append(out, n)
			}
		}
	}
	return out
}

// Zombie 判定：一个节点从未成功过（无 LastSuccess），且连续失败已达上限，
// 状态却卡在 NEW（旧规则的漏洞）—— 它既不发布、也不淘汰，纯粹占地方。
func isZombie(n *model.Node) bool {
	if n.LastSuccess != "" {
		return false
	}
	if n.State != model.StateNew && n.State != model.StateFailed {
		return false
	}
	return n.FailCount >= NewFailLimit
}

// ZombieCount 统计池内僵尸节点数。
func ZombieCount(p *model.Pool) int {
	c := 0
	for _, n := range p.Nodes {
		if isZombie(n) {
			c++
		}
	}
	return c
}

// PruneZombies 移除僵尸节点，返回清理后池内节点数。
func PruneZombies(p *model.Pool) int {
	keep := p.Nodes[:0]
	for _, n := range p.Nodes {
		if isZombie(n) {
			continue
		}
		keep = append(keep, n)
	}
	p.Nodes = keep
	return len(p.Nodes)
}

// Seen 本轮出现的节点 ID 集合。
func Seen(candidates []model.Node) map[string]bool {
	m := map[string]bool{}
	for i := range candidates {
		m[candidates[i].ID] = true
	}
	return m
}

func hasSource(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}

func now() string { return time.Now().UTC().Format(time.RFC3339) }
