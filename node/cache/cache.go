// Package cache 维护节点池的持久化、合并与状态机（任务书第十一/十三/十六章）。
package cache

import (
	"encoding/json"
	"os"
	"path/filepath"
	"time"

	"novanode/checker"
	"novanode/model"
)

// 淘汰阈值（任务书第十三章：连续失败降级、长期失败淘汰）。
const (
	ExpireDays = 7  // 超过 N 天未在来源出现 -> EXPIRED
	RemoveDays = 14 // EXPIRED 超过 N 天 -> REMOVED 并出池
	FailLimit  = 3  // 连续粗筛失败次数 -> FAILED
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
// 长期未在来源出现 -> EXPIRED；EXPIRED 持续过久 -> REMOVED 并移出池。
func Age(p *model.Pool, seen map[string]bool) (expired, removed int) {
	keep := p.Nodes[:0]
	nowT := time.Now().UTC()
	for _, n := range p.Nodes {
		if seen[n.ID] {
			if n.State == model.StateExpired { // 来源重新出现则复活观察
				n.State = model.StateNew
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
	return expired, removed
}

// ApplyCheck 将粗筛结果写回状态机（任务书第十三章：不因一次失败立即删除）。
func ApplyCheck(p *model.Pool, results map[string]checker.Result) (ok, degraded, failed int) {
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
			n.FailCount = 0
			n.LastSuccess = nowS
			n.LatencyMS = r.LatencyMS
			if r.LatencyMS >= 2000 {
				n.State = model.StateDegraded
				degraded++
			} else {
				n.State = model.StateAvailable
				ok++
			}
		} else {
			n.FailCount++
			if n.FailCount >= FailLimit {
				n.State = model.StateFailed
				failed++
			} else if n.State == model.StateAvailable || n.State == model.StateDegraded {
				n.State = model.StateDegraded
			}
		}
	}
	return ok, degraded, failed
}

// Publishable 返回可发布节点（任务书第十五章排序规则的准入线）：
// AVAILABLE / DEGRADED / 从未失败的 NEW。
// 检测失败过的新节点仍留在池内观察（可复活），但不发布给客户端。
func Publishable(p *model.Pool) []*model.Node {
	out := []*model.Node{}
	for _, n := range p.Nodes {
		switch n.State {
		case model.StateAvailable, model.StateDegraded:
			out = append(out, n)
		case model.StateNew:
			if n.FailCount == 0 {
				out = append(out, n)
			}
		}
	}
	return out
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
