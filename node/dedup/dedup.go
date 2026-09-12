// Package dedup 依据节点指纹去重并合并来源（任务书第十二章）。
package dedup

import "novanode/model"

// Merge 输入各来源解析出的候选节点，按指纹合并：
// 相同节点只保留一份，记录其出现的全部来源。
func Merge(bySource map[string][]model.Node) []model.Node {
	index := map[string]int{}
	out := []model.Node{}
	for source, nodes := range bySource {
		for i := range nodes {
			n := nodes[i]
			if idx, ok := index[n.ID]; ok {
				if !contains(out[idx].Sources, source) {
					out[idx].Sources = append(out[idx].Sources, source)
				}
				continue
			}
			n.Sources = []string{source}
			index[n.ID] = len(out)
			out = append(out, n)
		}
	}
	return out
}

func contains(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}
