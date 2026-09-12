// poolstats 分析节点池中各来源的重合度（一次性分析工具）。
package main

import (
	"fmt"
	"os"
	"sort"
	"strings"

	"novanode/cache"
)

// 来源归组：同一聚合器的多个通道/协议文件视为一组
func group(name string) string {
	switch {
	case strings.Contains(name, "Eternity"):
		return "mahdibland/Eternity"
	case strings.Contains(name, "Epodonios"):
		return "Epodonios"
	case strings.Contains(name, "AutoMerge"):
		return "chengaopan/AutoMerge"
	case strings.Contains(name, "roosterkid"):
		return "roosterkid/openproxylist"
	case strings.Contains(name, "Pawdroid"):
		return "Pawdroid/Free-servers"
	case strings.Contains(name, "ermaozi"):
		return "ermaozi/get_subscribe"
	case strings.Contains(name, "索引-lza6目录"):
		return "lza6目录展开(40源)"
	default:
		return ""
	}
}

func main() {
	pool, err := cache.Load(os.Args[1])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	sets := map[string]map[string]bool{}
	total := 0
	stateByGroup := map[string]map[string]int{}
	for _, n := range pool.Nodes {
		total++
		for _, s := range n.Sources {
			g := group(s)
			if g == "" {
				continue
			}
			if sets[g] == nil {
				sets[g] = map[string]bool{}
			}
			sets[g][n.ID] = true
			if stateByGroup[g] == nil {
				stateByGroup[g] = map[string]int{}
			}
			stateByGroup[g][n.State]++
		}
	}
	names := []string{}
	for g := range sets {
		names = append(names, g)
	}
	sort.Strings(names)
	fmt.Printf("池内总节点: %d\n\n各组独有节点数（该组指纹出现次数）:\n", total)
	for _, g := range names {
		fmt.Printf("  %-28s %5d\n", g, len(sets[g]))
	}
	fmt.Println("\n两两重合（交集 / 较小组规模 = 重合率）:")
	for i := 0; i < len(names); i++ {
		for j := i + 1; j < len(names); j++ {
			a, b := names[i], names[j]
			inter := 0
			for id := range sets[a] {
				if sets[b][id] {
					inter++
				}
			}
			min := len(sets[a])
			if len(sets[b]) < min {
				min = len(sets[b])
			}
			if min == 0 {
				continue
			}
			fmt.Printf("  %-24s × %-24s %5d / %5d = %3.0f%%\n", a, b, inter, min, float64(inter)*100/float64(min))
		}
	}
	fmt.Println("\n各组内部状态分布（同一节点多组共享会重复计入）:")
	for _, g := range names {
		line := ""
		for _, st := range []string{"AVAILABLE", "DEGRADED", "FAILED", "NEW"} {
			if c := stateByGroup[g][st]; c > 0 {
				line += fmt.Sprintf("%s=%d ", st, c)
			}
		}
		fmt.Printf("  %-28s %s\n", g, line)
	}
}
