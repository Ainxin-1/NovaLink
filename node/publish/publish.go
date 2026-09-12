// Package publish 将节点池发布为客户端可消费的标准格式
// （任务书第三十一章：通用 base64 订阅 + sing-box 原生 JSON 双格式）。
package publish

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"novanode/model"
)

// WriteAll 输出双格式发布文件与统计文件，返回发布节点数。
func WriteAll(dir string, pool *model.Pool, nodes []*model.Node) (int, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return 0, err
	}
	// 1. 通用 base64 订阅
	uris := make([]string, 0, len(nodes))
	for _, n := range nodes {
		if uri := n.Params["uri"]; uri != "" {
			uris = append(uris, uri)
		}
	}
	subs := base64.StdEncoding.EncodeToString([]byte(strings.Join(uris, "\n")))
	if err := os.WriteFile(filepath.Join(dir, "nodes_base64.txt"), []byte(subs), 0o644); err != nil {
		return 0, err
	}
	// 2. sing-box 原生配置
	cfg := singboxConfig(nodes)
	b, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return 0, err
	}
	if err := os.WriteFile(filepath.Join(dir, "singbox_config.json"), b, 0o644); err != nil {
		return 0, err
	}
	// 3. 统计信息（不含任何凭据）
	stats := map[string]any{
		"published":    len(nodes),
		"pool_total":   len(pool.Nodes),
		"by_state":     countByState(pool),
		"by_protocol":  countByProtocol(nodes),
		"updated":      pool.Updated,
	}
	sb, _ := json.MarshalIndent(stats, "", "  ")
	if err := os.WriteFile(filepath.Join(dir, "stats.json"), sb, 0o644); err != nil {
		return 0, err
	}
	return len(nodes), nil
}

func singboxConfig(nodes []*model.Node) map[string]any {
	outbounds := []any{}
	tags := []string{}
	for _, n := range nodes {
		if ob := Outbound(n); ob != nil {
			outbounds = append(outbounds, ob)
			tags = append(tags, n.ID)
		}
	}
	selector := map[string]any{
		"type": "selector", "tag": "proxy", "outbounds": append([]string{"auto"}, tags...),
		"default": "auto",
	}
	auto := map[string]any{
		"type": "urltest", "tag": "auto", "outbounds": tags,
		"url": "http://www.gstatic.com/generate_204", "interval": "10m",
	}
	direct := map[string]any{"type": "direct", "tag": "direct"}
	all := append([]any{selector, auto}, outbounds...)
	all = append(all, direct)
	return map[string]any{
		"log":       map[string]any{"level": "warn"},
		"inbounds":  []any{map[string]any{"type": "mixed", "tag": "mixed-in", "listen": "127.0.0.1", "listen_port": 7890}},
		"outbounds": all,
		"route":     map[string]any{"final": "proxy"},
	}
}

// outbound 依据统一参数生成单节点 sing-box 出站；无法生成时返回 nil。
// Outbound 依据统一参数生成单节点 sing-box 出站；无法生成时返回 nil。
func Outbound(n *model.Node) map[string]any {
	p := n.Params
	base := map[string]any{
		"tag": n.ID, "server": n.Server, "server_port": n.Port,
	}
	switch n.Protocol {
	case "ss":
		base["type"], base["method"], base["password"] = "shadowsocks", p["method"], p["password"]
		return base
	case "trojan", "vless":
		if n.Protocol == "trojan" {
			base["type"] = "trojan"
			base["password"] = p["password"]
		} else {
			base["type"] = "vless"
			base["uuid"] = p["uuid"]
			if p["flow"] != "" {
				base["flow"] = p["flow"]
			}
		}
		sec := p["security"]
		if sec == "reality" {
			if !model.ValidPublicKey(p["pbk"]) {
				return nil // reality 参数非法，跳过该节点
			}
			base["tls"] = map[string]any{
				"enabled": true, "server_name": orDefault(p["sni"], n.Server),
				"utls":        map[string]any{"enabled": true, "fingerprint": "chrome"},
				"reality":     map[string]any{"enabled": true, "public_key": p["pbk"], "short_id": p["sid"]},
			}
		} else if sec == "tls" || n.Protocol == "trojan" {
			base["tls"] = map[string]any{"enabled": true, "server_name": orDefault(p["sni"], n.Server)}
		}
		if !addTransport(base, p) {
			return nil
		}
		return base
	case "vmess":
		base["type"] = "vmess"
		base["uuid"] = p["uuid"]
		base["security"] = orDefault(p["security"], "auto")
		base["alter_id"] = atoiOr(p["aid"], 0)
		if p["tls"] == "tls" {
			base["tls"] = map[string]any{"enabled": true, "server_name": orDefault(p["sni"], n.Server)}
		}
		if !addTransport(base, p) {
			return nil
		}
		return base
	default:
		return nil
	}
}

func addTransport(base map[string]any, p map[string]string) bool {
	switch p["net"] {
	case "ws":
		path := orDefault(p["path"], "/")
		if _, err := url.Parse(path); err != nil {
			return false // 非法路径，交由上层跳过该节点
		}
		t := map[string]any{"type": "ws", "path": path}
		if p["host"] != "" {
			t["headers"] = map[string]any{"Host": p["host"]}
		}
		base["transport"] = t
	case "grpc":
		base["transport"] = map[string]any{"type": "grpc", "service_name": p["path"]}
	}
	return true
}

func countByState(pool *model.Pool) map[string]int {
	m := map[string]int{}
	for _, n := range pool.Nodes {
		m[n.State]++
	}
	return m
}

func countByProtocol(nodes []*model.Node) map[string]int {
	m := map[string]int{}
	for _, n := range nodes {
		m[n.Protocol]++
	}
	return m
}

func orDefault(v, def string) string {
	if v == "" {
		return def
	}
	return v
}

func atoiOr(s string, def int) int {
	n := 0
	if _, err := fmt.Sscanf(s, "%d", &n); err != nil {
		return def
	}
	return n
}

// SortByLatency 供客户端按延迟排序时使用（任务书第十五章简单规则）。
func SortByLatency(nodes []*model.Node) {
	sort.Slice(nodes, func(i, j int) bool {
		a, b := nodes[i], nodes[j]
		sa, sb := stateRank(a.State), stateRank(b.State)
		if sa != sb {
			return sa < sb
		}
		return a.LatencyMS < b.LatencyMS
	})
}

func stateRank(s string) int {
	switch s {
	case model.StateAvailable:
		return 0
	case model.StateNew:
		return 1
	case model.StateDegraded:
		return 2
	case model.StateFailed:
		return 3
	default:
		return 9
	}
}
