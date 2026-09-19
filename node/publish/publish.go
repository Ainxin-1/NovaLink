// Package publish 将节点池发布为客户端可消费的标准格式
// （任务书第三十一章：通用 base64 订阅 + sing-box 原生 JSON 双格式）。
package publish

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net"
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
	// 3. 客户端用的精简池：全量 pool.json 已达数十 MB，而客户端要整个解析
	// 它（界面每 30 秒刷新一次列表）。只留"值得在用户机器上试"的一批。
	slim := SlimPool(pool, nodes, SlimNodes)
	slimb, err := json.Marshal(slim)
	if err != nil {
		return 0, err
	}
	if err := os.WriteFile(filepath.Join(dir, "pool_slim.json"), slimb, 0o644); err != nil {
		return 0, err
	}
	// 4. 统计信息（不含任何凭据）
	stats := map[string]any{
		"published":   len(nodes),
		"pool_total":  len(pool.Nodes),
		"slim_total":  len(slim.Nodes),
		"by_state":    countByState(pool),
		"by_protocol": countByProtocol(nodes),
		"updated":     pool.Updated,
	}
	sb, _ := json.MarshalIndent(stats, "", "  ")
	if err := os.WriteFile(filepath.Join(dir, "stats.json"), sb, 0o644); err != nil {
		return 0, err
	}
	return len(nodes), nil
}

// SlimNodes 精简池上限（约 1~2MB，客户端一轮扫描 ~10 分钟）。
const SlimNodes = 3000

// SlimPool 组装精简池：已发布节点在前，其余按状态与新近度补到上限。
//
// 排序依据说明：全量池里国内可用率约 0.05%，且各协议/各 IP 段没有显著差别
// （2026-09-19 实测），所以"值得试"只能看管线自己的状态与新近度，
// 不能拿海外机房测出来的延迟当筛选条件。
func SlimPool(pool *model.Pool, published []*model.Node, maxNodes int) *model.Pool {
	included := map[string]bool{}
	out := make([]*model.Node, 0, maxNodes)
	for _, n := range published {
		if len(out) >= maxNodes {
			break
		}
		if !included[n.ID] {
			included[n.ID] = true
			out = append(out, n)
		}
	}
	rest := make([]*model.Node, 0, len(pool.Nodes))
	for _, n := range pool.Nodes {
		if included[n.ID] {
			continue
		}
		if n.State == model.StateRemoved || n.State == model.StateExpired || n.State == model.StateFailed {
			continue // 判死或已出池的，不必再让客户端浪费时间
		}
		rest = append(rest, n)
	}
	sort.SliceStable(rest, func(i, j int) bool {
		ri, rj := stateRank(rest[i].State), stateRank(rest[j].State)
		if ri != rj {
			return ri < rj
		}
		return rest[i].FirstSeen > rest[j].FirstSeen // 免费节点：更新的更可能还活着
	})
	for _, n := range rest {
		if len(out) >= maxNodes {
			break
		}
		out = append(out, n)
	}
	slim := &model.Pool{Updated: pool.Updated, Nodes: out}
	SortByLatency(slim.Nodes)
	return slim
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
		"url": "https://www.gstatic.com/generate_204", "interval": "10m",
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
	case "hysteria2", "tuic", "anytls":
		// 这三类总是 TLS/QUIC，不需要 security 参数开关
		base["type"] = n.Protocol
		if n.Protocol == "tuic" {
			base["uuid"] = p["uuid"]
			base["congestion_control"] = orDefault(p["congestion_control"], "bbr")
			base["udp_relay_mode"] = orDefault(p["udp_relay_mode"], "native")
		}
		base["password"] = p["password"]
		if obfs := p["obfs"]; obfs != "" {
			base["obfs"] = map[string]any{"type": obfs, "password": p["obfs_password"]}
		}
		base["tls"] = tlsConfig(p, n.Server)
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
			rl := map[string]any{"enabled": true, "public_key": p["pbk"]}
			if p["sid"] != "" {
				rl["short_id"] = p["sid"]
			}
			tl := tlsConfig(p, n.Server)
			tl["reality"] = rl
			base["tls"] = tl
		} else if sec == "tls" || n.Protocol == "trojan" {
			base["tls"] = tlsConfig(p, n.Server)
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
			base["tls"] = tlsConfig(p, n.Server)
		}
		if !addTransport(base, p) {
			return nil
		}
		return base
	default:
		return nil
	}
}

// tlsConfig 生成 sing-box 的 TLS 段。
//
// 两处修正：
//   - server_name 只在有域名时给出。原先无条件 orDefault(sni, server)，
//     裸 IP 节点会被填成 server_name:"1.2.3.4" —— 非法 SNI，握手必失败，
//     这类节点在检测里表现为"TCP 通、协议不通"。
//   - fp/alpn/insecure 此前完全没落到配置里，带这些参数的节点配不出来。
func tlsConfig(p map[string]string, server string) map[string]any {
	m := map[string]any{"enabled": true}
	if sni := serverName(p, server); sni != "" {
		m["server_name"] = sni
	}
	if p["insecure"] == "1" {
		m["insecure"] = true
	}
	if alpn := p["alpn"]; alpn != "" {
		list := []string{}
		for _, a := range strings.Split(alpn, ",") {
			if a = strings.TrimSpace(a); a != "" {
				list = append(list, a)
			}
		}
		if len(list) > 0 {
			m["alpn"] = list
		}
	}
	m["utls"] = map[string]any{
		"enabled": true, "fingerprint": orDefault(p["fp"], "chrome"),
	}
	return m
}

// serverName 取可用于 SNI 的域名；没有 sni 且服务器本身是 IP 时返回空串。
func serverName(p map[string]string, server string) string {
	if v := p["sni"]; v != "" {
		return v
	}
	if net.ParseIP(strings.Trim(server, "[]")) == nil {
		return server // 域名可直接作 SNI
	}
	return ""
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
		t := map[string]any{"type": "grpc", "service_name": p["path"]}
		if p["host"] != "" {
			t["authority"] = p["host"]
		}
		base["transport"] = t
	case "http":
		t := map[string]any{"type": "http", "path": orDefault(p["path"], "/")}
		if h := orDefault(p["host"], serverName(p, "")); h != "" {
			t["host"] = []string{h}
		}
		base["transport"] = t
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
