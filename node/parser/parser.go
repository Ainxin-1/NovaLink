// Package parser 将各协议节点 URI 解析为统一 Node 结构并做格式检查
// （任务书第二阶段：节点解析 / 节点格式检查）。
package parser

import (
	"encoding/base64"
	"fmt"
	"net"
	"net/url"
	"regexp"
	"strconv"
	"strings"

	"novanode/model"
)

var schemeRe = regexp.MustCompile(`^[a-z0-9]+://`)

// ParseLines 解析多行节点文本，返回合法节点；非法行计数丢弃。
// 行内容可能是明文 URI，也可能是整段 base64 编码的 URI 列表。
func ParseLines(text string) (nodes []model.Node, dropped int) {
	text = strings.TrimSpace(text)
	if text != "" && !schemeRe.MatchString(firstLine(text)) {
		if dec, err := decodeB64(strings.Map(func(r rune) rune {
			if r == '\r' || r == '\n' || r == ' ' {
				return -1
			}
			return r
		}, text)); err == nil && schemeRe.MatchString(firstLine(dec)) {
			text = dec
		}
	}
	seen := map[string]bool{}
	for _, line := range strings.Split(text, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") || strings.HasPrefix(line, "//") {
			continue
		}
		n, err := ParseURI(line)
		if err != nil {
			dropped++
			continue
		}
		if seen[n.ID] {
			continue
		}
		seen[n.ID] = true
		nodes = append(nodes, n)
	}
	return nodes, dropped
}

// ParseURI 解析单条节点 URI。
func ParseURI(uri string) (model.Node, error) {
	switch {
	case strings.HasPrefix(uri, "ss://"):
		return parseSS(uri)
	case strings.HasPrefix(uri, "trojan://"):
		return parseTrojanLike(uri, "trojan")
	case strings.HasPrefix(uri, "vless://"):
		return parseTrojanLike(uri, "vless")
	case strings.HasPrefix(uri, "vmess://"):
		return parseVmess(uri)
	default:
		return model.Node{}, fmt.Errorf("unsupported scheme")
	}
}

// decodeB64 解码 URL-safe / 标准 base64，自动补齐 padding。
func decodeB64(s string) (string, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return "", fmt.Errorf("empty")
	}
	if pad := len(s) % 4; pad != 0 {
		s += strings.Repeat("=", 4-pad)
	}
	if b, err := base64.StdEncoding.DecodeString(s); err == nil {
		return string(b), nil
	}
	b, err := base64.URLEncoding.DecodeString(s)
	return string(b), err
}

func firstLine(s string) string {
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		return s[:i]
	}
	return s
}

// qget 从 a=1&b=2 形式的查询串取值。
func qget(query, key string) string {
	for _, kv := range strings.Split(query, "&") {
		if i := strings.IndexByte(kv, '='); i > 0 && kv[:i] == key {
			v, err := url.QueryUnescape(kv[i+1:])
			if err != nil {
				return kv[i+1:]
			}
			return v
		}
	}
	return ""
}

func hostport(hp string) (server string, port int, err error) {
	hp = strings.Trim(hp, "[]")
	i := strings.LastIndexByte(hp, ':')
	if i < 0 {
		return "", 0, fmt.Errorf("no port")
	}
	port, err = strconv.Atoi(hp[i+1:])
	if err != nil || port < 1 || port > 65535 {
		return "", 0, fmt.Errorf("bad port")
	}
	server = strings.Trim(hp[:i], "[]")
	if server == "" {
		return "", 0, fmt.Errorf("empty server")
	}
	return server, port, nil
}

// badServer 过滤明显无效的节点地址：回环、未指定、链路本地、
// 内网保留段（目录源常见垃圾数据），这些地址不可能构成公网节点。
func badServer(server string) bool {
	host := strings.Trim(server, "[]")
	ip := net.ParseIP(host)
	if ip == nil {
		return false // 域名交由后续检测判断
	}
	if ip.IsLoopback() || ip.IsUnspecified() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() {
		return true
	}
	if v4 := ip.To4(); v4 != nil {
		if v4[0] == 10 || v4[0] == 0 {
			return true
		}
		if v4[0] == 172 && v4[1] >= 16 && v4[1] <= 31 {
			return true
		}
		if v4[0] == 192 && v4[1] == 168 {
			return true
		}
	}
	return false
}

func parseSS(uri string) (model.Node, error) {
	main := strings.TrimPrefix(uri, "ss://")
	main = strings.SplitN(main, "#", 2)[0]
	if strings.Contains(main, "?") { // 带 plugin 的暂不支持
		return model.Node{}, fmt.Errorf("ss with plugin unsupported")
	}
	var userinfo, hp string
	if i := strings.LastIndexByte(main, '@'); i >= 0 {
		userinfo, hp = main[:i], main[i+1:]
	} else { // 旧式整体 base64: b64(method:password@host:port)
		dec, err := decodeB64(main)
		if err != nil {
			return model.Node{}, err
		}
		j := strings.LastIndexByte(dec, '@')
		if j < 0 {
			return model.Node{}, fmt.Errorf("bad legacy ss")
		}
		userinfo, hp = dec[:j], dec[j+1:]
	}
	server, port, err := hostport(hp)
	if err != nil {
		return model.Node{}, err
	}
	if badServer(server) {
		return model.Node{}, fmt.Errorf("reserved server address")
	}
	cred, err := decodeB64(userinfo)
	if err != nil {
		return model.Node{}, err
	}
	i := strings.IndexByte(cred, ':')
	if i <= 0 {
		return model.Node{}, fmt.Errorf("bad ss credential")
	}
	method, password := cred[:i], cred[i+1:]
	if method == "" || password == "" {
		return model.Node{}, fmt.Errorf("empty ss credential parts")
	}
	name, _ := url.QueryUnescape(fragment(uri))
	params := map[string]string{"uri": uri, "method": method, "password": password}
	return model.Node{
		ID: model.Fingerprint("ss", server, port, params),
		Name: name, Protocol: "ss", Server: server, Port: port, Params: params,
	}, nil
}

// trojan / vless 共用 URI 形态: scheme://凭据@host:port?query#name
func parseTrojanLike(uri, proto string) (model.Node, error) {
	main := strings.TrimPrefix(uri, proto+"://")
	var frag string
	if i := strings.IndexByte(main, '#'); i >= 0 {
		frag = main[i+1:]
		main = main[:i]
	}
	var query string
	if i := strings.IndexByte(main, '?'); i >= 0 {
		query = main[i+1:]
		main = main[:i]
	}
	i := strings.LastIndexByte(main, '@')
	if i < 0 {
		return model.Node{}, fmt.Errorf("no userinfo")
	}
	cred := main[:i]
	server, port, err := hostport(main[i+1:])
	if err != nil {
		return model.Node{}, err
	}
	if badServer(server) {
		return model.Node{}, fmt.Errorf("reserved server address")
	}
	password, err := url.QueryUnescape(cred)
	if err != nil {
		password = cred
	}
	if password == "" {
		return model.Node{}, fmt.Errorf("empty credential")
	}
	net := qget(query, "type")
	if net == "" {
		net = "tcp"
	}
	if net != "tcp" && net != "ws" && net != "grpc" {
		return model.Node{}, fmt.Errorf("unsupported transport %s", net)
	}
	sec := qget(query, "security")
	sni := qget(query, "sni")
	if sni == "" {
		sni = qget(query, "host")
	}
	params := map[string]string{
		"uri": uri, "password": password, "net": net,
		"sni": sni, "path": qget(query, "path"), "host": qget(query, "host"),
		"flow": qget(query, "flow"), "pbk": qget(query, "pbk"), "sid": qget(query, "sid"),
	}
	if sec == "tls" || sec == "reality" || net == "tls" {
		params["security"] = sec
	}
	if pbk := params["pbk"]; pbk != "" && !model.ValidPublicKey(pbk) {
		return model.Node{}, fmt.Errorf("invalid reality public_key")
	}
	if params["net"] == "ws" && params["path"] != "" {
		if _, err := url.Parse(params["path"]); err != nil {
			return model.Node{}, fmt.Errorf("invalid ws path")
		}
	}
	if proto == "vless" {
		params["uuid"] = password
	}
	name, _ := url.QueryUnescape(frag)
	return model.Node{
		ID: model.Fingerprint(proto, server, port, params),
		Name: name, Protocol: proto, Server: server, Port: port, Params: params,
	}, nil
}

func parseVmess(uri string) (model.Node, error) {
	dec, err := decodeB64(strings.TrimPrefix(uri, "vmess://"))
	if err != nil {
		return model.Node{}, err
	}
	get := func(key string) string {
		re := regexp.MustCompile(`"` + key + `"\s*:\s*"?([^",}]*)"?`)
		if m := re.FindStringSubmatch(dec); m != nil {
			return m[1]
		}
		return ""
	}
	server := get("add")
	port, err := strconv.Atoi(get("port"))
	if err != nil || server == "" {
		return model.Node{}, fmt.Errorf("bad vmess addr")
	}
	if badServer(server) {
		return model.Node{}, fmt.Errorf("reserved server address")
	}
	id := get("id")
	if id == "" {
		return model.Node{}, fmt.Errorf("empty vmess uuid")
	}
	net := get("net")
	if net == "" {
		net = "tcp"
	}
	if net != "tcp" && net != "ws" && net != "grpc" {
		return model.Node{}, fmt.Errorf("unsupported transport %s", net)
	}
	sni := get("sni")
	if sni == "" {
		sni = get("host")
	}
	params := map[string]string{
		"uri": uri, "uuid": id, "net": net, "sni": sni,
		"path": get("path"), "host": get("host"),
		"aid": get("aid"), "security": get("scy"), "tls": get("tls"),
	}
	if net == "ws" && params["path"] != "" {
		if _, err := url.Parse(params["path"]); err != nil {
			return model.Node{}, fmt.Errorf("invalid ws path")
		}
	}
	return model.Node{
		ID: model.Fingerprint("vmess", server, port, params),
		Name: get("ps"), Protocol: "vmess", Server: server, Port: port, Params: params,
	}, nil
}

func fragment(uri string) string {
	if i := strings.IndexByte(uri, '#'); i >= 0 {
		return uri[i+1:]
	}
	return ""
}
