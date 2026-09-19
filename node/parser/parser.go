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
//
// 支持范围即本管线能发布出去的范围。早先只认 ss/trojan/vless/vmess 四类，
// hysteria2/tuic/anytls 整类被当作"非法行"丢弃 —— 而 2026-09-19 国内实测中
// 活下来的 18 个节点里 15 个是裸 IP + 随机高端口，正是这一类跑在独立服务器上
// 的 QUIC 节点。丢协议等于丢供给。
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
	case strings.HasPrefix(uri, "hysteria2://"), strings.HasPrefix(uri, "hy2://"):
		return parseHysteria2(uri)
	case strings.HasPrefix(uri, "tuic://"):
		return parseTuic(uri)
	case strings.HasPrefix(uri, "anytls://"):
		return parseAnyTLS(uri)
	default:
		return model.Node{}, fmt.Errorf("unsupported scheme")
	}
}

// uriParts 是 scheme://凭据@host:port?query#name 形态 URI 的切分结果。
type uriParts struct {
	raw    string // 原始 URI，作为节点参数随池子保存
	cred   string
	server string
	port   int
	query  string
	frag   string
}

// splitCredURI 切分凭据型 URI，vless/trojan/hysteria2/tuic/anytls 共用。
func splitCredURI(uri string) (uriParts, error) {
	i := strings.Index(uri, "://")
	if i < 0 {
		return uriParts{}, fmt.Errorf("no scheme")
	}
	rest := uri[i+3:]
	p := uriParts{raw: uri}
	if j := strings.IndexByte(rest, '#'); j >= 0 {
		p.frag, rest = rest[j+1:], rest[:j]
	}
	if j := strings.IndexByte(rest, '?'); j >= 0 {
		p.query, rest = rest[j+1:], rest[:j]
	}
	// host:port 后可能跟一个空 path（hy2://pw@1.1.1.1:443/?insecure=1#n）
	rest = strings.TrimSuffix(rest, "/")
	k := strings.LastIndexByte(rest, '@')
	if k < 0 {
		return uriParts{}, fmt.Errorf("no userinfo")
	}
	p.cred = rest[:k]
	server, port, err := hostport(rest[k+1:])
	if err != nil {
		return uriParts{}, err
	}
	if badServer(server) {
		return uriParts{}, fmt.Errorf("reserved server address")
	}
	p.server, p.port = server, port
	return p, nil
}

func unescape(s string) string {
	if v, err := url.QueryUnescape(s); err == nil {
		return v
	}
	return s
}

// tlsParams 收齐 TLS 相关参数。早先只取 sni/path/host/flow/pbk/sid，
// fp（uTLS 指纹）、alpn、insecure 直接丢掉，导致部分节点在客户端配不出来。
func tlsParams(p uriParts) map[string]string {
	m := map[string]string{"uri": p.raw}
	sni := qget(p.query, "sni")
	if sni == "" {
		sni = qget(p.query, "peer") // hysteria2 常用 peer 表示 SNI
	}
	if sni == "" {
		sni = qget(p.query, "host")
	}
	m["sni"] = sni
	m["alpn"] = qget(p.query, "alpn")
	m["fp"] = qget(p.query, "fp")
	if qget(p.query, "insecure") == "1" || qget(p.query, "allow_insecure") == "1" ||
		qget(p.query, "allowinsecure") == "true" {
		m["insecure"] = "1"
	}
	return m
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

// badWSPath 识别上游 URI 拼接残留造成的畸形 WebSocket 路径。
//
// 典型样本（真实数据）：
//
//	path=/?ed=2560security=tls        ← 漏了 "&"，security 参数被粘进前一个值
//	path=/?ed=2048type=ws
//
// 判据：在 query 段的**值**里发现了另一个已知参数名（或其 "=" 形式），
// 说明上游拼接时缺少分隔符。正常路径（如 /?ed=2560&foo=bar）不会触发。
func badWSPath(path string) bool {
	i := strings.IndexByte(path, '?')
	if i < 0 {
		// 没有 query 段：整体形如 "xxx=yyy" 且不以 / 开头，属于拼接残留
		return strings.Contains(path, "=") && !strings.HasPrefix(path, "/")
	}
	// 已知参数名 —— 出现在值里即为拼接残留
	keys := []string{"security=", "type=", "host=", "path=", "sni=", "fp=",
		"alpn=", "headertype=", "allowinsecure=", "encryption=", "flow="}
	for _, part := range strings.Split(path[i+1:], "&") {
		j := strings.IndexByte(part, '=')
		if j < 0 {
			continue
		}
		val := strings.ToLower(part[j+1:])
		for _, k := range keys {
			if strings.Contains(val, k) {
				return true
			}
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
		ID:   model.Fingerprint("ss", server, port, params),
		Name: name, Protocol: "ss", Server: server, Port: port, Params: params,
	}, nil
}

// trojan / vless 共用 URI 形态: scheme://凭据@host:port?query#name
func parseTrojanLike(uri, proto string) (model.Node, error) {
	p, err := splitCredURI(uri)
	if err != nil {
		return model.Node{}, err
	}
	cred := unescape(p.cred)
	if cred == "" {
		return model.Node{}, fmt.Errorf("empty credential")
	}
	net := qget(p.query, "type")
	if net == "" || net == "raw" {
		net = "tcp"
	}
	if net == "h2" {
		net = "http" // sing-box 1.11 起 h2 传输更名为 http
	}
	if net != "tcp" && net != "ws" && net != "grpc" && net != "http" {
		return model.Node{}, fmt.Errorf("unsupported transport %s", net)
	}
	sec := qget(p.query, "security")
	params := tlsParams(p)
	params["password"] = cred
	params["net"] = net
	params["path"] = qget(p.query, "path")
	params["host"] = qget(p.query, "host")
	params["flow"] = qget(p.query, "flow")
	params["pbk"] = qget(p.query, "pbk")
	params["sid"] = qget(p.query, "sid")
	params["spx"] = qget(p.query, "spx") // reality spiderX，原先丢失
	if sec == "tls" || sec == "reality" {
		params["security"] = sec
	}
	if pbk := params["pbk"]; pbk != "" && !model.ValidPublicKey(pbk) {
		return model.Node{}, fmt.Errorf("invalid reality public_key")
	}
	if params["net"] == "ws" && params["path"] != "" {
		if _, err := url.Parse(params["path"]); err != nil {
			return model.Node{}, fmt.Errorf("invalid ws path")
		}
		// 上游 URI 拼接残留：形如 /?ed=2560security=tls —— 作者漏了 &，
		// 把下一个参数粘进了 path。服务端不会认这个路径，节点必然失败，直接丢弃。
		if badWSPath(params["path"]) {
			return model.Node{}, fmt.Errorf("malformed ws path (upstream missing separator)")
		}
	}
	if proto == "vless" {
		params["uuid"] = cred
	}
	return model.Node{
		ID:   model.Fingerprint(proto, p.server, p.port, params),
		Name: unescape(p.frag), Protocol: proto, Server: p.server, Port: p.port, Params: params,
	}, nil
}

func newNode(proto string, p uriParts, params map[string]string) model.Node {
	return model.Node{
		ID:   model.Fingerprint(proto, p.server, p.port, params),
		Name: unescape(p.frag), Protocol: proto, Server: p.server, Port: p.port, Params: params,
	}
}

func orDefault(v, def string) string {
	if v == "" {
		return def
	}
	return v
}

// parseHysteria2 解析 hysteria2://（别名 hy2://）。QUIC/UDP 上的独立服务器节点，
// 不依赖 Cloudflare CDN —— 国内实测里唯一还有活口的免费节点类别。
func parseHysteria2(uri string) (model.Node, error) {
	p, err := splitCredURI(uri)
	if err != nil {
		return model.Node{}, err
	}
	pw := unescape(p.cred)
	if pw == "" {
		return model.Node{}, fmt.Errorf("empty credential")
	}
	params := tlsParams(p)
	params["password"] = pw
	if obfs := qget(p.query, "obfs"); obfs != "" && obfs != "none" {
		params["obfs"] = obfs
		params["obfs_password"] = qget(p.query, "obfs-password")
	}
	return newNode("hysteria2", p, params), nil
}

// parseTuic 解析 tuic://（凭据为 uuid:password 两段）。
func parseTuic(uri string) (model.Node, error) {
	p, err := splitCredURI(uri)
	if err != nil {
		return model.Node{}, err
	}
	i := strings.IndexByte(p.cred, ':')
	if i <= 0 || i == len(p.cred)-1 {
		return model.Node{}, fmt.Errorf("bad tuic credential")
	}
	params := tlsParams(p)
	params["uuid"] = unescape(p.cred[:i])
	params["password"] = unescape(p.cred[i+1:])
	params["congestion_control"] = orDefault(qget(p.query, "congestion_control"), "bbr")
	params["udp_relay_mode"] = orDefault(qget(p.query, "udp_relay_mode"), "native")
	return newNode("tuic", p, params), nil
}

// parseAnyTLS 解析 anytls://。TCP+TLS，靠 sni 完成握手。
func parseAnyTLS(uri string) (model.Node, error) {
	p, err := splitCredURI(uri)
	if err != nil {
		return model.Node{}, err
	}
	pw := unescape(p.cred)
	if pw == "" {
		return model.Node{}, fmt.Errorf("empty credential")
	}
	params := tlsParams(p)
	params["password"] = pw
	if obfs := qget(p.query, "obfs"); obfs != "" && obfs != "none" {
		params["obfs"] = obfs
		params["obfs_password"] = qget(p.query, "obfs-password")
	}
	return newNode("anytls", p, params), nil
}

func parseVmess(uri string) (model.Node, error) {
	// 订阅里 vmess 普遍带 #名称 后缀，必须先剥掉再解 base64，
	// 否则 StdEncoding 会在 '#' 处报 illegal base64 —— 整条节点被丢弃。
	body := strings.TrimPrefix(uri, "vmess://")
	frag := ""
	if i := strings.IndexByte(body, '#'); i >= 0 {
		frag, body = body[i+1:], body[:i]
	}
	dec, err := decodeB64(body)
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
	if net == "" || net == "raw" {
		net = "tcp"
	}
	if net == "h2" {
		net = "http"
	}
	if net != "tcp" && net != "ws" && net != "grpc" && net != "http" {
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
		"fp": get("fp"), "alpn": get("alpn"),
	}
	if get("insecure") == "1" || get("allowInsecure") == "1" {
		params["insecure"] = "1"
	}
	if net == "ws" && params["path"] != "" {
		if _, err := url.Parse(params["path"]); err != nil {
			return model.Node{}, fmt.Errorf("invalid ws path")
		}
	}
	return model.Node{
		ID:       model.Fingerprint("vmess", server, port, params),
		Name:     orDefault(get("ps"), unescape(frag)),
		Protocol: "vmess", Server: server, Port: port, Params: params,
	}, nil
}

func fragment(uri string) string {
	if i := strings.IndexByte(uri, '#'); i >= 0 {
		return uri[i+1:]
	}
	return ""
}
