package parser

import (
	"strings"
	"testing"
)

// TestParseHysteria2 hysteria2/hy2 曾被整类当作非法行丢弃，
// 而国内实测里活下来的节点大多属于这一类（独立 IP + 随机高端口）。
func TestParseHysteria2(t *testing.T) {
	cases := map[string]string{
		"hy2://MyPasswd@203.0.113.9:59924/?insecure=1&sni=beacon.example.com#tokyo-1": "hysteria2",
		"hysteria2://pw2@host.example.org:443/?obfs=salamander&obfs-password=opw#sf":  "hysteria2",
	}
	for uri, wantProto := range cases {
		n, err := ParseURI(uri)
		if err != nil {
			t.Fatalf("解析 %q 失败: %v", uri, err)
		}
		if n.Protocol != wantProto {
			t.Errorf("Protocol = %q，期望 %q", n.Protocol, wantProto)
		}
		if n.Server == "" || n.Port == 0 {
			t.Errorf("server/port 未解析: %+v", n)
		}
		if n.Params["password"] == "" {
			t.Errorf("password 未解析")
		}
	}
	n, _ := ParseURI("hy2://pw@203.0.113.9:59924/?insecure=1&obfs=salamander&obfs-password=opw&peer=a.com#x")
	if n.Params["obfs"] != "salamander" || n.Params["obfs_password"] != "opw" {
		t.Errorf("obfs 参数丢失: %q/%q", n.Params["obfs"], n.Params["obfs_password"])
	}
	if n.Params["insecure"] != "1" {
		t.Errorf("insecure 未识别: %q", n.Params["insecure"])
	}
	if n.Params["sni"] != "a.com" {
		t.Errorf("peer 应回落为 sni，实得 %q", n.Params["sni"])
	}
	if _, err := ParseURI("hy2://@203.0.113.9:443/#empty"); err == nil {
		t.Errorf("空密码 hy2 应被拒绝")
	}
}

// TestParseTuicAndAnyTLS tuic 凭据是 uuid:password 两段；anytls 只有密码。
func TestParseTuicAndAnyTLS(t *testing.T) {
	n, err := ParseURI("tuic://00000000-0000-0000-0000-000000000000:sec@203.0.113.9:443" +
		"?congestion_control=bbr&udp_relay_mode=native&sni=a.com&alpn=h3&allow_insecure=0#t")
	if err != nil {
		t.Fatalf("tuic 解析失败: %v", err)
	}
	if n.Protocol != "tuic" || n.Params["uuid"] != "00000000-0000-0000-0000-000000000000" ||
		n.Params["password"] != "sec" || n.Params["congestion_control"] != "bbr" {
		t.Errorf("tuic 字段解析异常: %+v", n.Params)
	}
	a, err := ParseURI("anytls://token123@203.0.113.9:8443?sni=a.com#any")
	if err != nil {
		t.Fatalf("anytls 解析失败: %v", err)
	}
	if a.Protocol != "anytls" || a.Params["password"] != "token123" {
		t.Errorf("anytls 字段解析异常: %+v", a.Params)
	}
	if _, err := ParseURI("tuic://nocolon@203.0.113.9:443#bad"); err == nil {
		t.Errorf("缺少 : 分隔的 tuic 凭据应被拒绝")
	}
}

// TestParseSupportsHTTPAndRawTransport type=h2/http 与 type=raw 此前直接判非法。
func TestParseSupportsHTTPAndRawTransport(t *testing.T) {
	n, err := ParseURI("vless://uuid-here@203.0.113.9:443?security=tls&type=h2&path=/x&host=a.com#h2")
	if err != nil {
		t.Fatalf("h2 传输应被支持: %v", err)
	}
	if n.Params["net"] != "http" {
		t.Errorf("h2 应归一为 sing-box 的 http，实得 %q", n.Params["net"])
	}
	if _, err := ParseURI("trojan://pw@203.0.113.9:443?security=tls&type=raw#raw"); err != nil {
		t.Errorf("raw 传输应等价 tcp，被拒: %v", err)
	}
	if _, err := ParseURI("vless://u@203.0.113.9:443?type=meek#x"); err == nil {
		t.Errorf("未知传输仍应被拒绝")
	}
}

// TestFingerprintSeparatesDistinctParams 同 host:port:password 但 obfs 不同
// 是两个不同节点。指纹若忽略该参数会把它们并成一个，节点池就此丢节点。
func TestFingerprintSeparatesDistinctParams(t *testing.T) {
	base := "hy2://pw@203.0.113.9:443/#n"
	other := "hy2://pw@203.0.113.9:443/?obfs=salamander&obfs-password=opw#n"
	a, err := ParseURI(base)
	if err != nil {
		t.Fatal(err)
	}
	b, err := ParseURI(other)
	if err != nil {
		t.Fatal(err)
	}
	if a.ID == b.ID {
		t.Errorf("不同 obfs 的节点指纹相同（%s），会被去重并成一个", a.ID)
	}
	same := "hy2://pw@203.0.113.9:443/?obfs=salamander&obfs-password=opw#另一个来源的名字"
	c, err := ParseURI(same)
	if err != nil {
		t.Fatal(err)
	}
	if c.ID != b.ID {
		t.Errorf("仅 #名称 不同的同一节点指纹不同: %q vs %q", c.ID, b.ID)
	}
}

// TestParseVmessWithFragment vmess 订阅普遍带 #名称，
// 不先剥片段就解 base64 会整条报错丢弃。
func TestParseVmessWithFragment(t *testing.T) {
	body := "eyJhZGQiOiIyMDMuMC4xMTMuOSIsInBvcnQiOiI0NDMiLCJpZCI6InV1aWQtdjEiLCJuZXQiOiJ3cyIsInBhdGgiOiIvd3MiLCJob3N0IjoiYS5jb20iLCJ0bHMiOiJ0bHMiLCJzbmkiOiJhLmNvbSIsImFpZCI6IjAiLCJwcyI6Iue+juWbvSJ9"
	n, err := ParseURI("vmess://" + body + "#-US-自由节点")
	if err != nil {
		t.Fatalf("带 #名称 的 vmess 必须能解析: %v", err)
	}
	if n.Name != "美国" {
		t.Errorf("Name = %q，期望取 ps 字段", n.Name)
	}
	if n.Params["sni"] != "a.com" || n.Params["net"] != "ws" {
		t.Errorf("vmess 参数解析异常: %+v", n.Params)
	}
	n2, err := ParseURI("vmess://" + "eyJhZGQiOiIyMDMuMC4xMTMuOSIsInBvcnQiOiI0NDMiLCJpZCI6InUiLCJuZXQiOiJ0Y3AifQ==" + "#frag名")
	if err != nil {
		t.Fatalf("无 ps 字段的 vmess 应回退到片段名: %v", err)
	}
	if n2.Name != "frag名" {
		t.Errorf("Name = %q，期望 frag名", n2.Name)
	}
}

// TestParseLinesCountsSupported 端到端：新协议不得再进 dropped 计数。
func TestParseLinesCountsSupported(t *testing.T) {
	text := strings.Join([]string{
		"hy2://pw@203.0.113.9:59924/?insecure=1#1",
		"tuic://00000000-0000-0000-0000-000000000000:sec@203.0.113.9:443#2",
		"anytls://tok@203.0.113.9:8443?sni=a.com#3",
		"not-a-node-line",
	}, "\n")
	nodes, dropped := ParseLines(text)
	if len(nodes) != 3 {
		t.Errorf("解析出 %d 个节点，期望 3", len(nodes))
	}
	if dropped != 1 {
		t.Errorf("dropped = %d，期望仅 1（非法行）", dropped)
	}
}
