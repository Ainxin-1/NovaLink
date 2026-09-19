package publish

import (
	"encoding/base64"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"novanode/model"
	"novanode/parser"
)

func b64(s string) string { return base64.StdEncoding.EncodeToString([]byte(s)) }

func mustNode(t *testing.T, uri string) *model.Node {
	t.Helper()
	n, err := parser.ParseURI(uri)
	if err != nil {
		t.Fatalf("解析 %q 失败: %v", uri, err)
	}
	return &n
}

// TestOutboundBareIPHasNoServerName 裸 IP 节点不得把 IP 填进 server_name。
// 非法 SNI 会让握手必败，表现为"TCP 通、协议不通"，整类节点被误判为死。
func TestOutboundBareIPHasNoServerName(t *testing.T) {
	n := mustNode(t, "trojan://pw@203.0.113.9:443?security=tls#ip-only")
	ob := Outbound(n)
	if ob == nil {
		t.Fatal("Outbound 返回 nil")
	}
	tls, _ := ob["tls"].(map[string]any)
	if tls == nil {
		t.Fatal("trojan 应带 tls 段")
	}
	if _, ok := tls["server_name"]; ok {
		t.Errorf("裸 IP 节点不应有 server_name，实得 %v", tls["server_name"])
	}
}

// TestOutboundCarriesTLSSettings uTLS 指纹 / alpn / insecure 此前被解析器收了却没落到配置里。
func TestOutboundCarriesTLSSettings(t *testing.T) {
	n := mustNode(t, "hy2://pw@203.0.113.9:443/?insecure=1&alpn=h3&fp=firefox&obfs=salamander&obfs-password=op&sni=a.com#x")
	ob := Outbound(n)
	if ob["type"] != "hysteria2" {
		t.Fatalf("type = %v", ob["type"])
	}
	tls, _ := ob["tls"].(map[string]any)
	if tls["insecure"] != true {
		t.Errorf("insecure 未落到配置: %v", tls)
	}
	if tls["server_name"] != "a.com" {
		t.Errorf("server_name = %v", tls["server_name"])
	}
	if got, _ := tls["alpn"].([]string); len(got) != 1 || got[0] != "h3" {
		t.Errorf("alpn = %v", tls["alpn"])
	}
	u, _ := tls["utls"].(map[string]any)
	if u["fingerprint"] != "firefox" {
		t.Errorf("utls 指纹 = %v，期望 firefox", u["fingerprint"])
	}
	if ob["obfs"] == nil {
		t.Errorf("obfs 未落到配置")
	}
}

// TestOutboundAllProtocols 每种受支持协议都要能产出可序列化的 outbound，
// 返回 nil 会让节点在检测与发布两条链路上被静默丢弃。
func TestOutboundAllProtocols(t *testing.T) {
	uris := []string{
		"ss://YWVzLTI1Ni1nY206cHc@203.0.113.9:8080#ss",
		"trojan://pw@203.0.113.9:443?security=tls&sni=a.com#tj",
		"vless://uuid-1@203.0.113.9:443?security=tls&type=ws&path=%2Fws&host=a.com&sni=a.com#vl-ws",
		"vless://uuid-2@203.0.113.9:443?security=reality&sni=a.com&pbk=" +
			"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA&sid=6ba8&fp=chrome&type=tcp#vl-reality",
		"vmess://" + b64(`{"add":"203.0.113.9","port":"443","id":"uuid-v1","net":"ws","path":"/ws","host":"a.com","tls":"tls","sni":"a.com","aid":"0","scy":"auto"}`) + "#vmess",
		"hy2://pw@203.0.113.9:59924/?insecure=1&sni=a.com#hy2",
		"tuic://00000000-0000-0000-0000-000000000000:pw@203.0.113.9:443?sni=a.com&alpn=h3#tuic",
		"anytls://pw@203.0.113.9:8443?sni=a.com#anytls",
	}
	var obs []any
	for _, u := range uris {
		n := mustNode(t, u)
		ob := Outbound(n)
		if ob == nil {
			t.Errorf("%s 节点 Outbound 返回 nil（会被静默丢弃）", n.Protocol)
			continue
		}
		if ob["tag"] != n.ID {
			t.Errorf("%s tag 非节点指纹", n.Protocol)
		}
		obs = append(obs, ob)
	}
	if len(obs) != len(uris) {
		t.Fatalf("仅 %d/%d 产出 outbound", len(obs), len(uris))
	}
	coreOutboundConfigValid(t, obs)
}

// coreOutboundConfigValid 用真实 sing-box 校验生成的 outbound 段。
// 内核配置一旦非法，深检会二分退化成逐节点失败，客户端也连不上，
// 且两处都不报错——所以必须让机器来判字段名，而不是靠人记。
// 找不到核心二进制时跳过（CI 与本地路径不同）。
func coreOutboundConfigValid(t *testing.T, obs []any) {
	t.Helper()
	bin := os.Getenv("NOVALINK_SINGBOX")
	if bin == "" {
		bin = "E:/NovaLink/core/vpn-core/sing-box-1.14.0-windows-amd64/sing-box.exe"
	}
	if _, err := os.Stat(bin); err != nil {
		t.Skip("未找到 sing-box 核心，跳过配置合法性校验")
	}
	cfg := map[string]any{
		"inbounds": []any{map[string]any{
			"type": "mixed", "tag": "in", "listen": "127.0.0.1", "listen_port": 39998,
		}},
		"outbounds": append(obs, map[string]any{"type": "direct", "tag": "d"}),
		"route":     map[string]any{"final": "d"},
	}
	b, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	p := filepath.Join(dir, "cfg.json")
	if err := os.WriteFile(p, b, 0o644); err != nil {
		t.Fatal(err)
	}
	out, err := exec.Command(bin, "check", "-c", p).CombinedOutput()
	if err != nil {
		t.Fatalf("sing-box check 失败: %v\n%s\n配置:\n%s", err, out, b)
	}
}
