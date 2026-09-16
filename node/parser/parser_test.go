package parser

import "testing"

// TestBadWSPathUpstreamResidue 上游漏 "&" 导致参数粘进值里时必须被识别。
func TestBadWSPathUpstreamResidue(t *testing.T) {
	bad := []string{
		"/?ed=2560security=tls",
		"/path?ed=2048security=tls",
		"/?ed=2048type=ws",
		"/?foo=barhost=evil.com",
	}
	for _, p := range bad {
		if !badWSPath(p) {
			t.Errorf("badWSPath(%q) = false，应为畸形", p)
		}
	}
}

// TestBadWSPathKeepsNormal 正常路径必须放行，避免误杀真实节点。
// 注意："/?type=ws&security=tls" 是合法的多参数 query，必须放行。
func TestBadWSPathKeepsNormal(t *testing.T) {
	good := []string{
		"/",
		"/pai50288",
		"/?ed=2560",
		"/?ed=2048&foo=bar",
		"/?type=ws&security=tls",
		"/howdy",
		"/---@MiTiVPN---@MiTiVPN/s-w",
		"",
	}
	for _, p := range good {
		if badWSPath(p) {
			t.Errorf("badWSPath(%q) = true，误杀了正常路径", p)
		}
	}
}

// TestParseURIDropsMalformedPath 端到端：畸形 path 的 URI 应被解析器拒绝。
func TestParseURIDropsMalformedPath(t *testing.T) {
	uri := "vless://47fcef29-ab4e-4aa6-932b-d95a18f28a4e@162.159.48.32:2082" +
		"?security=none&type=ws&path=%2F%3Fed%3D2560security%3Dtls&host=example.com#test"
	_, err := ParseURI(uri)
	if err == nil {
		t.Fatal("畸形 path 的 URI 应被拒绝，但解析成功了")
	}
}

// TestParseURIKeepsHealthyWS 健康 ws 节点必须照常解析。
func TestParseURIKeepsHealthyWS(t *testing.T) {
	uri := "vless://47616b8c-d9f4-4e5a-9b6e-1a2b3c4d5e6f@1.2.3.4:443" +
		"?security=tls&type=ws&path=%2Fws&host=example.com&sni=example.com#ok"
	n, err := ParseURI(uri)
	if err != nil {
		t.Fatalf("健康节点解析失败: %v", err)
	}
	if n.Params["path"] != "/ws" {
		t.Errorf("path = %q，期望 /ws", n.Params["path"])
	}
}
