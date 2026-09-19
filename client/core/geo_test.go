package core

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func nopLog() func(string, ...any) { return func(string, ...any) {} }

func TestCnRouteAlwaysPrefersPrivateDirect(t *testing.T) {
	// 没有任何 geo 文件时也必须产出私网直连，且不能引用不存在的 rule_set
	rs, rules := cnRoute(nil)
	if len(rs) != 0 {
		t.Errorf("无规则集时不应产出 rule_set，实得 %d 条", len(rs))
	}
	if len(rules) != 1 {
		t.Fatalf("应仅有私网直连 1 条，实得 %d", len(rules))
	}
	if _, ok := rules[0].(map[string]any)["ip_is_private"]; !ok {
		t.Errorf("首条规则应为 ip_is_private 直连: %v", rules[0])
	}
}

func TestCnRouteUsesEveryLandedRuleSet(t *testing.T) {
	rs, rules := cnRoute(map[string]string{
		"geoip-cn":   "D:/x/geoip-cn.srs",
		"geosite-cn": "D:/x/geosite-cn.srs",
	})
	if len(rs) != 2 {
		t.Fatalf("rule_set 条数 = %d，期望 2", len(rs))
	}
	if len(rules) != 3 { // 私网 + geoip-cn + geosite-cn
		t.Fatalf("rules 条数 = %d，期望 3", len(rules))
	}
	for _, one := range rs {
		m := one.(map[string]any)
		// sing-box 1.11 起 rule_set 的 type 只能是 local/remote/inline，
		// 写成 binary 会让核心直接 FATAL，整批连接失败。
		if m["type"] != "local" || m["format"] != "binary" {
			t.Errorf("rule_set 字段不合法: %v", m)
		}
		if p, _ := m["path"].(string); strings.Contains(p, "\\") {
			t.Errorf("path 需为斜杠形式（Windows 反斜杠在 JSON 里易出错）: %q", p)
		}
	}
}

func TestLocalRuleSetsIgnoresTruncatedFile(t *testing.T) {
	dir := t.TempDir()
	rules := filepath.Join(dir, "rules")
	if err := os.MkdirAll(rules, 0o755); err != nil {
		t.Fatal(err)
	}
	full := strings.Repeat("A", ruleMinSize+10)
	brief := "半截下载" // 小于阈值：被打断的下载不能当有效规则集
	if err := os.WriteFile(filepath.Join(rules, "geoip-cn.srs"), []byte(full), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(rules, "geosite-cn.srs"), []byte(brief), 0o644); err != nil {
		t.Fatal(err)
	}
	got := LocalRuleSets(dir)
	if _, ok := got["geoip-cn"]; !ok {
		t.Errorf("完整规则集应被识别: %v", got)
	}
	if _, ok := got["geosite-cn"]; ok {
		t.Errorf("不完整（%d 字节）的规则集不得判为可用", len(brief))
	}
}

// TestEnsureRuleSetsWorksOffline 断网也必须拿到规则集：随包分发。
// 早先这里是"运行时下载"，而本机线路上 jsDelivr 会被 RST、raw 会整批超时，
// 等于把分流失效当成常态。
func TestEnsureRuleSetsWorksOffline(t *testing.T) {
	dir := t.TempDir()
	var logs []string
	got := EnsureRuleSets(dir, func(f string, a ...any) { logs = append(logs, f) })
	if len(got) != len(geoRuleSets) {
		t.Fatalf("仅 %d/%d 规则集就绪: %v", len(got), len(geoRuleSets), got)
	}
	joined := strings.Join(logs, "\n")
	if !strings.Contains(joined, "未联网") {
		t.Errorf("应从随包资源解出，日志: %s", joined)
	}
	for _, r := range geoRuleSets {
		b, err := os.ReadFile(got[r.tag])
		if err != nil {
			t.Fatalf("读 %s: %v", r.tag, err)
		}
		emb, err := embeddedRules.ReadFile("rules/" + r.file)
		if err != nil {
			t.Fatal(err)
		}
		if int64(len(b)) != int64(len(emb)) {
			t.Errorf("%s 落地后大小 %d ≠ 随包 %d", r.tag, len(b), len(emb))
		}
	}
}

// TestEmbeddedRuleSetsMatchCNOnly 用真实核心验规则集的**语义**：
// 国内域名/IP 命中、被墙域名不得命中。
// 这条锁的是"换错规则文件"——例如某些第三方 geosite-cn 把 google-cn 之类
// 也并了进来，那会让 google 走直连，表现是"代理开着却上不了外网"，
// 而且不会有任何报错。
func TestEmbeddedRuleSetsMatchCNOnly(t *testing.T) {
	bin := coreBin(t)
	dir := t.TempDir()
	rules := EnsureRuleSets(dir, nopLog())
	if len(rules) != len(geoRuleSets) {
		t.Fatalf("规则集未就绪: %v", rules)
	}
	cases := []struct {
		file string
		want map[string]bool
	}{
		{"geosite-cn.srs", map[string]bool{
			"www.baidu.com": true, "tv.cctv.com": true, "www.taobao.com": true,
			"www.google.com": false, "www.youtube.com": false, "github.com": false,
		}},
		{"geoip-cn.srs", map[string]bool{
			"114.114.114.114": true, "223.5.5.5": true,
			"8.8.8.8": false, "104.17.208.5": false,
		}},
	}
	for _, c := range cases {
		path := filepath.Join(dir, "rules", c.file)
		for target, shouldMatch := range c.want {
			out, err := exec.Command(bin, "rule-set", "match", "-f", "binary", path, target).CombinedOutput()
			matched := err == nil && strings.Contains(string(out), "match")
			if matched != shouldMatch {
				t.Errorf("%s 对 %s：命中=%v，期望=%v（输出 %s）", c.file, target, matched, shouldMatch, firstLine(out))
			}
		}
	}
}

func coreBin(t *testing.T) string {
	t.Helper()
	bin := os.Getenv("NOVALINK_SINGBOX")
	if bin == "" {
		bin = "E:/NovaLink/core/vpn-core/sing-box-1.14.0-windows-amd64/sing-box.exe"
	}
	if _, err := os.Stat(bin); err != nil {
		t.Skip("未找到 sing-box 核心")
	}
	return bin
}

// TestGeoRouteConfigAcceptedByCore 用真实核心校验带分流的路由段。
// 字段名写错的表现是核心 FATAL 拒启，用户侧看到的是"点了连接没反应"。
func TestGeoRouteConfigAcceptedByCore(t *testing.T) {
	bin := coreBin(t)
	dir := t.TempDir()
	rules := EnsureRuleSets(dir, nopLog())
	if len(rules) == 0 {
		t.Skip("本机无规则集且下载不可达，跳过")
	}
	ruleSet, routeRules := cnRoute(rules)
	cfg := map[string]any{
		"inbounds":  []any{map[string]any{"type": "mixed", "tag": "i", "listen": "127.0.0.1", "listen_port": 39990}},
		"outbounds": []any{map[string]any{"type": "direct", "tag": "proxy"}, map[string]any{"type": "direct", "tag": "direct"}},
		"route":     map[string]any{"final": "proxy", "rule_set": ruleSet, "rules": routeRules},
	}
	b, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	p := filepath.Join(t.TempDir(), "cfg.json")
	if err := os.WriteFile(p, b, 0o644); err != nil {
		t.Fatal(err)
	}
	if out, err := exec.Command(bin, "check", "-c", p).CombinedOutput(); err != nil {
		t.Fatalf("sing-box check 失败: %v\n%s\n配置:\n%s", err, out, b)
	}
}
