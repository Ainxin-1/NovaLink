package checker

import (
	"net/url"
	"strings"
	"testing"
)

// TestProbeURLIsNotDirectlyReachable 锁住探测目标的核心约束：
// 目标必须"运行环境直连不可达"，否则直连就能返回 204，死节点也会被判可用。
//
// 这是用真实事故换来的规则（2026-09-16）：探测目标曾是
// https://www.gstatic.com/generate_204，该域名国内直连 0.3s 返回 204，
// 导致整池节点被误判为"可用"，节点池质量长期失真。
func TestProbeURLIsNotDirectlyReachable(t *testing.T) {
	u, err := url.Parse(ProbeURL())
	if err != nil {
		t.Fatalf("探测目标不是合法 URL: %v", err)
	}
	if u.Scheme != "https" {
		t.Errorf("探测目标必须用 HTTPS（明文可被伪造应答欺骗），当前 %q", u.Scheme)
	}
	// 已知国内直连可达、绝不可作为探测目标的域名
	banned := []string{"gstatic.com", "cloudflare.com", "baidu.com", "qq.com"}
	host := strings.ToLower(u.Host)
	for _, b := range banned {
		if strings.HasSuffix(host, b) {
			t.Errorf("探测目标指向 %s —— 该域名大陆直连可达，无法验证翻墙能力，"+
				"会导致死节点被误判为可用", host)
		}
	}
}

// TestProbeURLEndsWithGenerate204 探测端点应是轻量 204 端点。
func TestProbeURLEndsWithGenerate204(t *testing.T) {
	if !strings.HasSuffix(ProbeURL(), "/generate_204") {
		t.Errorf("探测目标应以 /generate_204 结尾（轻量无 body），当前 %q", ProbeURL())
	}
}

// TestProbeURLEnvOverride 环境变量覆盖生效（CI 需要按自身网络环境选择目标）。
func TestProbeURLEnvOverride(t *testing.T) {
	t.Setenv("NOVANODE_PROBE_URL", "https://www.youtube.com/generate_204")
	if got := ProbeURL(); got != "https://www.youtube.com/generate_204" {
		t.Errorf("环境变量覆盖失败: got %q", got)
	}
	t.Setenv("NOVANODE_PROBE_URL", "")
	if got := ProbeURL(); got != defaultProbeURL {
		t.Errorf("空环境变量应回落默认值: got %q", got)
	}
}
