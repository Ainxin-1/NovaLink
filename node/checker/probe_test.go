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

// TestPassVerdict 判定口径必须与客户端连接验证一致（3 目标里 ≥2）。
// 单目标判可用会产出"能到 google 却上不了 YouTube"的节点：本地实测判它可用、
// 进了候选组，连接时又被 2/3 规则拒掉（2026-09-19 实测出现"有可用节点却连不上"）。
func TestPassVerdict(t *testing.T) {
	cases := []struct {
		ok, total int
		want      bool
	}{
		{3, 3, true}, {2, 3, true}, {1, 3, false}, {0, 3, false},
		{1, 1, true}, {0, 1, false}, {1, 2, true}, {0, 0, false},
	}
	for _, c := range cases {
		if got := PassVerdict(c.ok, c.total); got != c.want {
			t.Errorf("PassVerdict(%d,%d) = %v，期望 %v", c.ok, c.total, got, c.want)
		}
	}
}

// TestProbeTargetsMultiAndDirectUnreachable 多目标里的每一个都不许国内直连可达。
func TestProbeTargetsMultiAndDirectUnreachable(t *testing.T) {
	t.Setenv("NOVANODE_PROBE_URL", "")
	urls := ProbeTargets()
	if len(urls) < 3 {
		t.Fatalf("默认应至少 3 个目标，实得 %d", len(urls))
	}
	bad := []string{"gstatic", "cloudflare", "baidu", "qq.com", "miui", "vivo"}
	for _, u := range urls {
		if !strings.HasPrefix(u, "https://") {
			t.Errorf("目标必须 HTTPS（明文可被伪造应答）: %s", u)
		}
		if !strings.HasSuffix(u, "/generate_204") {
			t.Errorf("目标应为轻量 204 端点: %s", u)
		}
		for _, b := range bad {
			if strings.Contains(u, b) {
				t.Errorf("目标 %s 含 %s：国内直连可达或明文，判别力为零", u, b)
			}
		}
	}
	// 环境变量覆盖时退化为单目标口径
	t.Setenv("NOVANODE_PROBE_URL", "https://example.com/generate_204")
	if got := ProbeTargets(); len(got) != 1 || got[0] != "https://example.com/generate_204" {
		t.Errorf("NOVANODE_PROBE_URL 未生效: %v", got)
	}
}
