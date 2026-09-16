// 探测目标约束的单元测试。
//
// 核心规则（2026-09-16 真实事故）：探测目标必须"大陆直连不可达"，
// 否则直连就能返回 204，死节点也会被判成功，健康检查形同虚设。
// 事故背景：probeTargets 曾含 gstatic / cloudflare，二者国内直连均可达
// （实测 gstatic 0.3s 返回 204），导致"节点可用"判定长期假阳性。
package core

import (
	"net/url"
	"strings"
	"testing"
)

// bannedProbeHosts 国内直连可达、绝不可作为"是否翻墙成功"判据的域名。
var bannedProbeHosts = []string{"gstatic.com", "cloudflare.com", "baidu.com", "qq.com"}

// TestProbeTargetsExcludeDirectlyReachable probeTargets 不得包含大陆直连可达域名。
func TestProbeTargetsExcludeDirectlyReachable(t *testing.T) {
	if len(probeTargets) == 0 {
		t.Fatal("probeTargets 不能为空")
	}
	for _, raw := range probeTargets {
		u, err := url.Parse(raw)
		if err != nil {
			t.Errorf("probeTarget %q 不是合法 URL: %v", raw, err)
			continue
		}
		if u.Scheme != "https" {
			t.Errorf("probeTarget %q 必须用 HTTPS（明文可被伪造应答）", raw)
		}
		host := strings.ToLower(u.Host)
		for _, b := range bannedProbeHosts {
			if strings.HasSuffix(host, b) {
				t.Errorf("probeTarget %q 指向 %s —— 大陆直连可达，会把死节点判成可用", raw, b)
			}
		}
	}
}

// TestProbeHTTPNotDirectlyReachable probeHTTP（内核 urltest 用）同样受此约束。
func TestProbeHTTPNotDirectlyReachable(t *testing.T) {
	u, err := url.Parse(probeHTTP)
	if err != nil {
		t.Fatalf("probeHTTP 不是合法 URL: %v", err)
	}
	host := strings.ToLower(u.Host)
	for _, b := range bannedProbeHosts {
		if strings.HasSuffix(host, b) {
			t.Errorf("probeHTTP 指向 %s —— 大陆直连可达，urltest 会选择假可用节点", host)
		}
	}
}

// TestDirectTargetsAreDirectlyReachable directTargets 的职责相反：
// 它用于区分"本地网络断了"与"节点坏了"，因此必须是国内可达目标。
func TestDirectTargetsAreDirectlyReachable(t *testing.T) {
	if len(directTargets) == 0 {
		t.Fatal("directTargets 不能为空")
	}
	for _, raw := range directTargets {
		u, err := url.Parse(raw)
		if err != nil {
			t.Errorf("directTarget %q 不是合法 URL: %v", raw, err)
			continue
		}
		host := strings.ToLower(u.Host)
		for _, b := range bannedProbeHosts {
			if strings.HasSuffix(host, b) {
				t.Errorf("directTarget %q 指向 %s，但 %s 在国内并非稳定可达，"+
					"用它判断本地网络会误判", raw, host, b)
			}
		}
	}
}
