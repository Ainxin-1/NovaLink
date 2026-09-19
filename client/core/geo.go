// 国内直连分流（geo-cn 规则集）。
//
// 早先客户端生成的路由只有 {"final":"proxy"}：淘宝、微信更新、Windows 更新、
// 内网设备全部塞进隧道。于是一个 900ms 的节点能把本来 3ms 的国内站一起拖慢，
// 节点一抖全站卡死 —— 用户感知的"不稳定/高延迟"有相当比例出在这里，
// 与节点本身质量无关。
package core

import (
	"bytes"
	"embed"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
)

//go:embed rules
var embeddedRules embed.FS

// geoRuleSets 随包分发的规则集（tag → 文件名 / 上游刷新地址）。
//
// 为什么打进二进制而不是运行时下载：分流规则一旦拿不到，国内站点就得
// 陪着一起绕隧道（实测一个 900ms 节点能让 3ms 的国内站一起卡）。
// 而这条线路上的 jsDelivr 会被 RST、raw 会整批超时 —— 把一个纯优化项
// 挂在最不靠谱的链路上，是拿稳定性换一个"安装包小 500KB"。
// 上游：SagerNet/sing-geoip 与 sing-geosite 的 rule-set 分支（.srs 二进制）。
var geoRuleSets = []struct {
	tag, file, repo string
}{
	{"geoip-cn", "geoip-cn.srs", "SagerNet/sing-geoip"},
	{"geosite-cn", "geosite-cn.srs", "SagerNet/sing-geosite"},
}

// geoMirrors 刷新用的镜像，按"国内线路实测"排序而非理论可用性。
var geoMirrors = []string{
	"https://raw.githubusercontent.com/%s/rule-set/%s",
	"https://cdn.jsdelivr.net/gh/%s@rule-set/%s",
	"https://testingcf.jsdelivr.net/gh/%s@rule-set/%s",
	"https://fastly.jsdelivr.net/gh/%s@rule-set/%s",
}

const (
	ruleMinSize   = 1024                // 小于此值不可能是完整规则集（多半是被打断的下载）
	ruleRefreshAt = 30 * 24 * time.Hour // 随包规则超过该时长才尝试联网刷新
)

// LocalRuleSets 只报告本机已落地的规则集，不做任何网络 IO —— 供生成核心配置使用。
func LocalRuleSets(dataDir string) map[string]string {
	dir := filepath.Join(dataDir, "rules")
	got := map[string]string{}
	for _, r := range geoRuleSets {
		if p := filepath.Join(dir, r.file); usableFile(p) {
			got[r.tag] = p
		}
	}
	return got
}

// EnsureRuleSets 保证 geo-cn 规则集落地在 dataDir/rules 下，返回 tag→路径（仅含可用的）。
//
// 顺序：盘上已有（不旧）→ 从随包资源解出（离线可用）→ 联网刷新。
// 任何一级失败都只是"这一轮不分流"，退回全域代理，不影响连接；
// 联网刷新失败时继续用旧的那份，规则旧一点远好过分流失效。
func EnsureRuleSets(dataDir string, logf func(string, ...any)) map[string]string {
	dir := filepath.Join(dataDir, "rules")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		logf("[GEO] 建规则目录失败: %v", err)
		return nil
	}
	got := map[string]string{}
	for _, r := range geoRuleSets {
		dst := filepath.Join(dir, r.file)
		switch {
		case usableFile(dst):
			if fileTooOld(dst) {
				downloadRule(dst, r.repo, r.file, logf) // 失败也照用旧文件
			}
		case writeEmbedded(dst, r.file, logf):
		case downloadRule(dst, r.repo, r.file, logf):
		default:
			logf("[GEO] %s 拿不到，本轮不分流（国内站点也会走隧道）", r.tag)
			continue
		}
		got[r.tag] = dst
	}
	return got
}

func usableFile(path string) bool {
	st, err := os.Stat(path)
	return err == nil && st.Size() >= ruleMinSize
}

func fileTooOld(path string) bool {
	st, err := os.Stat(path)
	return err != nil || time.Since(st.ModTime()) > ruleRefreshAt
}

// writeEmbedded 把随包分发的规则集解到目标路径。
func writeEmbedded(dst, file string, logf func(string, ...any)) bool {
	b, err := embeddedRules.ReadFile("rules/" + file)
	if err != nil {
		logf("[GEO] 随包规则 %s 缺失: %v", file, err)
		return false
	}
	if err := writeFileFrom(dst, bytes.NewReader(b)); err != nil {
		logf("[GEO] 写入随包规则 %s 失败: %v", file, err)
		return false
	}
	logf("[GEO] %s 已从随包资源解出（%d 字节，未联网）", file, len(b))
	return true
}

// downloadRule 依次尝试各镜像（直连优先，再经本机 socks5）。
func downloadRule(dst, repo, file string, logf func(string, ...any)) bool {
	type attempt struct {
		url  string
		sock bool
	}
	var attempts []attempt
	for _, m := range geoMirrors {
		attempts = append(attempts, attempt{fmt.Sprintf(m, repo, file), false})
	}
	if socksOpen() {
		for _, m := range geoMirrors {
			attempts = append(attempts, attempt{fmt.Sprintf(m, repo, file), true})
		}
	}
	for _, a := range attempts {
		if err := fetchToFile(a.url, dst, a.sock); err != nil {
			logf("[GEO] %s 经 %s(%s) 失败: %v", file, hostOf(a.url), mode(a.sock), err)
			continue
		}
		logf("[GEO] %s 已从 %s(%s) 下载", file, hostOf(a.url), mode(a.sock))
		return true
	}
	return false
}

func mode(sock bool) string {
	if sock {
		return "代理"
	}
	return "直连"
}

func fetchToFile(u, dst string, sock bool) error {
	client := &http.Client{Timeout: 30 * time.Second, Transport: transportFor(sock)}
	resp, err := client.Get(u)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	// 上限 32MB：规则集实际只有几十到几百 KB，超限说明拿到的是垃圾响应
	if err := writeFileFrom(dst, io.LimitReader(resp.Body, 32<<20)); err != nil {
		return err
	}
	if !usableFile(dst) {
		return fmt.Errorf("内容过小，判为不完整")
	}
	return nil
}

// writeFileFrom 原子落盘：先写 .tmp，再改名替换。
func writeFileFrom(dst string, r io.Reader) error {
	tmp := dst + ".tmp"
	out, err := os.Create(tmp)
	if err != nil {
		return err
	}
	_, err = io.Copy(out, r)
	if cerr := out.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if err := os.Remove(dst); err != nil && !os.IsNotExist(err) {
		_ = os.Remove(tmp)
		return err
	}
	return os.Rename(tmp, dst)
}

func hostOf(u string) string {
	if i := strings.Index(u, "://"); i > 0 {
		rest := u[i+3:]
		if j := strings.IndexByte(rest, '/'); j > 0 {
			return rest[:j]
		}
	}
	return u
}

// cnRoute 由已落地的规则集生成 sing-box 的 route.rule_set 与 route.rules。
// 私网直连无条件保留；geo 缺失只是不分流，不影响核心启动。
func cnRoute(rules map[string]string) (ruleSet []any, routeRules []any) {
	routeRules = append(routeRules, map[string]any{
		"ip_is_private": true, "outbound": "direct",
	})
	for _, r := range geoRuleSets {
		path, ok := rules[r.tag]
		if !ok {
			continue
		}
		ruleSet = append(ruleSet, map[string]any{
			"type": "local", "tag": r.tag, "format": "binary", "path": filepath.ToSlash(path),
		})
		routeRules = append(routeRules, map[string]any{
			"rule_set": []string{r.tag}, "outbound": "direct",
		})
	}
	return ruleSet, routeRules
}
