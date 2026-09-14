// 云端节点池订阅刷新（NovaLink 2.0 完善项）。
//
// 管线在 GitHub Actions 每 2 小时发布最新节点池，客户端通过本模块拉取：
//
//	1) jsDelivr（先 purge 缓存保证新鲜）
//	2) raw.githubusercontent（直连，常被墙但作为备选）
//	3) 上述两源经本机 socks5 127.0.0.1:10808 代理重试（如端口开放）
//	4) 全部失败保持本地文件不动
//
// 拉取结果仅在"云端比本地新"时原子覆盖本地 pool.json。
package core

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"time"

	"novanode/cache"
	"novanode/model"
)

// DefaultPoolURL 默认云端订阅地址（仓库 CI 发布产物）。
const DefaultPoolURL = "https://cdn.jsdelivr.net/gh/Ainxin-1/NovaLink@main/data/pool.json"

const (
	proxySocksAddr  = "127.0.0.1:10808" // 本机常见代理端口（v2rayN 默认）
	fetchTimeout    = 12 * time.Second
	defaultStaleAge = 6 * time.Hour // 本地池超过该时长视为过期，启动时自动刷新
)

// fetchSource 一个可尝试的下载源。
type fetchSource struct {
	name string
	url  string
	sock bool // 是否经本机 socks5 代理
}

// RefreshPool 拉取云端节点池并按新旧覆盖本地文件。
// 返回命中的来源名与池内节点总数。
func (m *Manager) RefreshPool(logf func(string, ...any)) (source string, total int, err error) {
	url := m.settings.PoolURL
	if url == "" {
		url = DefaultPoolURL
	}
	// 从订阅 URL 推导 raw 回退地址（仅识别 jsDelivr 的 gh 链接）
	raw := rawFallback(url)

	sources := []fetchSource{
		{"jsDelivr", url, false},
		{"raw.githubusercontent", raw, false},
	}
	if socksOpen() {
		sources = append(sources,
			fetchSource{"jsDelivr(代理)", url, true},
			fetchSource{"raw(代理)", raw, true},
		)
	}

	local, lerr := cache.Load(m.settings.PoolPath)
	for _, s := range sources {
		pool, ferr := fetchPool(s)
		if ferr != nil {
			logf("[POOL] 来源 %s 失败: %v", s.name, ferr)
			continue
		}
		total = len(pool.Nodes)
		// 新旧判定：本地读取失败（无文件/损坏）或云端更新时间更晚才覆盖
		if lerr == nil && local != nil && pool.Updated != "" && local.Updated != "" &&
			pool.Updated <= local.Updated {
			logf("[POOL] 云端(%s)更新时间 %s 不晚于本地 %s，无需刷新", s.name, pool.Updated, local.Updated)
			return s.name + "(已是最新)", len(local.Nodes), nil
		}
		if serr := cache.Save(pool, m.settings.PoolPath); serr != nil {
			return s.name, total, fmt.Errorf("保存节点池失败: %w", serr)
		}
		logf("[POOL] 已从 %s 更新节点池：%d 个节点（池更新时间 %s）", s.name, total, pool.Updated)
		return s.name, total, nil
	}
	return "", 0, fmt.Errorf("所有订阅源均不可达，保留本地节点池")
}
// PoolStale 本地节点池是否已过期（文件缺失/损坏/更新时间超过 maxAge）。
func PoolStale(path string, maxAge time.Duration) bool {
	pool, err := cache.Load(path)
	if err != nil || pool == nil || len(pool.Nodes) == 0 {
		return true
	}
	upd, err := time.Parse(time.RFC3339, pool.Updated)
	if err != nil {
		return true
	}
	return time.Since(upd) > maxAge
}

// fetchPool 从单一来源下载并解析节点池。
func fetchPool(s fetchSource) (*model.Pool, error) {
	if s.name == "jsDelivr" || s.name == "jsDelivr(代理)" {
		// 先 purge CDN 缓存，保证拿到最近一次 Actions 的产物（失败忽略）
		purge := http.Client{Timeout: 6 * time.Second, Transport: transportFor(s.sock)}
		resp, err := purge.Get("https://purge.jsdelivr.net/gh/Ainxin-1/NovaLink@main/data/pool.json")
		if err == nil {
			_ = resp.Body.Close()
		}
	}
	client := http.Client{Timeout: fetchTimeout, Transport: transportFor(s.sock)}
	resp, err := client.Get(s.url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	var pool model.Pool
	if err := json.NewDecoder(resp.Body).Decode(&pool); err != nil {
		return nil, err
	}
	return &pool, nil
}

// rawFallback 从 jsDelivr gh 地址推导 raw.githubusercontent 地址。
func rawFallback(cdnURL string) string {
	const prefix = "https://cdn.jsdelivr.net/gh/"
	if len(cdnURL) > len(prefix) && cdnURL[:len(prefix)] == prefix {
		rest := cdnURL[len(prefix):] // owner/repo@branch/path
		for i := 0; i < len(rest); i++ {
			if rest[i] == '@' {
				ownerRepo, tail := rest[:i], rest[i+1:]
				for j := 0; j < len(tail); j++ {
					if tail[j] == '/' {
						return "https://raw.githubusercontent.com/" + ownerRepo + "/" + tail[:j] + "/" + tail[j+1:]
					}
				}
			}
		}
	}
	return "https://raw.githubusercontent.com/Ainxin-1/NovaLink/main/data/pool.json"
}

// transportFor 直连或走本机 socks5 代理。
func transportFor(sock bool) *http.Transport {
	if !sock {
		return &http.Transport{}
	}
	return &http.Transport{Proxy: func(*http.Request) (*url.URL, error) {
		return url.Parse("socks5://127.0.0.1:10808")
	}}
}

// socksOpen 探测本机 socks5 代理端口是否开放。
func socksOpen() bool {
	conn, err := net.DialTimeout("tcp", proxySocksAddr, 400*time.Millisecond)
	if err != nil {
		return false
	}
	_ = conn.Close()
	return true
}
