// Package sources 负责多来源节点获取（任务书第八/九/十章）。
// 每个来源独立获取、独立记录结果，单个来源失败不影响其他来源。
package sources

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	"novanode/model"
)

const fetchTimeout = 30 * time.Second

// FetchAll 并发获取所有启用的来源，返回各来源文本。
// 任何来源失败只记录到对应 SourceResult，不产生全局错误。
// Type=index 的来源是"目录源"：抓取文本后自动提取其中的订阅 URL，
// 每个子 URL 作为独立子来源抓取（借鉴 ghboost 的做法）。
func FetchAll(cfgs []model.SourceConfig) (map[string]string, []model.SourceResult) {
	var mu sync.Mutex
	texts := map[string]string{}
	results := make([]model.SourceResult, 0, len(cfgs))
	var wg sync.WaitGroup
	for _, cfg := range cfgs {
		if !cfg.Enabled {
			continue
		}
		wg.Add(1)
		go func(cfg model.SourceConfig) {
			defer wg.Done()
			if cfg.Type == "index" {
				fetchIndex(cfg, &mu, texts, &results)
				return
			}
			text, err := fetchOne(cfg.URL)
			mu.Lock()
			defer mu.Unlock()
			r := model.SourceResult{Name: cfg.Name, FetchedAt: now()}
			if err != nil {
				r.Error = err.Error()
			} else {
				r.OK = true
				texts[cfg.Name] = text
			}
			results = append(results, r)
		}(cfg)
	}
	wg.Wait()
	return texts, results
}

// indexCap 限制单个目录源展开的子来源数量，避免一次抓取失控。
const indexCap = 40

func fetchIndex(cfg model.SourceConfig, mu *sync.Mutex, texts map[string]string, results *[]model.SourceResult) {
	text, err := fetchOne(cfg.URL)
	if err != nil {
		mu.Lock()
		*results = append(*results, model.SourceResult{Name: cfg.Name, Error: err.Error(), FetchedAt: now()})
		mu.Unlock()
		return
	}
	urls := extractSubscriptionURLs(text, indexCap)
	mu.Lock()
	*results = append(*results, model.SourceResult{Name: cfg.Name, OK: true, NodeCount: len(urls), FetchedAt: now()})
	mu.Unlock()
	if len(urls) == 0 {
		return
	}
	var wg sync.WaitGroup
	for i, u := range urls {
		wg.Add(1)
		go func(i int, u string) {
			defer wg.Done()
			t, err := fetchOne(u)
			name := fmt.Sprintf("%s#%02d", cfg.Name, i+1)
			mu.Lock()
			defer mu.Unlock()
			r := model.SourceResult{Name: name, FetchedAt: now()}
			if err != nil {
				r.Error = err.Error()
			} else {
				r.OK = true
				texts[name] = t
			}
			*results = append(*results, r)
		}(i, u)
	}
	wg.Wait()
}

// extractSubscriptionURLs 从目录文本提取疑似订阅地址：
// 跳过 blob 页面、纯域名首页与明显非订阅链接，去重并限量。
func extractSubscriptionURLs(text string, cap int) []string {
	re := regexp.MustCompile(`https?://[^\s)\x60"'<>]+`)
	seen := map[string]bool{}
	out := []string{}
	for _, raw := range re.FindAllString(text, -1) {
		u := strings.TrimRight(raw, ".,;，。；")
		if seen[u] {
			continue
		}
		seen[u] = true
		l := strings.ToLower(u)
		switch {
		case strings.Contains(l, "/blob/"), // GitHub 网页页签，非原始文件
			strings.HasSuffix(l, "/"), // 目录/首页
			strings.Count(strings.TrimPrefix(strings.TrimPrefix(l, "https://"), "http://"), "/") < 1: // 纯域名
			continue
		}
		out = append(out, u)
		if len(out) >= cap {
			break
		}
	}
	return out
}

func fetchOne(rawURL string) (string, error) {
	switch {
	case strings.HasPrefix(rawURL, "file://"):
		p := strings.TrimPrefix(rawURL, "file://")
		// Windows 盘符路径: file:///E:/x -> /E:/x，去掉盘符前的斜杠
		if len(p) >= 3 && p[0] == '/' && p[2] == ':' {
			p = p[1:]
		}
		b, err := os.ReadFile(filepath.FromSlash(p))
		if err != nil {
			return "", err
		}
		return string(b), nil
	case strings.HasPrefix(rawURL, "http://"), strings.HasPrefix(rawURL, "https://"):
		client := &http.Client{Timeout: fetchTimeout}
		req, err := http.NewRequest(http.MethodGet, rawURL, nil)
		if err != nil {
			return "", err
		}
		req.Header.Set("User-Agent", "NovaLink-Pipeline/0.1")
		resp, err := client.Do(req)
		if err != nil {
			return "", err
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return "", &statusError{code: resp.StatusCode}
		}
		b, err := io.ReadAll(io.LimitReader(resp.Body, 32<<20)) // 32MB 上限
		if err != nil {
			return "", err
		}
		return string(b), nil
	default:
		return "", &schemeError{url: rawURL}
	}
}

func now() string { return time.Now().UTC().Format(time.RFC3339) }

type statusError struct{ code int }

func (e *statusError) Error() string { return fmt.Sprintf("HTTP %d", e.code) }

type schemeError struct{ url string }

func (e *schemeError) Error() string { return "unsupported source url scheme" }
