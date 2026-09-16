package core

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// 用本地 httptest 模拟 CDN，验证 RefreshPool 的拉取、新旧判定与原子落盘。
func TestRefreshPoolFromLocalCDN(t *testing.T) {
	remote := map[string]any{
		"updated": time.Now().UTC().Add(time.Hour).Format(time.RFC3339),
		"nodes": []map[string]any{
			{"id": "aaa", "protocol": "vless", "server": "1.1.1.1", "port": 443,
				"state": "AVAILABLE", "first_seen": "2026-09-16T00:00:00Z",
				"last_checked": "2026-09-16T00:00:00Z", "latency_ms": 120},
			{"id": "bbb", "protocol": "ss", "server": "2.2.2.2", "port": 8388,
				"state": "AVAILABLE", "first_seen": "2026-09-16T00:00:00Z",
				"last_checked": "2026-09-16T00:00:00Z", "latency_ms": 200},
		},
	}
	body, _ := json.Marshal(remote)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(body)
	}))
	defer srv.Close()

	dir := t.TempDir()
	st := &Settings{PoolPath: filepath.Join(dir, "pool.json"), PoolURL: srv.URL}
	m := NewManager(st, dir)

	logf := func(string, ...any) {}
	src, total, err := m.RefreshPool(logf)
	if err != nil {
		t.Fatalf("首次刷新失败: %v", err)
	}
	if total != 2 {
		t.Fatalf("期望 2 个节点，实际 %d", total)
	}
	if src == "" {
		t.Fatal("来源名不应为空")
	}

	// 落盘校验
	b, err := os.ReadFile(st.PoolPath)
	if err != nil {
		t.Fatalf("落盘文件不存在: %v", err)
	}
	var got map[string]any
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatalf("落盘 JSON 非法: %v", err)
	}
	if nodes, _ := got["nodes"].([]any); len(nodes) != 2 {
		t.Fatalf("落盘节点数应为 2，实际 %d", len(nodes))
	}

	// 远端未更新时二次刷新不应报错
	if _, _, err := m.RefreshPool(logf); err != nil {
		t.Fatalf("二次刷新不应失败: %v", err)
	}
}

func TestPoolStale(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "pool.json")

	// 文件缺失 → 过期
	if !PoolStale(p, time.Hour) {
		t.Error("文件缺失应判定为过期")
	}

	// 刚写入 → 新鲜
	pool := map[string]any{
		"updated": time.Now().UTC().Format(time.RFC3339),
		"nodes":   []map[string]any{{"id": "x", "protocol": "ss", "server": "1.1.1.1", "port": 80}},
	}
	b, _ := json.Marshal(pool)
	if err := os.WriteFile(p, b, 0o644); err != nil {
		t.Fatal(err)
	}
	if PoolStale(p, time.Hour) {
		t.Error("刚写入的池应判定为新鲜")
	}
	// 超过极小时限 → 过期
	if !PoolStale(p, time.Nanosecond) {
		t.Error("超过极小时限应判定为过期")
	}
}

// rawFallback 应能把 jsDelivr 地址正确转成 raw.githubusercontent 地址。
func TestRawFallback(t *testing.T) {
	got := rawFallback("https://cdn.jsdelivr.net/gh/Ainxin-1/NovaLink@main/data/pool.json")
	want := "https://raw.githubusercontent.com/Ainxin-1/NovaLink/main/data/pool.json"
	if got != want {
		t.Fatalf("rawFallback 结果错误:\n got %s\nwant %s", got, want)
	}
}
