package core

import (
	"net"
	"testing"
	"time"

	"novanode/model"
)

// 预筛应把 TCP 不可达的节点沉到末尾，可达的保持在前面。
// 用本机监听端口构造「可达」，用保留的不可路由地址构造「不可达」。
func TestSetFailoverPoolLocalReorders(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			_ = c.Close()
		}
	}()
	_, portStr, _ := net.SplitHostPort(ln.Addr().String())
	port := 0
	for _, ch := range portStr {
		port = port*10 + int(ch-'0')
	}

	// 不可达地址：TEST-NET-1（RFC 5737，保证不可路由）
	dead1 := &model.Node{ID: "dead1", Server: "192.0.2.1", Port: 9}
	dead2 := &model.Node{ID: "dead2", Server: "192.0.2.2", Port: 9}
	alive := &model.Node{ID: "alive", Server: "127.0.0.1", Port: port}

	// 故意把死节点放前面，模拟「池子头部是死区」
	nodes := []*model.Node{dead1, dead2, alive}

	dir := t.TempDir()
	m := NewManager(&Settings{PoolPath: dir + "/pool.json"}, dir)
	m.SetFailoverPoolLocal(nodes, nil)

	m.mu.Lock()
	got := make([]string, len(m.pool))
	for i, n := range m.pool {
		got[i] = n.ID
	}
	m.mu.Unlock()

	if len(got) != 3 {
		t.Fatalf("池长度应为 3，实际 %d", len(got))
	}
	if got[0] != "alive" {
		t.Fatalf("可达节点应排首位，实际顺序 %v", got)
	}
	// 死节点应仍在池内（作为兜底）
	seen := map[string]bool{}
	for _, id := range got {
		seen[id] = true
	}
	if !seen["dead1"] || !seen["dead2"] {
		t.Fatalf("不可达节点不应被丢弃，实际 %v", got)
	}
}

// 空池不应 panic。
func TestSetFailoverPoolLocalEmpty(t *testing.T) {
	dir := t.TempDir()
	m := NewManager(&Settings{PoolPath: dir + "/pool.json"}, dir)
	m.SetFailoverPoolLocal(nil, nil)
	m.SetFailoverPoolLocal([]*model.Node{}, nil)
	time.Sleep(10 * time.Millisecond)
}
