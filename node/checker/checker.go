// Package checker 对节点做基础可用性粗筛（任务书第二阶段/第十四章：
// Node Pipeline 负责粗筛，最终可用性以客户端设备端实测为准）。
package checker

import (
	"net"
	"strconv"
	"sync"
	"time"

	"novanode/model"
)

// Result 是单个节点的粗筛结果。
type Result struct {
	ID        string
	OK        bool
	LatencyMS int
}

// TCP 并发对节点地址做 TCP 连通性测试。
// 这是服务端环境下的粗筛，结果不代表用户设备上的真实可用性。
func TCP(nodes []model.Node, timeout time.Duration, workers int) map[string]Result {
	if workers <= 0 {
		workers = 16
	}
	sem := make(chan struct{}, workers)
	var mu sync.Mutex
	out := map[string]Result{}
	var wg sync.WaitGroup
	for i := range nodes {
		n := nodes[i]
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			start := time.Now()
			conn, err := net.DialTimeout("tcp", net.JoinHostPort(n.Server, strconv.Itoa(n.Port)), timeout)
			lat := int(time.Since(start).Milliseconds())
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				out[n.ID] = Result{ID: n.ID, OK: false}
				return
			}
			conn.Close()
			out[n.ID] = Result{ID: n.ID, OK: true, LatencyMS: lat}
		}()
	}
	wg.Wait()
	return out
}
