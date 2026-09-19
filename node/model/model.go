// Package model 定义 NovaLink 节点管线统一数据结构（任务书第十一章）。
package model

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"sort"
	"strconv"
)

// 节点状态（任务书第十三章）。
const (
	StateNew       = "NEW"
	StateTesting   = "TESTING"
	StateAvailable = "AVAILABLE"
	StateDegraded  = "DEGRADED"
	StateFailed    = "FAILED"
	StateExpired   = "EXPIRED"
	StateRemoved   = "REMOVED"
)

// Node 是节点池中的统一节点结构。
// Params 保存协议敏感字段（密码/UUID 等），禁止写入日志。
type Node struct {
	ID          string            `json:"id"` // 节点指纹（任务书第十二章）
	Name        string            `json:"name"`
	Protocol    string            `json:"protocol"` // ss / trojan / vmess / vless
	Server      string            `json:"server"`
	Port        int               `json:"port"`
	Params      map[string]string `json:"params"` // 含原始 uri
	Sources     []string          `json:"sources"`
	FirstSeen   string            `json:"first_seen"`
	LastUpdated string            `json:"last_updated"`
	LastChecked string            `json:"last_checked,omitempty"`
	LastSuccess string            `json:"last_success,omitempty"`
	State       string            `json:"state"`
	FailCount   int               `json:"fail_count"`
	SlowCount   int               `json:"slow_count,omitempty"`
	LatencyMS   int               `json:"latency_ms,omitempty"`
}

// Pool 是节点池的持久化形态（任务书第十一章/第十六章缓存）。
type Pool struct {
	Updated string  `json:"updated"`
	Nodes   []*Node `json:"nodes"`
}

// SourceConfig 是节点来源配置（任务书第八章）。
type SourceConfig struct {
	Name    string `json:"name"`
	URL     string `json:"url"`  // http(s):// 或 file://
	Type    string `json:"type"` // auto / uri / base64
	Enabled bool   `json:"enabled"`
	Note    string `json:"note,omitempty"`
}

// SourceResult 记录单次来源获取结果（任务书第八章：来源独立记录）。
type SourceResult struct {
	Name      string `json:"name"`
	OK        bool   `json:"ok"`
	Error     string `json:"error,omitempty"`
	NodeCount int    `json:"node_count"`
	FetchedAt string `json:"fetched_at"`
}

// ValidPublicKey 校验 XTLS Reality public_key 格式（32 字节 URL-safe base64）。
func ValidPublicKey(pbk string) bool {
	if pbk == "" {
		return false
	}
	b, err := base64.RawURLEncoding.DecodeString(pbk)
	return err == nil && len(b) == 32
}

// Fingerprint 依据实际连接信息生成节点指纹（任务书第十二章）。
// 不使用节点名称；协议+地址+端口+凭据+关键传输配置相同即同一节点。
func Fingerprint(protocol, server string, port int, keyParams map[string]string) string {
	h := sha256.New()
	w := func(s string) {
		h.Write([]byte(s))
		h.Write([]byte{0})
	}
	w(protocol)
	w(server)
	w(strconv.Itoa(port))
	// 参数按 key 排序后全量参与指纹（"uri" 除外，它带着来源自己的 #名称）。
	// 原先是硬编码参数名白名单：每加一种协议都得记得回来加 key，漏了就会撞指纹
	// —— 例如同 host:port:password 但 obfs 不同的两个 hysteria2 节点会被并成一个。
	keys := make([]string, 0, len(keyParams))
	for k := range keyParams {
		if k != "uri" {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	for _, k := range keys {
		w(k)
		w(keyParams[k])
	}
	return hex.EncodeToString(h.Sum(nil))[:16]
}
