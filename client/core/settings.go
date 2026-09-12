// Package core 封装 NovaLink 客户端设置与 VPN 核心生命周期管理
// （任务书第十七章：客户端负责选择节点、生成配置、启停核心、显示结果）。
package core

import (
	"encoding/json"
	"os"
	"path/filepath"
)

// Settings 是客户端设置（任务书第二十章：第一版本不堆大量参数）。
type Settings struct {
	SingBoxPath  string `json:"singbox_path"`  // 核心可执行文件路径
	PoolPath     string `json:"pool_path"`     // 节点池文件（Node Pipeline 产出）
	Listen       string `json:"listen"`        // 本客户端 Web 界面监听地址
	ProxyPort    int    `json:"proxy_port"`    // 核心 mixed 入站端口
	AutoSysProxy bool   `json:"auto_sysproxy"` // 连接成功后自动接管系统代理
}

// LoadSettings 读取设置；不存在时写入默认值。
func LoadSettings(path string) (*Settings, error) {
	b, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		s := defaultSettings()
		if err := SaveSettings(path, s); err != nil {
			return nil, err
		}
		return s, nil
	}
	if err != nil {
		return nil, err
	}
	s := defaultSettings()
	if err := json.Unmarshal(b, s); err != nil {
		return nil, err
	}
	return s, nil
}

// SaveSettings 保存设置。
func SaveSettings(path string, s *Settings) error {
	b, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	return os.WriteFile(path, b, 0o644)
}

func defaultSettings() *Settings {
	return &Settings{
		SingBoxPath:  "E:/NovaLink/core/vpn-core/sing-box-1.14.0-windows-amd64/sing-box.exe",
		PoolPath:     "E:/NovaLink/node/data/pool.json",
		Listen:       "127.0.0.1:7892",
		ProxyPort:    7890,
		AutoSysProxy: true,
	}
}
