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
	PoolURL      string `json:"pool_url"`      // 云端节点池订阅地址（jsDelivr，留空用默认）
	Listen       string `json:"listen"`        // 本客户端 Web 界面监听地址
	ProxyPort    int    `json:"proxy_port"`    // 核心 mixed 入站端口
	AutoSysProxy bool   `json:"auto_sysproxy"` // 连接成功后自动接管系统代理
}

// LoadSettings 读取设置；不存在时按所在目录生成默认值。
func LoadSettings(path string) (*Settings, error) {
	dir := filepath.Dir(path)
	b, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		s := defaultSettings(dir)
		if err := SaveSettings(path, s); err != nil {
			return nil, err
		}
		return s, nil
	}
	if err != nil {
		return nil, err
	}
	s := defaultSettings(dir)
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

func defaultSettings(dataDir string) *Settings {
	return &Settings{
		SingBoxPath:  resolveCore(dataDir),
		PoolPath:     filepath.Join(dataDir, "pool.json"),
		PoolURL:      DefaultPoolURL,
		Listen:       "127.0.0.1:7892",
		ProxyPort:    7890,
		AutoSysProxy: true,
	}
}

// resolveCore 定位核心可执行文件。
//
// 原先默认值是一个开发机的绝对路径（E:/NovaLink/core/...），换台机器或换
// 目录布局就必然报"核心程序不存在"。现在按显式环境变量 → 随程序分发 →
// 数据目录 → 仓库开发树（取版本目录名字典序最后一个 = 较新版本）依次找。
func resolveCore(dataDir string) string {
	if v := os.Getenv("NOVALINK_SINGBOX"); v != "" {
		return v
	}
	exeDir := ""
	if p, err := os.Executable(); err == nil {
		exeDir = filepath.Dir(p)
	}
	candidates := []string{}
	for _, dir := range []string{exeDir, dataDir} {
		if dir == "" {
			continue
		}
		candidates = append(candidates,
			filepath.Join(dir, "sing-box.exe"),
			filepath.Join(dir, "core", "sing-box.exe"),
		)
	}
	for _, dir := range []string{exeDir, dataDir} {
		if dir == "" {
			continue
		}
		// 仓库开发树：../core/vpn-core/sing-box-<ver>-windows-amd64/sing-box.exe
		if ms, err := filepath.Glob(filepath.Join(dir, "..", "core", "vpn-core",
			"sing-box-*-windows-amd64", "sing-box.exe")); err == nil && len(ms) > 0 {
			candidates = append(candidates, ms[len(ms)-1])
		}
	}
	for _, c := range candidates {
		if st, err := os.Stat(c); err == nil && !st.IsDir() {
			return c
		}
	}
	if len(candidates) > 0 {
		return candidates[0]
	}
	return "sing-box.exe"
}
