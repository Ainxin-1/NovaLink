package core

import (
	"os"
	"path/filepath"
	"testing"
)

// TestDefaultSettingsArePortable 默认值不得含开发机绝对路径。
// 原先 PoolPath 固定指向 E:/NovaLink/node/data/pool.json（另一套管线的本地池，
// 实测是 6 天前的陈旧数据），换台机器则直接"核心程序不存在"。
func TestDefaultSettingsArePortable(t *testing.T) {
	dir := t.TempDir()
	s := defaultSettings(dir)
	if s.PoolPath != filepath.Join(dir, "pool.json") {
		t.Errorf("PoolPath = %q，应在给定数据目录内", s.PoolPath)
	}
	if s.Listen == "" || s.ProxyPort == 0 {
		t.Errorf("监听/端口未给默认值: %+v", s)
	}
	if !s.AutoSysProxy {
		t.Errorf("默认应自动接管系统代理")
	}
}

// TestLoadSettingsWritesDefaultsAtDir 首次运行应在数据目录里生成设置并复用。
func TestLoadSettingsWritesDefaultsAtDir(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "settings.json")
	s1, err := LoadSettings(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("应已生成设置文件: %v", err)
	}
	if filepath.Dir(s1.PoolPath) != dir {
		t.Errorf("默认池路径 %q 不在 %q 下", s1.PoolPath, dir)
	}
	s1.ProxyPort = 7899
	if err := SaveSettings(path, s1); err != nil {
		t.Fatal(err)
	}
	s2, err := LoadSettings(path)
	if err != nil {
		t.Fatal(err)
	}
	if s2.ProxyPort != 7899 {
		t.Errorf("已存设置未复用，ProxyPort = %d", s2.ProxyPort)
	}
}

// TestResolveCoreHonoursEnv 环境变量必须最高优先（CI 与非常规安装位置要用）。
func TestResolveCoreHonoursEnv(t *testing.T) {
	p := filepath.Join(t.TempDir(), "my-core.exe")
	if err := os.WriteFile(p, []byte("x"), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("NOVALINK_SINGBOX", p)
	if got := resolveCore(t.TempDir()); got != p {
		t.Errorf("resolveCore = %q，期望环境变量 %q", got, p)
	}
}
