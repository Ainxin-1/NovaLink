// Windows 系统代理接管（任务书第十八章：像普通桌面软件一样使用）。
// 连接成功后写入系统代理，断开后恢复用户原设置（先备份再修改）。
package core

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"syscall"
)

const internetSettingsKey = `HKCU\Software\Microsoft\Windows\CurrentVersion\Internet Settings`

type proxyBackup struct {
	ProxyEnable string `json:"proxy_enable"`
	ProxyServer string `json:"proxy_server,omitempty"`
}

var wininet = syscall.NewLazyDLL("wininet.dll")

// refreshNotify 通知系统代理设置已变更（InternetSetOption）。
func refreshNotify() {
	const (
		internetOptionSettingsChanged = 39
		internetOptionRefresh         = 37
	)
	p := wininet.NewProc("InternetSetOptionW")
	_, _, _ = p.Call(0, internetOptionSettingsChanged, 0, 0)
	_, _, _ = p.Call(0, internetOptionRefresh, 0, 0)
}

func regQueryValue(name string) (string, bool) {
	out, err := exec.Command("reg", "query", internetSettingsKey, "/v", name).Output()
	if err != nil {
		return "", false
	}
	for _, line := range strings.Split(string(out), "\n") {
		f := strings.Fields(strings.TrimSpace(line))
		if len(f) >= 3 && f[0] == name {
			return f[len(f)-1], true
		}
	}
	return "", false
}

func regSet(name, typ, value string) error {
	cmd := exec.Command("reg", "add", internetSettingsKey, "/v", name, "/t", typ, "/d", value, "/f")
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("reg add %s: %v: %s", name, err, strings.TrimSpace(string(out)))
	}
	return nil
}

func regDelete(name string) {
	_ = exec.Command("reg", "delete", internetSettingsKey, "/v", name, "/f").Run()
}

// SetSystemProxy 将系统代理指向 127.0.0.1:port，并把用户原设置备份到 backupPath。
func SetSystemProxy(port int, backupPath string) error {
	b := proxyBackup{ProxyEnable: "0x0"}
	if v, ok := regQueryValue("ProxyEnable"); ok {
		b.ProxyEnable = v
	}
	if v, ok := regQueryValue("ProxyServer"); ok {
		b.ProxyServer = v
	}
	if jb, err := json.MarshalIndent(b, "", "  "); err == nil {
		_ = os.WriteFile(backupPath, jb, 0o644)
	}
	if err := regSet("ProxyServer", "REG_SZ", fmt.Sprintf("127.0.0.1:%d", port)); err != nil {
		return err
	}
	if err := regSet("ProxyEnable", "REG_DWORD", "0x1"); err != nil {
		return err
	}
	if err := regSet("ProxyOverride", "REG_SZ",
		"localhost;127.*;10.*;172.16.*;192.168.*;<local>"); err != nil {
		return err
	}
	refreshNotify()
	return nil
}

// RestoreSystemProxy 恢复备份的系统代理设置；无备份时仅关闭代理开关。
func RestoreSystemProxy(backupPath string) {
	b := proxyBackup{ProxyEnable: "0x0"}
	if jb, err := os.ReadFile(backupPath); err == nil {
		_ = json.Unmarshal(jb, &b)
	}
	if b.ProxyServer != "" {
		_ = regSet("ProxyServer", "REG_SZ", b.ProxyServer)
	} else {
		regDelete("ProxyServer")
	}
	_ = regSet("ProxyEnable", "REG_DWORD", b.ProxyEnable)
	refreshNotify()
}
