// 残留核心清理：客户端被强杀时其 sing-box 子进程会变成孤儿并继续
// 占用代理端口，导致下次连接"端口被占用"。启动时按端口找到占用者，
// 仅当确认是自家核心路径时才清理（绝不误杀 v2rayN 等同名核心）。
package core

import (
	"fmt"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
)

func CleanupOrphan(port int, singboxPath string, logf func(string, ...any)) {
	out, err := exec.Command("netstat", "-ano").Output()
	if err != nil {
		return
	}
	suffix := ":" + strconv.Itoa(port)
	pids := map[string]bool{}
	for _, line := range strings.Split(string(out), "\n") {
		f := strings.Fields(line)
		if len(f) >= 5 && f[3] == "LISTENING" && strings.HasSuffix(f[1], suffix) {
			pids[f[4]] = true
		}
	}
	self := strings.ToLower(filepath.Dir(singboxPath))
	for pid := range pids {
		b, err := exec.Command("powershell", "-NoProfile", "-Command",
			fmt.Sprintf("(Get-CimInstance Win32_Process -Filter 'ProcessId=%s').ExecutablePath", pid)).Output()
		if err != nil {
			continue
		}
		path := strings.Map(func(r rune) rune {
			if r < 32 {
				return -1
			}
			return r
		}, strings.TrimSpace(string(b)))
		if path == "" {
			continue
		}
		if strings.Contains(strings.ToLower(path), "novalink") ||
			strings.EqualFold(strings.ToLower(filepath.Dir(path)), self) {
			if err := exec.Command("taskkill", "/F", "/T", "/PID", pid).Run(); err == nil {
				logf("已清理残留核心进程 PID %s", pid)
			}
		} else {
			logf("端口 %d 被其他程序占用（%s），不做清理", port, path)
		}
	}
}
