package main

import (
	"reflect"
	"testing"
)

// TestReorderArgs 保证 `novanode fetch -dir data` 这种"子命令在前"的写法
// 真的把 -dir 传进去（Go 的 flag 包默认遇到子命令就停止解析，早先静默用默认目录）。
func TestReorderArgs(t *testing.T) {
	cases := []struct {
		in, want []string
	}{
		{[]string{"fetch", "-dir", "data"}, []string{"-dir", "data", "fetch"}},
		{[]string{"check", "-dir=x", "-full"}, []string{"-dir=x", "-full", "check"}},
		{[]string{"-dir", "d", "fetch"}, []string{"-dir", "d", "fetch"}},
		{[]string{"status"}, []string{"status"}},
		{[]string{"filter", "-filter-timeout", "5", "-dir", "d"}, []string{"-filter-timeout", "5", "-dir", "d", "filter"}},
		// -full 是 bool，不能把后面的子命令吞掉
		{[]string{"check", "-full", "-dir", "d"}, []string{"-full", "-dir", "d", "check"}},
	}
	for _, c := range cases {
		if got := reorderArgs(c.in); !reflect.DeepEqual(got, c.want) {
			t.Errorf("reorderArgs(%v) = %v，期望 %v", c.in, got, c.want)
		}
	}
}
