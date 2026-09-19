package checker

import "testing"

// TestPercentileTailNotMean 检验筛选方案里最关键的一条：
// 均值低不代表稳 —— 排序必须看尾部。
// 样本 A：稳定 550~700ms；样本 B：多数 450ms 但有一次 6s 尖峰。
// B 必须因为尾部尖峰被判为更不稳，尽管它多数样本更快。
func TestPercentileTailNotMean(t *testing.T) {
	a := []int{550, 600, 620, 650, 700}
	b := []int{450, 460, 455, 470, 6000}
	sa := Summarize(a, 5, 5)
	sb := Summarize(b, 5, 5)
	if sb.P50 >= sa.P50 {
		t.Fatalf("样本设置无效：B 的中位数本应更低（A %+v / B %+v）", sa, sb)
	}
	if sb.P95 < 6000 {
		t.Errorf("B 的 P95 = %d，应抓到 6000 的尖峰", sb.P95)
	}
	if sa.Jitter >= sb.Jitter {
		t.Errorf("抖动应区分稳/抖：A.Jitter=%d B.Jitter=%d", sa.Jitter, sb.Jitter)
	}
}

// TestSummarizeLossAndEmpty 丢包率与空样本的行为。
func TestSummarizeLossAndEmpty(t *testing.T) {
	s := Summarize([]int{500, 500}, 4, 2) // 4 轮里 2 轮成功
	if s.Loss < 0.49 || s.Loss > 0.51 {
		t.Errorf("Loss = %v，期望 0.5", s.Loss)
	}
	if s.Samples != 4 || s.OKs != 2 {
		t.Errorf("Samples/OKs = %d/%d，期望 4/2", s.Samples, s.OKs)
	}
	e := Summarize(nil, 3, 0) // 一轮都没通
	if e.P50 != 0 || e.P95 != 0 || e.Loss != 1 {
		t.Errorf("全失败画像异常: %+v", e)
	}
	if z := Summarize(nil, 0, 0); z.Samples != 0 || z.Loss != 0 {
		t.Errorf("rounds=0 应为零值画像: %+v", z)
	}
}

// TestPercentileBounds 分位数下标不得越界（单样本与 100 分位是常见踩点）。
func TestPercentileBounds(t *testing.T) {
	if got := percentile([]int{42}, 95); got != 42 {
		t.Errorf("单样本 P95 = %d，期望 42", got)
	}
	if got := percentile([]int{1, 2, 3}, 0); got != 1 {
		t.Errorf("p=0 应夹到最小，实得 %d", got)
	}
	if got := percentile([]int{1, 2, 3}, 100); got != 3 {
		t.Errorf("P100 = %d，期望 3", got)
	}
	if got := percentile(nil, 50); got != 0 {
		t.Errorf("空样本应得 0，实得 %d", got)
	}
}
