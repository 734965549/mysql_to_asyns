package fullload

import (
	"sync"
	"time"
)

// streamWatch 为无主键流式查询提供可重置、可暂停的无进展超时，
// 以及可选的绝对最长时长。与 keyset 的 context.WithTimeout 绝对超时不同：
// 等待写队列时暂停计时，Rows.Next 成功后重置计时。
type streamWatch struct {
	mu          sync.Mutex
	cancel      func()
	idleTimeout time.Duration
	maxDuration time.Duration
	timer       *time.Timer
	maxTimer    *time.Timer
	paused      bool
	stopped     bool
	fired      bool
	fireLimit time.Duration // 触发取消时对应的限额，供错误上报
}

func newStreamWatch(cancel func(), idleTimeout, maxDuration time.Duration) *streamWatch {
	w := &streamWatch{
		cancel:       cancel,
		idleTimeout:  idleTimeout,
		maxDuration:  maxDuration,
	}
	if maxDuration > 0 {
		w.maxTimer = time.AfterFunc(maxDuration, func() { w.fireWith(maxDuration) })
	}
	return w
}

// armIdle 启动或重置无进展超时。paused/stopped 时不启动计时器。
func (w *streamWatch) armIdle() {
	if w == nil {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.stopped || w.fired || w.paused || w.idleTimeout <= 0 {
		return
	}
	if w.timer == nil {
		idle := w.idleTimeout
		w.timer = time.AfterFunc(idle, func() { w.fireWith(idle) })
		return
	}
	w.timer.Reset(w.idleTimeout)
}

// noteProgress 在 Rows.Next 成功后调用，重置无进展计时。
func (w *streamWatch) noteProgress() {
	w.armIdle()
}

// pause 暂停无进展计时（例如等待写队列）。绝对 maxDuration 不受影响。
func (w *streamWatch) pause() {
	if w == nil {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	w.paused = true
	if w.timer != nil {
		w.timer.Stop()
	}
}

// resume 恢复无进展计时，从完整 idleTimeout 重新计时。
func (w *streamWatch) resume() {
	if w == nil {
		return
	}
	w.mu.Lock()
	w.paused = false
	stopped := w.stopped || w.fired
	w.mu.Unlock()
	if stopped {
		return
	}
	w.armIdle()
}

// stop 停止所有计时器，不再触发 cancel。
func (w *streamWatch) stop() {
	if w == nil {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	w.stopped = true
	if w.timer != nil {
		w.timer.Stop()
		w.timer = nil
	}
	if w.maxTimer != nil {
		w.maxTimer.Stop()
		w.maxTimer = nil
	}
}

func (w *streamWatch) fire() {
	limit := time.Duration(0)
	if w != nil {
		limit = w.idleTimeout
	}
	w.fireWith(limit)
}

func (w *streamWatch) fireWith(limit time.Duration) {
	if w == nil {
		return
	}
	w.mu.Lock()
	if w.stopped || w.fired {
		w.mu.Unlock()
		return
	}
	w.fired = true
	w.fireLimit = limit
	cancel := w.cancel
	w.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

func (w *streamWatch) wasFired() bool {
	if w == nil {
		return false
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.fired
}

func (w *streamWatch) limitOnFire() time.Duration {
	if w == nil {
		return 0
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.fireLimit
}
