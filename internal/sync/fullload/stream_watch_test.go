package fullload

import (
	"sync/atomic"
	"testing"
	"time"
)

func TestStreamWatch_IdleFiresWithoutProgress(t *testing.T) {
	var fired atomic.Bool
	w := newStreamWatch(func() { fired.Store(true) }, 30*time.Millisecond, 0)
	defer w.stop()
	w.armIdle()

	deadline := time.Now().Add(500 * time.Millisecond)
	for !fired.Load() && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if !fired.Load() || !w.wasFired() {
		t.Fatal("expected idle timeout to fire")
	}
}

func TestStreamWatch_ProgressResetsIdle(t *testing.T) {
	var fired atomic.Bool
	w := newStreamWatch(func() { fired.Store(true) }, 40*time.Millisecond, 0)
	defer w.stop()
	w.armIdle()

	for i := 0; i < 5; i++ {
		time.Sleep(20 * time.Millisecond)
		w.noteProgress()
	}
	if fired.Load() {
		t.Fatal("idle should not fire while progress keeps resetting")
	}
	w.stop()
}

func TestStreamWatch_PauseSkipsIdle(t *testing.T) {
	var fired atomic.Bool
	w := newStreamWatch(func() { fired.Store(true) }, 30*time.Millisecond, 0)
	defer w.stop()
	w.armIdle()
	w.pause()
	time.Sleep(80 * time.Millisecond)
	if fired.Load() {
		t.Fatal("idle should not fire while paused")
	}
	w.resume()
	deadline := time.Now().Add(500 * time.Millisecond)
	for !fired.Load() && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if !fired.Load() {
		t.Fatal("expected idle to fire after resume")
	}
}

func TestStreamWatch_MaxDurationFiresEvenWhenPaused(t *testing.T) {
	var fired atomic.Bool
	w := newStreamWatch(func() { fired.Store(true) }, time.Hour, 40*time.Millisecond)
	defer w.stop()
	w.armIdle()
	w.pause()
	deadline := time.Now().Add(500 * time.Millisecond)
	for !fired.Load() && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if !fired.Load() {
		t.Fatal("expected max duration to fire while idle is paused")
	}
}

func TestResolveOptions_StreamTimeoutDefaults(t *testing.T) {
	opt := ResolveOptions(RawOptions{})
	if opt.QueryTimeout != time.Duration(defaultQueryTimeoutSec)*time.Second {
		t.Errorf("QueryTimeout=%v want %ds", opt.QueryTimeout, defaultQueryTimeoutSec)
	}
	if opt.StreamIdleTimeout != time.Duration(defaultStreamIdleTimeoutSec)*time.Second {
		t.Errorf("StreamIdleTimeout=%v want %ds", opt.StreamIdleTimeout, defaultStreamIdleTimeoutSec)
	}
	if opt.StreamMaxDuration != 0 {
		t.Errorf("StreamMaxDuration=%v want 0 (unlimited)", opt.StreamMaxDuration)
	}
}

func TestResolveOptions_StreamTimeoutExplicit(t *testing.T) {
	opt := ResolveOptions(RawOptions{
		QueryTimeoutSec:      600,
		StreamIdleTimeoutSec: 120,
		StreamMaxDurationSec: 3600,
	})
	if opt.QueryTimeout != 600*time.Second {
		t.Errorf("QueryTimeout=%v want 600s", opt.QueryTimeout)
	}
	if opt.StreamIdleTimeout != 120*time.Second {
		t.Errorf("StreamIdleTimeout=%v want 120s", opt.StreamIdleTimeout)
	}
	if opt.StreamMaxDuration != 3600*time.Second {
		t.Errorf("StreamMaxDuration=%v want 3600s", opt.StreamMaxDuration)
	}
}

func TestResolveOptions_StreamMaxDurationClamped(t *testing.T) {
	opt := ResolveOptions(RawOptions{StreamMaxDurationSec: hardMaxStreamMaxDurationSec + 100})
	if opt.StreamMaxDuration != time.Duration(hardMaxStreamMaxDurationSec)*time.Second {
		t.Errorf("StreamMaxDuration=%v want hard max %ds", opt.StreamMaxDuration, hardMaxStreamMaxDurationSec)
	}
}
