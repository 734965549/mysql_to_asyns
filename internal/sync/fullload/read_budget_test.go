package fullload

import (
	"context"
	"testing"
	"time"
)

func TestComputeGlobalReadBudget(t *testing.T) {
	if got := ComputeGlobalReadBudget(4, 32); got != 4 {
		t.Fatalf("budget=%d want 4", got)
	}
	// pool 4, reserved max(2, ceil(0.4))=2 -> avail 2
	if got := ComputeGlobalReadBudget(8, 4); got != 2 {
		t.Fatalf("budget=%d want 2", got)
	}
	if got := ComputeGlobalReadBudget(0, 0); got != defaultReadWorkers {
		t.Fatalf("budget=%d want default %d", got, defaultReadWorkers)
	}
}

func TestPerTableEffectiveLimit(t *testing.T) {
	if got := PerTableEffectiveLimit(4, 4, 1); got != 4 {
		t.Fatalf("single table limit=%d want 4", got)
	}
	if got := PerTableEffectiveLimit(4, 4, 3); got != 2 {
		t.Fatalf("multi table limit=%d want 2", got)
	}
}

func TestReadBudgetAcquireRelease(t *testing.T) {
	b := NewReadBudget(2)
	ctx := context.Background()
	if err := b.Acquire(ctx, "a.b", 1); err != nil {
		t.Fatal(err)
	}
	if err := b.Acquire(ctx, "c.d", 1); err != nil {
		t.Fatal(err)
	}
	if b.InUse() != 2 {
		t.Fatalf("in_use=%d want 2", b.InUse())
	}

	done := make(chan error, 1)
	go func() {
		cctx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
		defer cancel()
		done <- b.Acquire(cctx, "e.f", 1)
	}()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected timeout on saturated budget")
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("acquire did not unblock")
	}

	b.Release("a.b")
	done2 := make(chan error, 1)
	go func() {
		done2 <- b.Acquire(context.Background(), "e.f", 1)
	}()
	select {
	case err := <-done2:
		if err != nil {
			t.Fatalf("expected acquire after release: %v", err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("waiting acquire did not succeed")
	}
	b.Release("c.d")
	b.Release("e.f")
	if b.InUse() != 0 {
		t.Fatalf("in_use=%d want 0", b.InUse())
	}
}

func TestReadBudgetPerTableLimit(t *testing.T) {
	b := NewReadBudget(4)
	ctx := context.Background()
	if err := b.Acquire(ctx, "db.t", 1); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		cctx, cancel := context.WithTimeout(ctx, 150*time.Millisecond)
		defer cancel()
		done <- b.Acquire(cctx, "db.t", 1)
	}()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected per-table limit block")
		}
	case <-time.After(400 * time.Millisecond):
		t.Fatal("timeout")
	}
	b.Release("db.t")
	done2 := make(chan error, 1)
	go func() {
		done2 <- b.Acquire(context.Background(), "db.t", 1)
	}()
	select {
	case err := <-done2:
		if err != nil {
			t.Fatalf("second acquire: %v", err)
		}
	case <-time.After(400 * time.Millisecond):
		t.Fatal("second acquire timeout")
	}
	b.Release("db.t")
}
