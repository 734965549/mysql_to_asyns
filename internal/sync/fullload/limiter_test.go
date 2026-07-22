package fullload

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestSnapshotLimiter_AcquireRelease(t *testing.T) {
	lim := newSnapshotLimiter(2, 3)
	ctx := context.Background()

	g1, err := lim.acquireGroup(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := lim.acquireConns(ctx, 2); err != nil {
		t.Fatal(err)
	}
	groups, conns, _ := lim.snapshot()
	if groups != 1 || conns != 2 {
		t.Fatalf("groups=%d conns=%d", groups, conns)
	}

	lim.releaseConns(1)
	g1.release()
	groups, conns, age := lim.snapshot()
	if groups != 0 || conns != 1 || age != 0 {
		t.Fatalf("after release groups=%d conns=%d age=%v", groups, conns, age)
	}
	lim.releaseConns(1)
}

func TestSnapshotLimiter_BlocksWhenExhausted(t *testing.T) {
	lim := newSnapshotLimiter(1, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	g, err := lim.acquireGroup(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer g.release()

	_, err = lim.acquireGroup(ctx)
	if err == nil {
		t.Fatal("expected context timeout while groups exhausted")
	}
}

func TestSnapshotLimiter_ReserveCoordinatorAvoidsDeadlock(t *testing.T) {
	// 容量刚好能放下 readers+coordinator；若未预留协调连接会卡死。
	lim := newSnapshotLimiter(1, 3)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	g, err := lim.acquireGroup(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer g.release()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		// 模拟：先拿 2 个 reader + 1 coordinator
		if err := lim.acquireConns(ctx, 3); err != nil {
			t.Errorf("acquire 3: %v", err)
			return
		}
		// 解锁后归还 coordinator
		lim.releaseConns(1)
		lim.releaseConns(2)
	}()
	wg.Wait()
}

func TestSnapshotLimiter_WeightedAcquireAvoidsPartialHoldDeadlock(t *testing.T) {
	// 容量 5：两个请求各要 3。逐槽获取会互占一部分后永久互等；加权全有或全无应串行通过。
	lim := newSnapshotLimiter(2, 5)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	errCh := make(chan error, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := lim.acquireConns(ctx, 3); err != nil {
				errCh <- err
				return
			}
			time.Sleep(20 * time.Millisecond)
			lim.releaseConns(3)
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatalf("weighted acquire deadlocked or failed: %v", err)
	}
}

func TestSnapshotLimiter_RejectsOverCapacity(t *testing.T) {
	lim := newSnapshotLimiter(1, 2)
	err := lim.acquireConns(context.Background(), 3)
	if err == nil {
		t.Fatal("expected over-capacity error")
	}
}
