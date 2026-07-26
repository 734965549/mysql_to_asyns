package fullload

import (
	"context"
	"testing"
	"time"
)

func TestBatchQueue_FIFOAndClose(t *testing.T) {
	q := newBatchQueue(1024, &Stats{})
	ctx := context.Background()
	for i := 0; i < 3; i++ {
		if err := q.Put(ctx, &RowBatch{ChunkID: string(rune('a' + i)), ApproxBytes: 1}); err != nil {
			t.Fatalf("put: %v", err)
		}
	}
	q.Close()
	var got []string
	for {
		b, ok := q.Get(ctx)
		if !ok {
			break
		}
		got = append(got, b.ChunkID)
	}
	if len(got) != 3 || got[0] != "a" || got[2] != "c" {
		t.Fatalf("FIFO order broken: %v", got)
	}
}

func TestBatchQueue_Backpressure(t *testing.T) {
	q := newBatchQueue(100, &Stats{})
	ctx := context.Background()

	// 第一个批次即使超限也允许放入（队列为空）。
	if err := q.Put(ctx, &RowBatch{ChunkID: "big", ApproxBytes: 80}); err != nil {
		t.Fatalf("put1: %v", err)
	}

	// 第二个批次会超过上限 → Put 阻塞，直到消费者取走第一个。
	putDone := make(chan struct{})
	go func() {
		_ = q.Put(ctx, &RowBatch{ChunkID: "second", ApproxBytes: 80})
		close(putDone)
	}()

	select {
	case <-putDone:
		t.Fatal("Put should block due to byte backpressure")
	case <-time.After(100 * time.Millisecond):
	}

	if _, ok := q.Get(ctx); !ok {
		t.Fatal("expected first batch")
	}

	select {
	case <-putDone:
	case <-time.After(time.Second):
		t.Fatal("Put should unblock after consuming")
	}
}

func TestBatchQueue_ContextCancel(t *testing.T) {
	q := newBatchQueue(10, &Stats{})
	ctx, cancel := context.WithCancel(context.Background())
	q.watchContext(ctx)

	// 填满队列。
	_ = q.Put(ctx, &RowBatch{ApproxBytes: 10})

	blocked := make(chan error, 1)
	go func() {
		blocked <- q.Put(ctx, &RowBatch{ApproxBytes: 10})
	}()
	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case err := <-blocked:
		if err == nil {
			t.Fatal("expected error after cancel")
		}
	case <-time.After(time.Second):
		t.Fatal("Put should return after context cancel")
	}
}
