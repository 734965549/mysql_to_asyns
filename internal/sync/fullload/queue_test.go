package fullload

import (
	"context"
	"testing"
	"time"
)

func TestBatchQueue_FairRoundRobin(t *testing.T) {
	q := newBatchQueue(1024, &Stats{})
	ctx := context.Background()
	_ = q.Put(ctx, &RowBatch{Schema: "s", Table: "a", ChunkID: "a1", ApproxBytes: 1})
	_ = q.Put(ctx, &RowBatch{Schema: "s", Table: "b", ChunkID: "b1", ApproxBytes: 1})
	_ = q.Put(ctx, &RowBatch{Schema: "s", Table: "a", ChunkID: "a2", ApproxBytes: 1})

	var got []string
	for i := 0; i < 3; i++ {
		b, ok := q.Get(ctx)
		if !ok || b == nil {
			t.Fatalf("get %d failed", i)
		}
		got = append(got, b.ChunkID)
	}
	if got[0] != "a1" || got[1] != "b1" || got[2] != "a2" {
		t.Fatalf("fair order want a1,b1,a2 got %v", got)
	}
}

func TestBatchQueue_ReRegisterPollKeyAfterDrain(t *testing.T) {
	q := newBatchQueue(1024, &Stats{})
	ctx := context.Background()
	_ = q.Put(ctx, &RowBatch{Schema: "s", Table: "a", ChunkID: "a1", ApproxBytes: 1})
	if _, ok := q.Get(ctx); !ok {
		t.Fatal("first get failed")
	}
	_ = q.Put(ctx, &RowBatch{Schema: "s", Table: "a", ChunkID: "a2", ApproxBytes: 1})
	b, ok := q.Get(ctx)
	if !ok || b == nil || b.ChunkID != "a2" {
		t.Fatalf("expected second batch after subqueue drain, got %+v ok=%v", b, ok)
	}
}

func TestBatchQueue_GetUntilTableKey(t *testing.T) {
	q := newBatchQueue(1024, &Stats{})
	ctx := context.Background()
	_ = q.Put(ctx, &RowBatch{Schema: "s", Table: "a", ChunkID: "a1", ApproxBytes: 1})
	_ = q.Put(ctx, &RowBatch{Schema: "s", Table: "b", ChunkID: "b1", ApproxBytes: 1})

	b, ok, _ := q.GetUntil(ctx, time.Time{}, tableQueueKey("s", "b"))
	if !ok || b == nil || b.ChunkID != "b1" {
		t.Fatalf("table-scoped get want b1 got %+v ok=%v", b, ok)
	}
}

func TestBatchQueue_TableSoftLimit(t *testing.T) {
	q := newBatchQueue(200, &Stats{})
	ctx := context.Background()
	// 两表各放 80 字节，第三张表应受 soft limit（min(global/2,32MiB)=100）阻塞。
	_ = q.Put(ctx, &RowBatch{Schema: "s", Table: "a", ApproxBytes: 80})
	_ = q.Put(ctx, &RowBatch{Schema: "s", Table: "b", ApproxBytes: 80})

	putDone := make(chan error, 1)
	go func() {
		putDone <- q.Put(ctx, &RowBatch{Schema: "s", Table: "a", ApproxBytes: 30})
	}()

	select {
	case err := <-putDone:
		if err == nil {
			t.Fatal("expected table soft limit to block second batch on table a")
		}
	case <-time.After(100 * time.Millisecond):
	}

	if _, ok := q.Get(ctx); !ok {
		t.Fatal("expected dequeue to unblock put")
	}
	select {
	case err := <-putDone:
		if err != nil {
			t.Fatalf("put after consume: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("put should succeed after consume")
	}
}
