package fullload

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestStressFairness_ChunkSchedulerSmallTablesNotStarved(t *testing.T) {
	s := newChunkScheduler()
	mk := func(schema, table string, n int) []*Chunk {
		out := make([]*Chunk, n)
		for i := 0; i < n; i++ {
			out[i] = &Chunk{
				ID:   chunkID(&TableSpec{SourceSchema: schema, SourceTable: table}, i),
				Spec: &TableSpec{SourceSchema: schema, SourceTable: table},
			}
		}
		return out
	}
	s.addTable("db", "big", mk("db", "big", 50))
	for i := 0; i < 5; i++ {
		name := fmt.Sprintf("small%d", i)
		s.addTable("db", name, mk("db", name, 2))
	}

	ctx := context.Background()
	firstSmall := -1
	for round := 0; round < 6; round++ {
		chunk, ok := s.next(ctx, 1)
		if !ok || chunk == nil {
			t.Fatalf("round %d: scheduler returned nothing", round)
		}
		tbl := chunk.Spec.SourceTable
		if tbl != "big" && firstSmall < 0 {
			firstSmall = round
		}
		s.markDone(chunk.Spec.SourceSchema, chunk.Spec.SourceTable)
	}
	if firstSmall < 0 || firstSmall > 5 {
		t.Fatalf("small table should start within first 6 dispatches, firstSmall=%d", firstSmall)
	}
}

func TestStressFairness_WriteQueueSmallTableDequeuesUnderPressure(t *testing.T) {
	q := newBatchQueue(256, &Stats{})
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		if err := q.Put(ctx, &RowBatch{
			Schema: "s", Table: "big", ChunkID: fmt.Sprintf("b%d", i), ApproxBytes: 80,
		}); err != nil {
			t.Fatalf("put big batch: %v", err)
		}
	}
	if err := q.Put(ctx, &RowBatch{Schema: "s", Table: "small", ChunkID: "s1", ApproxBytes: 1}); err != nil {
		t.Fatalf("put small batch: %v", err)
	}

	var order []string
	for i := 0; i < 4; i++ {
		b, ok := q.Get(ctx)
		if !ok || b == nil {
			t.Fatalf("get %d failed", i)
		}
		order = append(order, b.Table)
	}
	if order[0] != "big" || order[1] != "small" {
		t.Fatalf("expected small table interleaved early, got %v", order)
	}
}

func TestStressFairness_ReadBudgetNeverExceedsCap(t *testing.T) {
	const cap = 4
	b := NewReadBudget(cap)
	ctx := context.Background()

	var peak int64
	var wg sync.WaitGroup
	stop := make(chan struct{})
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			key := fmt.Sprintf("db.t%d", idx%4)
			for {
				select {
				case <-stop:
					return
				default:
				}
				if err := b.Acquire(ctx, key, 2); err != nil {
					return
				}
				for {
					cur := int64(b.InUse())
					old := atomic.LoadInt64(&peak)
					if cur > old && !atomic.CompareAndSwapInt64(&peak, old, cur) {
						continue
					}
					break
				}
				time.Sleep(time.Millisecond)
				b.Release(key)
			}
		}(i)
	}
	time.Sleep(200 * time.Millisecond)
	close(stop)
	wg.Wait()
	if peak > cap {
		t.Fatalf("read budget peak in_use=%d exceeds cap=%d", peak, cap)
	}
}

func TestStressFairness_BackpressureEventsUnderSlowConsumer(t *testing.T) {
	stats := &Stats{}
	q := newBatchQueue(120, stats)
	ctx := context.Background()
	sink := &recordingSink{}
	var st queueBackpressureState

	for i := 0; i < 2; i++ {
		_ = q.Put(ctx, &RowBatch{Schema: "s", Table: "a", ApproxBytes: 50})
	}
	snap := stats.Snapshot()
	st.observe(sink, snap.QueueBytes, snap.QueueCap)
	if codes := sink.codes(); len(codes) != 1 || codes[0] != EventCodeQueueBackpressureHigh {
		t.Fatalf("expected backpressure high, got %v", codes)
	}

	for i := 0; i < 2; i++ {
		if _, ok := q.Get(ctx); !ok {
			t.Fatal("dequeue failed")
		}
	}
	snap = stats.Snapshot()
	st.observe(sink, snap.QueueBytes, snap.QueueCap)
	codes := sink.codes()
	if len(codes) != 2 || codes[1] != EventCodeQueueBackpressureRecovered {
		t.Fatalf("expected backpressure recovered, got %v", codes)
	}
}
