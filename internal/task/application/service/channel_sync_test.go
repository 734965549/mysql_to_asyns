package service

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestEffectiveChannelBufferBatches(t *testing.T) {
	tests := []struct {
		workers    int
		configured int
		want       int
	}{
		{4, 0, 16},
		{4, 8, 8},
		{4, 1000, 256},
		{0, 0, 4},
	}
	for _, tc := range tests {
		got := EffectiveChannelBufferBatches(tc.workers, tc.configured)
		if got != tc.want {
			t.Errorf("workers=%d configured=%d: got %d want %d", tc.workers, tc.configured, got, tc.want)
		}
	}
}

func TestChannelSync_WaitForCompletionWaitsAllBatches(t *testing.T) {
	const workers = 2
	const batches = 5

	cs := NewChannelSync(workers, 10, 0)
	var handled int32

	cs.StartWorkers(context.Background(), func(task *BatchTask) error {
		atomic.AddInt32(&handled, 1)
		time.Sleep(20 * time.Millisecond)
		return nil
	})

	ctx := context.Background()
	for i := 0; i < batches; i++ {
		row := map[string]interface{}{"id": i}
		if err := cs.AddBatch(ctx, i, []map[string]interface{}{row}, i, i, fmt.Sprintf("b%d", i)); err != nil {
			t.Fatalf("AddBatch %d: %v", i, err)
		}
	}

	if err := cs.WaitForCompletion(ctx); err != nil {
		t.Fatalf("WaitForCompletion: %v", err)
	}

	if got := atomic.LoadInt32(&handled); got != int32(batches) {
		t.Fatalf("handled batches: got %d want %d", got, batches)
	}
	processed, _ := cs.GetProgress()
	if processed != int64(batches) {
		t.Fatalf("processed rows: got %d want %d", processed, batches)
	}
}

func TestChannelSync_WaitForCompletionReturnsFirstWorkerError(t *testing.T) {
	cs := NewChannelSync(2, 10, 0)
	cs.StartWorkers(context.Background(), func(task *BatchTask) error {
		if task.BatchID == 1 {
			return fmt.Errorf("boom")
		}
		return nil
	})

	ctx := context.Background()
	for i := 0; i < 3; i++ {
		row := map[string]interface{}{"id": i}
		if err := cs.AddBatch(ctx, i, []map[string]interface{}{row}, i, i, ""); err != nil {
			t.Fatalf("AddBatch: %v", err)
		}
	}

	err := cs.WaitForCompletion(ctx)
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestChannelSync_AddBatchBlocksUntilSpace(t *testing.T) {
	const workers = 1
	cs := NewChannelSync(workers, 10, workers) // buffer = 1*4 = 4

	var workerReady sync.WaitGroup
	workerReady.Add(1)
	var workerStarted int32
	releaseWorker := make(chan struct{})

	cs.StartWorkers(context.Background(), func(task *BatchTask) error {
		if task.BatchID == 0 {
			if atomic.CompareAndSwapInt32(&workerStarted, 0, 1) {
				workerReady.Done()
			}
			<-releaseWorker
		}
		return nil
	})

	ctx := context.Background()
	if err := cs.AddBatch(ctx, 0, []map[string]interface{}{{"id": 0}}, 0, 0, ""); err != nil {
		t.Fatalf("first AddBatch: %v", err)
	}

	workerReady.Wait()

	done := make(chan struct{})
	go func() {
		defer close(done)
		// buffer=4：worker 占住首批后 channel 可再放 4 个，第 5 个应阻塞。
		for i := 1; i <= 5; i++ {
			if err := cs.AddBatch(ctx, i, []map[string]interface{}{{"id": i}}, i, i, ""); err != nil {
				t.Errorf("AddBatch %d: %v", i, err)
				return
			}
		}
	}()

	select {
	case <-done:
		t.Fatal("AddBatch should block while worker is stalled and buffer is full")
	case <-time.After(200 * time.Millisecond):
	}

	close(releaseWorker)

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("AddBatch did not unblock after worker consumed batch")
	}

	if err := cs.WaitForCompletion(ctx); err != nil {
		t.Fatalf("WaitForCompletion: %v", err)
	}
}

func TestChannelSync_AddBatchRespectsContextCancel(t *testing.T) {
	cs := NewChannelSync(1, 10, 0) // buffer = workerCount*4

	var workerReady sync.WaitGroup
	workerReady.Add(1)
	releaseWorker := make(chan struct{})
	var workerStarted int32

	cs.StartWorkers(context.Background(), func(task *BatchTask) error {
		if task.BatchID == 0 {
			if atomic.CompareAndSwapInt32(&workerStarted, 0, 1) {
				workerReady.Done()
			}
			<-releaseWorker
		}
		return nil
	})

	ctx := context.Background()
	if err := cs.AddBatch(ctx, 0, []map[string]interface{}{{"id": 0}}, 0, 0, ""); err != nil {
		t.Fatalf("first AddBatch: %v", err)
	}
	workerReady.Wait()

	for i := 1; i <= 4; i++ {
		if err := cs.AddBatch(ctx, i, []map[string]interface{}{{"id": i}}, i, i, ""); err != nil {
			t.Fatalf("prefill AddBatch %d: %v", i, err)
		}
	}

	cancelCtx, cancel := context.WithCancel(context.Background())

	done := make(chan error, 1)
	go func() {
		done <- cs.AddBatch(cancelCtx, 5, []map[string]interface{}{{"id": 5}}, 5, 5, "")
	}()

	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if err != context.Canceled {
			t.Fatalf("expected context.Canceled, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("AddBatch did not return after context cancel")
	}

	close(releaseWorker)
	if err := cs.WaitForCompletion(ctx); err != nil {
		t.Fatalf("WaitForCompletion: %v", err)
	}
}
