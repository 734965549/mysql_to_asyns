package fullload

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestReadCoordinator_PrepareTableRetryClearsTableError(t *testing.T) {
	ctx := context.Background()
	q := newBatchQueue(defaultBufferBytes, &Stats{})
	opt := Options{GlobalReadBudget: 2, TableParallelReaders: 1, BatchRows: 10, BatchBytes: defaultBatchBytes}
	stats := &Stats{}
	coord := newReadCoordinator(ctx, nil, q, opt, stats, nil, nil, nil, nil, nil)

	coord.setTableErr("s", "t", 1, errors.New("boom"))
	coord.scheduler.addTable("s", "t", []*Chunk{{ID: "c1"}})
	coord.prepareTableRetry("s", "t")

	if err := coord.tableErr("s", "t"); err != nil {
		t.Fatalf("expected table error cleared after prepareTableRetry, got %v", err)
	}
	if coord.scheduler.tablePending(tableKey("s", "t")) != 0 {
		t.Fatalf("expected scheduler queue cleared for table")
	}
}

func TestReadCoordinator_TableErrorDoesNotAffectOtherTable(t *testing.T) {
	ctx := context.Background()
	q := newBatchQueue(defaultBufferBytes, &Stats{})
	opt := Options{GlobalReadBudget: 2, TableParallelReaders: 1}
	coord := newReadCoordinator(ctx, nil, q, opt, &Stats{}, nil, nil, nil, nil, nil)

	coord.setTableErr("s", "a", 1, errors.New("table a failed"))
	if err := coord.tableErr("s", "b"); err != nil {
		t.Fatalf("table b should not inherit table a error, got %v", err)
	}
}

func TestReadCoordinator_StaleAttemptDoesNotCancelCurrentAttempt(t *testing.T) {
	ctx := context.Background()
	q := newBatchQueue(defaultBufferBytes, &Stats{})
	opt := Options{GlobalReadBudget: 2, TableParallelReaders: 1}
	coord := newReadCoordinator(ctx, nil, q, opt, &Stats{}, nil, nil, nil, nil, nil)

	tableKey := tableKey("s", "t")
	attemptCtx, attemptCancel := context.WithCancel(ctx)
	coord.mu.Lock()
	coord.attemptIDs[tableKey] = 2
	coord.attemptCtxs[tableKey] = attemptCtx
	coord.attemptCancels[tableKey] = attemptCancel
	coord.mu.Unlock()

	coord.scheduler.addTable("s", "t", []*Chunk{{ID: "c1"}, {ID: "c2"}})

	accepted := coord.setTableErr("s", "t", 1, errors.New("stale"))
	if accepted {
		t.Fatal("expected stale attempt error to be rejected")
	}
	coord.cancelTableAttempt("s", "t", 1)
	coord.markTableFailed("s", "t", 1)

	if err := attemptCtx.Err(); err != nil {
		t.Fatalf("current attempt context should remain active, got %v", err)
	}
	if pending := coord.scheduler.tablePending(tableKey); pending != 2 {
		t.Fatalf("expected pending chunks untouched, got %d", pending)
	}

	if !coord.setTableErr("s", "t", 2, errors.New("current")) {
		t.Fatal("expected current attempt error to be accepted")
	}
	coord.cancelTableAttempt("s", "t", 2)
	coord.markTableFailed("s", "t", 2)

	if err := attemptCtx.Err(); err == nil {
		t.Fatal("expected current attempt to be cancelled after accepted failure")
	}
	if pending := coord.scheduler.tablePending(tableKey); pending != 0 {
		t.Fatalf("expected failed attempt queue drained, got %d pending", pending)
	}
}

func TestBatchQueue_BlockedPutCountsForSoftLimit(t *testing.T) {
	q := newBatchQueue(200, &Stats{})
	ctx := context.Background()
	_ = q.Put(ctx, &RowBatch{Schema: "s", Table: "a", ApproxBytes: 100})
	_ = q.Put(ctx, &RowBatch{Schema: "s", Table: "b", ApproxBytes: 100})

	putDone := make(chan error, 1)
	go func() {
		putDone <- q.Put(ctx, &RowBatch{Schema: "s", Table: "c", ApproxBytes: 30})
	}()

	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		q.mu.Lock()
		sq := q.tables[tableQueueKey("s", "c")]
		waiting := q.waitingTableCount()
		q.mu.Unlock()
		if sq != nil && sq.waitingPut > 0 && waiting >= 3 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("blocked producer on table c should count toward waiting tables")
}
