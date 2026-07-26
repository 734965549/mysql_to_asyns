package fullload

import (
	"errors"
	"sync/atomic"
	"testing"
)

func TestTableCompletionTracker_FiresAfterReadAndCommit(t *testing.T) {
	var fired int32
	tracker := newTableCompletionTracker([]*TableSpec{
		{SourceSchema: "s", SourceTable: "t"},
	}, func(schema, table string) error {
		if schema != "s" || table != "t" {
			t.Fatalf("unexpected %s.%s", schema, table)
		}
		atomic.AddInt32(&fired, 1)
		return nil
	})

	tracker.onBatchEnqueued("s", "t")
	tracker.onBatchEnqueued("s", "t")
	if err := tracker.markReadDone("s", "t"); err != nil {
		t.Fatal(err)
	}
	if atomic.LoadInt32(&fired) != 0 {
		t.Fatal("should not fire while inflight > 0")
	}
	if err := tracker.onBatchesCommitted("s", "t", 1); err != nil {
		t.Fatal(err)
	}
	if atomic.LoadInt32(&fired) != 0 {
		t.Fatal("should not fire until all batches committed")
	}
	if err := tracker.onBatchesCommitted("s", "t", 1); err != nil {
		t.Fatal(err)
	}
	if atomic.LoadInt32(&fired) != 1 {
		t.Fatalf("fired=%d want 1", fired)
	}
	// 幂等：再次提交不重复触发。
	if err := tracker.onBatchesCommitted("s", "t", 1); err != nil {
		t.Fatal(err)
	}
	if atomic.LoadInt32(&fired) != 1 {
		t.Fatalf("fired=%d want still 1", fired)
	}
}

func TestTableCompletionTracker_EmptyTableFiresOnReadDone(t *testing.T) {
	var fired int32
	tracker := newTableCompletionTracker([]*TableSpec{
		{SourceSchema: "s", SourceTable: "empty"},
	}, func(schema, table string) error {
		atomic.AddInt32(&fired, 1)
		return nil
	})
	if err := tracker.markReadDone("s", "empty"); err != nil {
		t.Fatal(err)
	}
	if atomic.LoadInt32(&fired) != 1 {
		t.Fatalf("fired=%d want 1", fired)
	}
}

func TestTableCompletionTracker_CallbackError(t *testing.T) {
	tracker := newTableCompletionTracker([]*TableSpec{
		{SourceSchema: "s", SourceTable: "t"},
	}, func(schema, table string) error {
		return errors.New("restore failed")
	})
	err := tracker.markReadDone("s", "t")
	if err == nil {
		t.Fatal("expected callback error")
	}
}

func TestTableCompletionTracker_EnqueueBeforeVisiblePreventsLostInflight(t *testing.T) {
	// 模拟旧竞态的正确修复语义：先 +inflight，writer 再提交，最后 markReadDone 必须能触发。
	var fired int32
	tracker := newTableCompletionTracker([]*TableSpec{
		{SourceSchema: "s", SourceTable: "t"},
	}, func(schema, table string) error {
		atomic.AddInt32(&fired, 1)
		return nil
	})

	tracker.onBatchEnqueued("s", "t")
	if err := tracker.onBatchesCommitted("s", "t", 1); err != nil {
		t.Fatal(err)
	}
	if err := tracker.markReadDone("s", "t"); err != nil {
		t.Fatal(err)
	}
	if atomic.LoadInt32(&fired) != 1 {
		t.Fatalf("fired=%d want 1", fired)
	}
}

func TestTableCompletionTracker_AbortEnqueueRollsBackInflight(t *testing.T) {
	var fired int32
	tracker := newTableCompletionTracker([]*TableSpec{
		{SourceSchema: "s", SourceTable: "t"},
	}, func(schema, table string) error {
		atomic.AddInt32(&fired, 1)
		return nil
	})
	tracker.onBatchEnqueued("s", "t")
	if err := tracker.onBatchEnqueueAborted("s", "t"); err != nil {
		t.Fatal(err)
	}
	if err := tracker.markReadDone("s", "t"); err != nil {
		t.Fatal(err)
	}
	if atomic.LoadInt32(&fired) != 1 {
		t.Fatalf("fired=%d want 1 after aborted enqueue", fired)
	}
}
