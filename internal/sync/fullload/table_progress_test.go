package fullload

import (
	"testing"
	"time"
)

func TestTableProgressWatch_NoProgressAndRecovery(t *testing.T) {
	sink := &recordingSink{}
	w := newTableProgressWatch(1, sink)
	w.seed([]*TableSpec{
		{SourceSchema: "s", SourceTable: "big"},
		{SourceSchema: "s", SourceTable: "small"},
	})

	key := tableKey("s", "big")
	w.tick(map[string]int64{key: 100})
	time.Sleep(1100 * time.Millisecond)
	w.tick(map[string]int64{key: 100})

	codes := sink.codes()
	if len(codes) != 1 || codes[0] != EventCodeTableNoProgress {
		t.Fatalf("expected TABLE_NO_PROGRESS, got %v", codes)
	}

	w.tick(map[string]int64{key: 200})
	codes = sink.codes()
	if len(codes) != 2 || codes[1] != EventCodeTableProgressRecovered {
		t.Fatalf("expected TABLE_PROGRESS_RECOVERED, got %v", codes)
	}
}

func TestTableProgressWatch_DisabledWhenZeroThreshold(t *testing.T) {
	if w := newTableProgressWatch(0, &recordingSink{}); w != nil {
		t.Fatal("expected nil watch when threshold is 0")
	}
}

func TestStatsTableReadSnapshot(t *testing.T) {
	stats := &Stats{}
	stats.addReadBatchForTable("db", "t1", 10, 100, time.Millisecond)
	stats.addReadBatchForTable("db", "t2", 5, 50, time.Millisecond)
	stats.addReadBatchForTable("db", "t1", 3, 30, time.Millisecond)

	snap := stats.tableReadSnapshot()
	if snap[tableKey("db", "t1")] != 13 || snap[tableKey("db", "t2")] != 5 {
		t.Fatalf("unexpected snapshot: %v", snap)
	}
}
