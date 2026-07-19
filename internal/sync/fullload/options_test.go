package fullload

import (
	"testing"
	"time"
)

func TestResolveOptions_Defaults(t *testing.T) {
	opt := ResolveOptions(RawOptions{})
	if opt.ReadWorkers != defaultReadWorkers {
		t.Errorf("ReadWorkers=%d want %d", opt.ReadWorkers, defaultReadWorkers)
	}
	if opt.WriteWorkers != defaultWriteWorkers {
		t.Errorf("WriteWorkers=%d want %d", opt.WriteWorkers, defaultWriteWorkers)
	}
	if opt.BufferBytes != defaultBufferBytes {
		t.Errorf("BufferBytes=%d want %d", opt.BufferBytes, defaultBufferBytes)
	}
	if opt.BatchBytes != defaultBatchBytes {
		t.Errorf("BatchBytes=%d want %d", opt.BatchBytes, defaultBatchBytes)
	}
	if opt.BatchRows != defaultBatchRows {
		t.Errorf("BatchRows=%d want %d", opt.BatchRows, defaultBatchRows)
	}
	if opt.CommitRows != defaultCommitRows {
		t.Errorf("CommitRows=%d want %d", opt.CommitRows, defaultCommitRows)
	}
	if opt.CommitBytes != defaultCommitBytes {
		t.Errorf("CommitBytes=%d want %d", opt.CommitBytes, defaultCommitBytes)
	}
	if opt.CommitInterval != defaultCommitInterval {
		t.Errorf("CommitInterval=%v want %v", opt.CommitInterval, defaultCommitInterval)
	}
}

func TestResolveOptions_ExplicitMB(t *testing.T) {
	opt := ResolveOptions(RawOptions{
		ReadWorkers:   8,
		WriteWorkers:  6,
		BufferMB:      64,
		BatchBytesMB:  2,
		CommitRows:    5000,
		CommitBytesMB: 16,
		BatchSize:     2000,
	})
	if opt.ReadWorkers != 8 || opt.WriteWorkers != 6 {
		t.Errorf("workers=%d/%d", opt.ReadWorkers, opt.WriteWorkers)
	}
	if opt.BufferBytes != 64*1024*1024 {
		t.Errorf("buffer=%d", opt.BufferBytes)
	}
	if opt.BatchBytes != 2*1024*1024 {
		t.Errorf("batchbytes=%d", opt.BatchBytes)
	}
	if opt.CommitRows != 5000 {
		t.Errorf("commitrows=%d", opt.CommitRows)
	}
	if opt.CommitBytes != 16*1024*1024 {
		t.Errorf("commitbytes=%d", opt.CommitBytes)
	}
	if opt.BatchRows != 2000 {
		t.Errorf("batchrows=%d", opt.BatchRows)
	}
}

func TestResolveOptions_LegacyCommitDerivation(t *testing.T) {
	// 未设置 CommitRows 但显式设置旧字段：提交行数 = batch_size × 该值。
	opt := ResolveOptions(RawOptions{
		BatchSize:                    1000,
		LegacyTxCommitEveryNParallel: 5,
	})
	if opt.CommitRows != 5000 {
		t.Errorf("legacy derived CommitRows=%d want 5000", opt.CommitRows)
	}
	// 显式 CommitRows 优先于旧字段推导。
	opt2 := ResolveOptions(RawOptions{
		BatchSize:                    1000,
		LegacyTxCommitEveryNParallel: 5,
		CommitRows:                   20000,
	})
	if opt2.CommitRows != 20000 {
		t.Errorf("explicit CommitRows=%d want 20000", opt2.CommitRows)
	}
}

func TestResolveOptions_CommitFloorAtBatch(t *testing.T) {
	// 提交阈值不应小于单条 INSERT 上限。
	opt := ResolveOptions(RawOptions{
		BatchSize:  5000,
		CommitRows: 100, // 小于 batch，应被抬升到 batch
	})
	if opt.CommitRows < int64(opt.BatchRows) {
		t.Errorf("CommitRows=%d should be >= BatchRows=%d", opt.CommitRows, opt.BatchRows)
	}
}

func TestResolveOptions_Clamp(t *testing.T) {
	opt := ResolveOptions(RawOptions{
		ReadWorkers: 1000, WriteWorkers: -3, BatchSize: 999999999,
		BufferMB: int(^uint(0) >> 1), BatchBytesMB: int(^uint(0) >> 1),
		CommitRows: int(^uint(0) >> 1), CommitBytesMB: int(^uint(0) >> 1),
	})
	if opt.ReadWorkers != hardMaxWorkers {
		t.Errorf("ReadWorkers=%d want clamp %d", opt.ReadWorkers, hardMaxWorkers)
	}
	if opt.WriteWorkers != defaultWriteWorkers {
		t.Errorf("WriteWorkers=%d want default %d", opt.WriteWorkers, defaultWriteWorkers)
	}
	if opt.BatchRows != hardMaxBatchRow {
		t.Errorf("BatchRows=%d want clamp %d", opt.BatchRows, hardMaxBatchRow)
	}
	if opt.BufferBytes != int64(hardMaxBufferMB)*1024*1024 {
		t.Errorf("BufferBytes=%d want capped MiB=%d", opt.BufferBytes, hardMaxBufferMB)
	}
	if opt.BatchBytes != int64(hardMaxBatchMB)*1024*1024 {
		t.Errorf("BatchBytes=%d want capped MiB=%d", opt.BatchBytes, hardMaxBatchMB)
	}
	if opt.CommitRows != hardMaxCommitRows {
		t.Errorf("CommitRows=%d want cap %d", opt.CommitRows, hardMaxCommitRows)
	}
	if opt.CommitBytes != int64(hardMaxCommitMB)*1024*1024 {
		t.Errorf("CommitBytes=%d want capped MiB=%d", opt.CommitBytes, hardMaxCommitMB)
	}
}

func TestResolveOptions_LegacyCommitDerivationCannotOverflow(t *testing.T) {
	opt := ResolveOptions(RawOptions{
		BatchSize:                    hardMaxBatchRow,
		LegacyTxCommitEveryNParallel: int(^uint(0) >> 1),
	})
	if opt.CommitRows != hardMaxCommitRows {
		t.Fatalf("CommitRows=%d want cap %d", opt.CommitRows, hardMaxCommitRows)
	}
}

func TestResolveOptions_CommitIntervalStable(t *testing.T) {
	opt := ResolveOptions(RawOptions{})
	if opt.CommitInterval != 2*time.Second {
		t.Errorf("commit interval=%v", opt.CommitInterval)
	}
}
