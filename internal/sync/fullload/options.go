// Package fullload 实现全量同步 V2 任务级流水线引擎。
//
// V2 引擎不再让每张表独立创建完整的读写 worker 组，而是采用任务级流水线：
//
//	Chunk 生成 → 源读取 worker 池 → 有界 RowBatch 队列 → 目标写入 worker 池 → 事务提交
//
// 引擎与 V1（internal/task 内联的 syncDatabasePair）并行存在，通过任务配置
// full_load_engine=v2 灰度开关分发；V1 完全保留、默认行为不变。
package fullload

import "time"

// 4C8G 平衡预设默认值。所有 <=0 的配置项都会回退到这里。
const (
	defaultReadWorkers    = 4
	defaultWriteWorkers   = 4
	defaultBufferBytes    = 128 * 1024 * 1024 // 128 MiB
	defaultBatchBytes     = 4 * 1024 * 1024   // 4 MiB
	defaultBatchRows      = 1000
	defaultCommitRows     = 10000
	defaultCommitBytes    = 32 * 1024 * 1024 // 32 MiB
	defaultCommitInterval = 2 * time.Second
	defaultChunkOvershoot = 8 // chunk 数量约为读取 worker 数的 8 倍，用于工作窃取消除长尾

	// mysqlMaxPlaceholders 单条预处理语句占位符上限（留余量），与 writer 包保持一致。
	mysqlMaxPlaceholders = 62000

	hardMaxWorkers  = 64
	hardMaxBatchRow = 100000
)

// RawOptions 是从任务配置直接读取的原始参数（未经默认值推导）。
type RawOptions struct {
	ReadWorkers   int // full_load_read_workers
	WriteWorkers  int // full_load_write_workers
	BufferMB      int // full_load_buffer_mb
	BatchBytesMB  int // full_load_batch_bytes_mb
	CommitRows    int // full_load_commit_rows
	CommitBytesMB int // full_load_commit_bytes_mb

	BatchSize int // batch_size（单条 INSERT 行数上限，与 V1 语义一致）

	// LegacyTxCommitEveryNParallel 兼容旧字段：显式设置且未设置 CommitRows 时，
	// 提交行数按 BatchSize × 该值推导。
	LegacyTxCommitEveryNParallel int

	SkipBinlog bool // enable_skip_binlog
}

// Options 是经过默认值推导后引擎实际使用的运行参数。
type Options struct {
	ReadWorkers    int
	WriteWorkers   int
	BufferBytes    int64
	BatchRows      int
	BatchBytes     int64
	CommitRows     int64
	CommitBytes    int64
	CommitInterval time.Duration
	ChunkOvershoot int
	SkipBinlog     bool
}

// ResolveOptions 将原始配置推导为生效运行参数，应用 4C8G 平衡预设。
func ResolveOptions(raw RawOptions) Options {
	opt := Options{
		ReadWorkers:    clampInt(raw.ReadWorkers, defaultReadWorkers, 1, hardMaxWorkers),
		WriteWorkers:   clampInt(raw.WriteWorkers, defaultWriteWorkers, 1, hardMaxWorkers),
		BatchRows:      clampInt(raw.BatchSize, defaultBatchRows, 1, hardMaxBatchRow),
		CommitInterval: defaultCommitInterval,
		ChunkOvershoot: defaultChunkOvershoot,
		SkipBinlog:     raw.SkipBinlog,
	}

	if raw.BufferMB > 0 {
		opt.BufferBytes = int64(raw.BufferMB) * 1024 * 1024
	} else {
		opt.BufferBytes = defaultBufferBytes
	}

	if raw.BatchBytesMB > 0 {
		opt.BatchBytes = int64(raw.BatchBytesMB) * 1024 * 1024
	} else {
		opt.BatchBytes = defaultBatchBytes
	}

	switch {
	case raw.CommitRows > 0:
		opt.CommitRows = int64(raw.CommitRows)
	case raw.LegacyTxCommitEveryNParallel > 0 && opt.BatchRows > 0:
		// 兼容旧字段：提交行数 = batch_size × tx_commit_every_n_parallel。
		opt.CommitRows = int64(opt.BatchRows) * int64(raw.LegacyTxCommitEveryNParallel)
	default:
		opt.CommitRows = defaultCommitRows
	}

	if raw.CommitBytesMB > 0 {
		opt.CommitBytes = int64(raw.CommitBytesMB) * 1024 * 1024
	} else {
		opt.CommitBytes = defaultCommitBytes
	}

	// 单事务提交阈值不应小于单条 INSERT 上限，否则每批都提交、失去合并意义。
	if opt.CommitRows < int64(opt.BatchRows) {
		opt.CommitRows = int64(opt.BatchRows)
	}
	if opt.CommitBytes < opt.BatchBytes {
		opt.CommitBytes = opt.BatchBytes
	}

	return opt
}

func clampInt(v, def, min, max int) int {
	if v <= 0 {
		v = def
	}
	if v < min {
		v = min
	}
	if v > max {
		v = max
	}
	return v
}
