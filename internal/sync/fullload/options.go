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
	defaultChunkOvershoot = 8 // 每表 chunk 数量约为读取 worker 数的 8 倍，用于单连接顺序分段

	// 超大表判定：估算行数达到该阈值且 chunk>1 时，走短暂 FTWRL 对齐多连接快照。
	defaultLargeTableRows = int64(1_000_000)
	// 取表锁等待超时（秒），同时用于客户端 context 与 SESSION lock_wait_timeout。
	defaultLockWaitTimeoutSec = 10

	// mysqlMaxPlaceholders 单条预处理语句占位符上限（留余量），与 writer 包保持一致。
	mysqlMaxPlaceholders = 62000

	hardMaxWorkers    = 64
	hardMaxBatchRow   = 100000
	hardMaxBufferMB   = 4096
	hardMaxBatchMB    = 64
	hardMaxCommitMB   = 4096
	hardMaxCommitRows = 10000000
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

	// TableParallelReaders 单表内对齐快照的最大并行读连接数（超大表）。
	TableParallelReaders int
	// LargeTableRows 触发单表多连接对齐快照的估算行数阈值。
	LargeTableRows int64
	// MaxSnapshotGroups 同时活跃的表级 snapshot group 上限（并发表数）。
	MaxSnapshotGroups int
	// MaxSnapshotConns 同时持有的快照连接上限（含短暂协调锁连接预留）。
	MaxSnapshotConns int
	// LockWaitTimeoutSec 取表锁的双保险超时（context + SESSION lock_wait_timeout）。
	LockWaitTimeoutSec int
	// DegradeOnAlignLockFail 对齐多连接取锁失败时是否降级为单连接快照（ALL+无PK 捕获 HWM 时仍 fail-closed）。
	DegradeOnAlignLockFail bool
}

// ResolveOptions 将原始配置推导为生效运行参数，应用 4C8G 平衡预设。
func ResolveOptions(raw RawOptions) Options {
	opt := Options{
		ReadWorkers:            clampInt(raw.ReadWorkers, defaultReadWorkers, 1, hardMaxWorkers),
		WriteWorkers:           clampInt(raw.WriteWorkers, defaultWriteWorkers, 1, hardMaxWorkers),
		BatchRows:              clampInt(raw.BatchSize, defaultBatchRows, 1, hardMaxBatchRow),
		CommitInterval:         defaultCommitInterval,
		ChunkOvershoot:         defaultChunkOvershoot,
		SkipBinlog:             raw.SkipBinlog,
		LargeTableRows:         defaultLargeTableRows,
		LockWaitTimeoutSec:     defaultLockWaitTimeoutSec,
		DegradeOnAlignLockFail: true,
	}

	opt.BufferBytes = mebibytes(raw.BufferMB, defaultBufferBytes, hardMaxBufferMB)
	opt.BatchBytes = mebibytes(raw.BatchBytesMB, defaultBatchBytes, hardMaxBatchMB)

	switch {
	case raw.CommitRows > 0:
		opt.CommitRows = int64(clampUpper(raw.CommitRows, hardMaxCommitRows))
	case raw.LegacyTxCommitEveryNParallel > 0 && opt.BatchRows > 0:
		// 兼容旧字段：提交行数 = batch_size × tx_commit_every_n_parallel。
		legacyN := int64(raw.LegacyTxCommitEveryNParallel)
		maxN := int64(hardMaxCommitRows / opt.BatchRows)
		if legacyN > maxN {
			legacyN = maxN
		}
		opt.CommitRows = int64(opt.BatchRows) * legacyN
	default:
		opt.CommitRows = defaultCommitRows
	}

	opt.CommitBytes = mebibytes(raw.CommitBytesMB, defaultCommitBytes, hardMaxCommitMB)

	// 单事务提交阈值不应小于单条 INSERT 上限，否则每批都提交、失去合并意义。
	if opt.CommitRows < int64(opt.BatchRows) {
		opt.CommitRows = int64(opt.BatchRows)
	}
	if opt.CommitBytes < opt.BatchBytes {
		opt.CommitBytes = opt.BatchBytes
	}

	// 表级并行与信号量：默认单表并行度=ReadWorkers，并发表数=ReadWorkers，
	// 连接上限预留协调锁连接（每组 +1）。
	opt.TableParallelReaders = opt.ReadWorkers
	opt.MaxSnapshotGroups = opt.ReadWorkers
	opt.MaxSnapshotConns = opt.ReadWorkers * (opt.TableParallelReaders + 1)

	return opt
}

// CapBySourcePool 用真实源库连接池上限约束快照预算，避免 limiter 允许超过 pool 的占用量，
// 进而在持表锁后阻塞于 db.Conn。maxOpen<=0 时不调整。
func (opt *Options) CapBySourcePool(maxOpen int) {
	if opt == nil || maxOpen <= 0 {
		return
	}
	// 单组最坏需要 readers + 1（协调锁连接）。
	if opt.TableParallelReaders+1 > maxOpen {
		opt.TableParallelReaders = maxOpen - 1
		if opt.TableParallelReaders < 1 {
			opt.TableParallelReaders = 1
		}
	}
	if opt.MaxSnapshotConns > maxOpen {
		opt.MaxSnapshotConns = maxOpen
	}
	if opt.MaxSnapshotGroups > maxOpen {
		opt.MaxSnapshotGroups = maxOpen
	}
	// 保证至少能开一组单 reader（可选 +锁）。
	minPerGroup := 1
	if opt.TableParallelReaders > 1 {
		minPerGroup = opt.TableParallelReaders + 1
	}
	if opt.MaxSnapshotConns < minPerGroup {
		opt.MaxSnapshotConns = minPerGroup
		if opt.MaxSnapshotConns > maxOpen {
			opt.MaxSnapshotConns = maxOpen
		}
	}
}

func mebibytes(valueMB int, defaultBytes int64, maxMB int) int64 {
	if valueMB <= 0 {
		return defaultBytes
	}
	return int64(clampUpper(valueMB, maxMB)) * 1024 * 1024
}

func clampUpper(value, max int) int {
	if value > max {
		return max
	}
	return value
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
