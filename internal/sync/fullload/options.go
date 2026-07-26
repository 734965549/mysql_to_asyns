// Package fullload 实现全量同步 V2 任务级流水线引擎。
//
// V2 引擎不再让每张表独立创建完整的读写 worker 组，而是采用任务级流水线：
//
//	Chunk 生成 → 源读取 worker 池 → 有界 RowBatch 队列 → 目标写入 worker 池 → 事务提交
//
// 引擎与 V1（internal/task 内联的 syncDatabasePair）并行存在，通过任务配置
// full_load_engine=v2 灰度开关分发；V1 完全保留、默认行为不变。
package fullload

import (
	"fmt"
	"time"
)

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

	// 单次源端查询超时（秒）：keyset 为整次查询绝对超时；stream 仅覆盖打开查询。
	defaultQueryTimeoutSec = 300
	// 无主键流式查询无进展超时（秒）：每次 Rows.Next 成功后重置；等待写队列时暂停。
	defaultStreamIdleTimeoutSec = 300
	// 慢查询告警阈值（秒），超过后输出一次告警。
	defaultSlowQueryWarnSec = 30
	// 查询超时/慢查询/流式无进展阈值上限（秒）。
	hardMaxQueryTimeoutSec = 7200
	// 流式查询绝对最长时长上限（秒）；0 表示不限制总时长。
	hardMaxStreamMaxDurationSec = 86400

	// mysqlMaxPlaceholders 单条预处理语句占位符上限（留余量），与 writer 包保持一致。
	mysqlMaxPlaceholders = 62000

	hardMaxWorkers            = 64
	hardMaxBatchRow           = 100000
	hardMaxBufferMB           = 4096
	hardMaxBatchMB            = 64
	hardMaxCommitMB           = 4096
	hardMaxCommitRows         = 10000000
	hardMaxLockWaitTimeoutSec = 3600
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

	// LockWaitTimeoutSec 取表锁等待超时（秒）；<=0 时使用默认 10。
	LockWaitTimeoutSec int
	// DegradeOnAlignLockFail nil 时默认 true（降级单连接）；显式 false 为 fail-closed。
	DegradeOnAlignLockFail *bool

	// QueryTimeoutSec 单次源端查询超时（秒）；<=0 时使用默认 300。
	// keyset：整次查询绝对超时；stream：仅打开查询等待上限。
	QueryTimeoutSec int
	// StreamIdleTimeoutSec 无主键流式查询无进展超时（秒）；<=0 时使用默认 300。
	StreamIdleTimeoutSec int
	// StreamMaxDurationSec 无主键流式查询绝对最长时长（秒）；0=不限制总时长。
	StreamMaxDurationSec int
	// SlowQueryWarnSec 慢查询告警阈值（秒）；<=0 时使用默认 30。
	SlowQueryWarnSec int
	// TableNoProgressSec 表无进展告警阈值（秒）；<=0 时关闭。P0 阶段预留，未实现。
	TableNoProgressSec int
	// ReadRetryTimes 表级读取自动重试次数；<=0 时不启用重试。
	ReadRetryTimes int
	// EnableTwoPhaseRead 启用单列 PK 两阶段读取（pk_probe + payload_fetch）。
	EnableTwoPhaseRead bool
	// EnableStaging 启用 staging 表隔离：全量数据先写入 staging 表，完成后原子 RENAME 发布。
	EnableStaging bool
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

	// QueryTimeout 单次源端查询超时（keyset 绝对超时；stream 仅打开查询）。
	QueryTimeout time.Duration
	// StreamIdleTimeout 无主键流式查询无进展超时。
	StreamIdleTimeout time.Duration
	// StreamMaxDuration 无主键流式查询绝对最长时长；0=不限制。
	StreamMaxDuration time.Duration
	// SlowQueryWarnThreshold 慢查询告警阈值。
	SlowQueryWarnThreshold time.Duration
	// TableNoProgressSec 表无进展告警阈值（秒）；0=关闭。P0 未使用。
	TableNoProgressSec int
	// ReadRetryTimes 表级读取额外重试次数；0=仅首次 attempt，不重试。
	// 语义：总 attempt 数 = 1 + ReadRetryTimes。
	ReadRetryTimes int
	// TwoPhaseRead 启用单列 PK 两阶段读取。仅单列 PK 表生效。
	TwoPhaseRead bool
	// StagingEnabled 启用 staging 表隔离：全量数据先写入 staging 表，完成后原子 RENAME 发布。
	StagingEnabled bool
}

// Validate 校验运行参数的安全约束（fail-closed）。
// ReadRetryTimes>0 时必须启用 staging，否则首次 attempt 已提交的数据无法撤销，重跑会主键冲突或混合快照。
func (opt Options) Validate() error {
	if opt.ReadRetryTimes > 0 && !opt.StagingEnabled {
		return fmt.Errorf("full_load_read_retry_times=%d requires full_load_enable_staging=true (fail-closed: retry without staging would duplicate or conflict on the final table)", opt.ReadRetryTimes)
	}
	return nil
}

// ResolveOptions 将原始配置推导为生效运行参数，应用 4C8G 平衡预设。
func ResolveOptions(raw RawOptions) Options {
	degrade := true
	if raw.DegradeOnAlignLockFail != nil {
		degrade = *raw.DegradeOnAlignLockFail
	}

	opt := Options{
		ReadWorkers:            clampInt(raw.ReadWorkers, defaultReadWorkers, 1, hardMaxWorkers),
		WriteWorkers:           clampInt(raw.WriteWorkers, defaultWriteWorkers, 1, hardMaxWorkers),
		BatchRows:              clampInt(raw.BatchSize, defaultBatchRows, 1, hardMaxBatchRow),
		CommitInterval:         defaultCommitInterval,
		ChunkOvershoot:         defaultChunkOvershoot,
		SkipBinlog:             raw.SkipBinlog,
		LargeTableRows:         defaultLargeTableRows,
		LockWaitTimeoutSec:     clampInt(raw.LockWaitTimeoutSec, defaultLockWaitTimeoutSec, 1, hardMaxLockWaitTimeoutSec),
		DegradeOnAlignLockFail: degrade,
	}

	// 单次查询超时、流式无进展超时与慢查询告警阈值。
	opt.QueryTimeout = time.Duration(clampInt(raw.QueryTimeoutSec, defaultQueryTimeoutSec, 1, hardMaxQueryTimeoutSec)) * time.Second
	opt.StreamIdleTimeout = time.Duration(clampInt(raw.StreamIdleTimeoutSec, defaultStreamIdleTimeoutSec, 1, hardMaxQueryTimeoutSec)) * time.Second
	// StreamMaxDuration：0 表示不限制；显式正值夹到 [1, hardMax]。
	if raw.StreamMaxDurationSec > 0 {
		opt.StreamMaxDuration = time.Duration(clampInt(raw.StreamMaxDurationSec, 0, 1, hardMaxStreamMaxDurationSec)) * time.Second
	}
	opt.SlowQueryWarnThreshold = time.Duration(clampInt(raw.SlowQueryWarnSec, defaultSlowQueryWarnSec, 1, hardMaxQueryTimeoutSec)) * time.Second
	opt.TableNoProgressSec = raw.TableNoProgressSec             // P0 阶段暂不使用，直接透传
	opt.ReadRetryTimes = clampInt(raw.ReadRetryTimes, 0, 0, 10) // P2：表级重试次数，上限 10
	opt.TwoPhaseRead = raw.EnableTwoPhaseRead                   // P1：单列 PK 两阶段读取开关
	opt.StagingEnabled = raw.EnableStaging                      // P2.3：staging 表隔离开关

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
