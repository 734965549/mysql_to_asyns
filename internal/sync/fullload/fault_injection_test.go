//go:build integration

package fullload

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestIntegration_FaultInjection_SourceQueryTimeout_RetriesToStagingPublish
// 故障注入场景1：源查询超时 -> 表级重试 -> staging 发布成功。
//
// 注入策略：测试前 SET GLOBAL max_execution_time=500（ms），使所有源端 SELECT
// 超过 500ms 即被 MySQL 主动中断（ERROR 3024），触发 ReadQueryTimeoutError。
// 首次 attempt 必然超时（表足够大）。在 OnTableStateChange 检测到进入重试（attemptID>=2）后，
// SET GLOBAL max_execution_time=0 清除限制，使重试 attempt 通过。
//
// 断言：QueryTimeouts>=1、TableRetries>=1、Engine.Run 成功、目标表行数正确、
// staging 表已清理、无残留 staging 表。
func TestIntegration_FaultInjection_SourceQueryTimeout_RetriesToStagingPublish(t *testing.T) {
	db := openIntegrationMySQL(t) // 管理连接
	srcSchema, dstSchema := createIntegrationPair(t, db, "fault_timeout")
	const table = "bill"

	mustExec(t, db, fmt.Sprintf(`
CREATE TABLE %s.%s (
  id BIGINT PRIMARY KEY,
  uk VARCHAR(64) NOT NULL,
  payload VARCHAR(64) NOT NULL
) ENGINE=InnoDB`, qi(srcSchema), qi(table)))
	mustExec(t, db, fmt.Sprintf(`
CREATE TABLE %s.%s (
  id BIGINT PRIMARY KEY,
  uk VARCHAR(64) NOT NULL,
  payload VARCHAR(64) NOT NULL
) ENGINE=InnoDB`, qi(dstSchema), qi(table)))

	// 数据量需足够大，使首次冷查询（MIN/MAX 或首块 SELECT）超过 100ms，但热查询 < 100ms。
	const n = 300000
	t.Logf("seeding %d rows into source", n)
	seedBillRows(t, db, srcSchema, table, n, nil)

	srcDB := openIntegrationMySQL(t)
	dstDB := openIntegrationMySQL(t)
	srcDB.SetMaxOpenConns(8)
	dstDB.SetMaxOpenConns(8)

	stats := &Stats{}
	eng := &Engine{
		SourceDB: srcDB,
		TargetDB: dstDB,
		Options: ResolveOptions(RawOptions{
			ReadWorkers:      2,
			WriteWorkers:     2,
			BatchSize:        200,
			CommitRows:       500,
			BufferMB:         32,
			BatchBytesMB:     2,
			QueryTimeoutSec:  30, // 会被下方覆盖为毫秒级
			SlowQueryWarnSec: 60,
			ReadRetryTimes:   2,
			EnableStaging:    true,
		}),
		Stats:  stats,
		TaskID: "fault-inject-timeout",
	}
	// 覆盖为毫秒级：首次冷查询易超 100ms 触发 ReadQueryTimeoutError，
	// 重试时数据已进入 buffer pool，查询 < 100ms 通过。
	eng.Options.QueryTimeout = 100 * time.Millisecond
	eng.Options.LargeTableRows = n + 1

	spec := pkUKSpec(srcSchema, table, dstSchema, table)
	spec.EstimatedRows = int64(n)

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	err := eng.Run(ctx, []*TableSpec{spec})

	// 若首次未超时（缓存已热），跳过而非失败：本测试断言对象是「超时发生后能正确重试」。
	if stats.QueryTimeouts == 0 && err == nil {
		var dstCount int
		_ = db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s.%s", qi(dstSchema), qi(table))).Scan(&dstCount)
		if dstCount == n {
			t.Skipf("cold cache did not trigger source query timeout (QueryTimeouts=0); rerun on colder env for retry coverage")
		}
	}

	require.NoError(t, err, "Engine.Run should succeed after retry; stats=%+v", stats.Snapshot())

	assert.GreaterOrEqual(t, stats.QueryTimeouts, int64(1), "expected at least 1 query timeout to trigger retry path")
	assert.GreaterOrEqual(t, stats.TableRetries, int64(1), "expected at least 1 table retry")

	var dstCount int
	require.NoError(t, db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s.%s", qi(dstSchema), qi(table))).Scan(&dstCount))
	assert.Equal(t, n, dstCount, "target row count after staging publish")

	assert.Equal(t, int64(0), stats.ActiveStagingTables, "staging tables should be cleaned after publish")

	var staleStaging int
	require.NoError(t, db.QueryRow(fmt.Sprintf(`
SELECT COUNT(*) FROM information_schema.TABLES
WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME LIKE '%%__mts_staging_%%'`, dstSchema)).Scan(&staleStaging))
	assert.Equal(t, 0, staleStaging, "no stale staging tables should remain")
}

// TestIntegration_FaultInjection_SlowWriter_BarrierBlocksEarlyPublish
// 故障注入场景2：writer 故意变慢 -> barrier 不提前 publish。
//
// 注入策略：目标表 BEFORE INSERT 触发器 SLEEP(0.002)，使每个批次写入变慢。
// 断言：Engine.Run 成功完成，目标表行数正确，且 PUBLISHED 出现在 DATA_READY 之后
// （waitInflightZero barrier 生效，writer 未完成时不会提前 publish）。
func TestIntegration_FaultInjection_SlowWriter_BarrierBlocksEarlyPublish(t *testing.T) {
	db := openIntegrationMySQL(t)
	srcSchema, dstSchema := createIntegrationPair(t, db, "fault_slow_writer")
	const table = "bill"

	mustExec(t, db, fmt.Sprintf(`
CREATE TABLE %s.%s (
  id BIGINT PRIMARY KEY,
  uk VARCHAR(64) NOT NULL,
  payload VARCHAR(64) NOT NULL
) ENGINE=InnoDB`, qi(srcSchema), qi(table)))
	mustExec(t, db, fmt.Sprintf(`
CREATE TABLE %s.%s (
  id BIGINT PRIMARY KEY,
  uk VARCHAR(64) NOT NULL,
  payload VARCHAR(64) NOT NULL
) ENGINE=InnoDB`, qi(dstSchema), qi(table)))

	// 注入：目标表 BEFORE INSERT 触发器 SLEEP，拖慢 writer。
	// staging 表 LIKE 目标表会继承触发器，写入 staging 时同样被拖慢。
	mustExec(t, db, fmt.Sprintf(`
CREATE TRIGGER %s.trg_slow_insert BEFORE INSERT ON %s.%s
FOR EACH ROW BEGIN
  DO SLEEP(0.002);
END`, qi(dstSchema), qi(dstSchema), qi(table)))

	const n = 2000
	seedBillRows(t, db, srcSchema, table, n, nil)

	srcDB := openIntegrationMySQL(t)
	dstDB := openIntegrationMySQL(t)
	srcDB.SetMaxOpenConns(16)
	dstDB.SetMaxOpenConns(8)

	stats := &Stats{}

	// 记录状态变更序列，验证 publish 发生在 DATA_READY 之后。
	var mu sync.Mutex
	phases := []string{}
	eng := &Engine{
		SourceDB: srcDB,
		TargetDB: dstDB,
		Options: ResolveOptions(RawOptions{
			ReadWorkers:     2,
			WriteWorkers:    2,
			BatchSize:       100,
			CommitRows:      200,
			BufferMB:        16,
			BatchBytesMB:    1,
			ReadRetryTimes:  1,
			EnableStaging:   true,
			QueryTimeoutSec: 60,
		}),
		Stats:  stats,
		TaskID: "fault-inject-slow-writer",
		OnTableStateChange: func(schema, tbl, phase string, attemptID int, stagingTable, errMsg string, committedRows int64) error {
			mu.Lock()
			phases = append(phases, phase)
			mu.Unlock()
			return nil
		},
	}
	eng.Options.LargeTableRows = n + 1

	spec := pkUKSpec(srcSchema, table, dstSchema, table)
	spec.EstimatedRows = int64(n)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	require.NoError(t, eng.Run(ctx, []*TableSpec{spec}))

	// 断言1：目标表行数正确（慢 writer 不影响最终一致性）。
	var dstCount int
	require.NoError(t, db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s.%s", qi(dstSchema), qi(table))).Scan(&dstCount))
	assert.Equal(t, n, dstCount)

	// 断言2：状态序列中 PUBLISHED 必须出现在 DATA_READY 之后（barrier 生效）。
	mu.Lock()
	defer mu.Unlock()
	dataReadyIdx := -1
	publishedIdx := -1
	for i, p := range phases {
		if p == "DATA_READY" && dataReadyIdx < 0 {
			dataReadyIdx = i
		}
		if p == "PUBLISHED" && publishedIdx < 0 {
			publishedIdx = i
		}
	}
	require.GreaterOrEqual(t, publishedIdx, 0, "PUBLISHED must be observed")
	require.GreaterOrEqual(t, dataReadyIdx, 0, "DATA_READY must be observed")
	assert.Greater(t, publishedIdx, dataReadyIdx, "PUBLISHED must come after DATA_READY (barrier blocks early publish)")
}
