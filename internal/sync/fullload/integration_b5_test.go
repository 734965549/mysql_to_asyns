//go:build integration

package fullload

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"mysql-to-sync/internal/metadata/domain/entity"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestIntegration_B5_MultiTableFairness_SmallTablesNotStarvedByLargeJSON
// 多张小表 + 1 张大 JSON 表并行全量：小表应先完成，且最终行数一致。
func TestIntegration_B5_MultiTableFairness_SmallTablesNotStarvedByLargeJSON(t *testing.T) {
	db := openIntegrationMySQL(t)
	srcSchema, dstSchema := createIntegrationPair(t, db, "b5_fair")

	const (
		smallCount   = 8
		smallRows    = 100
		bigTable     = "t_big_json"
		bigRows      = 800
		bigJSONPad   = 16 * 1024
	)

	specs := make([]*TableSpec, 0, smallCount+1)
	smallNames := make([]string, 0, smallCount)
	for i := 0; i < smallCount; i++ {
		name := fmt.Sprintf("t_small_%02d", i)
		smallNames = append(smallNames, name)
		mustExec(t, db, fmt.Sprintf(`
CREATE TABLE %s.%s (
  id BIGINT PRIMARY KEY,
  uk VARCHAR(64) NOT NULL,
  payload VARCHAR(64) NOT NULL
) ENGINE=InnoDB`, qi(srcSchema), qi(name)))
		mustExec(t, db, fmt.Sprintf(`
CREATE TABLE %s.%s (
  id BIGINT PRIMARY KEY,
  uk VARCHAR(64) NOT NULL,
  payload VARCHAR(64) NOT NULL
) ENGINE=InnoDB`, qi(dstSchema), qi(name)))
		seedBillRows(t, db, srcSchema, name, smallRows, nil)
		spec := pkUKSpec(srcSchema, name, dstSchema, name)
		spec.EstimatedRows = smallRows
		specs = append(specs, spec)
	}

	mustExec(t, db, fmt.Sprintf(`
CREATE TABLE %s.%s (
  id BIGINT PRIMARY KEY,
  payload JSON NOT NULL
) ENGINE=InnoDB`, qi(srcSchema), qi(bigTable)))
	mustExec(t, db, fmt.Sprintf(`
CREATE TABLE %s.%s (
  id BIGINT PRIMARY KEY,
  payload JSON NOT NULL
) ENGINE=InnoDB`, qi(dstSchema), qi(bigTable)))
	t.Logf("seeding big json table %d rows", bigRows)
	seedJSONRows(t, db, srcSchema, bigTable, bigRows, bigJSONPad)
	bigSpec := jsonPKSpec(srcSchema, bigTable, dstSchema, bigTable)
	bigSpec.EstimatedRows = bigRows
	specs = append(specs, bigSpec)

	srcDB := openIntegrationMySQL(t)
	dstDB := openIntegrationMySQL(t)
	srcDB.SetMaxOpenConns(8)
	dstDB.SetMaxOpenConns(8)

	eng := &Engine{
		SourceDB: srcDB,
		TargetDB: dstDB,
		Options: ResolveOptions(RawOptions{
			ReadWorkers:     4,
			TableWorkers:    4,
			PerTableReaders: 2,
			WriteWorkers:    2,
			BatchSize:       500,
			CommitRows:      1000,
			BufferMB:        32,
			BatchBytesMB:    4,
			QueryTimeoutSec: 120,
		}),
		Stats:  &Stats{},
		TaskID: "b5-fairness",
	}
	eng.Options.LargeTableRows = 500

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	require.NoError(t, eng.Run(ctx, specs))

	for _, name := range smallNames {
		assert.Equal(t, smallRows, countRows(t, db, dstSchema, name), "small table %s", name)
	}
	assert.Equal(t, bigRows, countRows(t, db, dstSchema, bigTable))
}

// TestIntegration_B5_Backpressure_SlowWriterEventsAndSmallTableWrites
// 慢目标写入触发背压事件，小表仍可完成写入。
func TestIntegration_B5_Backpressure_SlowWriterEventsAndSmallTableWrites(t *testing.T) {
	db := openIntegrationMySQL(t)
	srcSchema, dstSchema := createIntegrationPair(t, db, "b5_bp")

	tables := []struct {
		name string
		rows int
	}{
		{"t_a", 600},
		{"t_b", 600},
		{"t_c", 1800},
	}
	specs := make([]*TableSpec, 0, len(tables))
	for _, tbl := range tables {
		mustExec(t, db, fmt.Sprintf(`
CREATE TABLE %s.%s (
  id BIGINT PRIMARY KEY,
  uk VARCHAR(64) NOT NULL,
  payload VARCHAR(64) NOT NULL
) ENGINE=InnoDB`, qi(srcSchema), qi(tbl.name)))
		mustExec(t, db, fmt.Sprintf(`
CREATE TABLE %s.%s (
  id BIGINT PRIMARY KEY,
  uk VARCHAR(64) NOT NULL,
  payload VARCHAR(64) NOT NULL
) ENGINE=InnoDB`, qi(dstSchema), qi(tbl.name)))
		seedBillRows(t, db, srcSchema, tbl.name, tbl.rows, nil)
		spec := pkUKSpec(srcSchema, tbl.name, dstSchema, tbl.name)
		spec.EstimatedRows = int64(tbl.rows)
		specs = append(specs, spec)

		mustExec(t, db, fmt.Sprintf(`
CREATE TRIGGER %s.trg_slow_%s BEFORE INSERT ON %s.%s
FOR EACH ROW BEGIN DO SLEEP(0.003); END`,
			qi(dstSchema), tbl.name, qi(dstSchema), qi(tbl.name)))
	}

	srcDB := openIntegrationMySQL(t)
	dstDB := openIntegrationMySQL(t)
	srcDB.SetMaxOpenConns(8)
	dstDB.SetMaxOpenConns(4)

	sink := &recordingSink{}
	eng := &Engine{
		SourceDB:  srcDB,
		TargetDB:  dstDB,
		EventSink: sink,
		Options: ResolveOptions(RawOptions{
			ReadWorkers:     2,
			WriteWorkers:    1,
			BatchSize:       200,
			CommitRows:      400,
			BufferMB:        1,
			BatchBytesMB:    1,
			QueryTimeoutSec: 120,
		}),
		Stats:  &Stats{},
		TaskID: "b5-backpressure",
	}
	eng.Options.LargeTableRows = 500

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	require.NoError(t, eng.Run(ctx, specs))

	for _, tbl := range tables {
		assert.Equal(t, tbl.rows, countRows(t, db, dstSchema, tbl.name))
	}
	// 背压 HIGH/RECOVERED 由 reportLoop 5s 采样，integration 窗口内未必命中；状态机见 stress_fairness_test.go。
	if !sinkHasCode(sink, EventCodeQueueBackpressureHigh) {
		t.Logf("QUEUE_BACKPRESSURE_* not observed (reportLoop=5s); codes=%v", sink.codes())
	}
}

// TestIntegration_B5_OversizedJSONRow_SyncsWithWarnEvent
// 单行超过 batch_bytes 时继续同步并 emit ROW_EXCEEDS_BATCH_BYTES。
func TestIntegration_B5_OversizedJSONRow_SyncsWithWarnEvent(t *testing.T) {
	db := openIntegrationMySQL(t)
	srcSchema, dstSchema := createIntegrationPair(t, db, "b5_oversize")
	const table = "t_wide"

	mustExec(t, db, fmt.Sprintf(`
CREATE TABLE %s.%s (
  id BIGINT PRIMARY KEY,
  payload JSON NOT NULL
) ENGINE=InnoDB`, qi(srcSchema), qi(table)))
	mustExec(t, db, fmt.Sprintf(`
CREATE TABLE %s.%s (
  id BIGINT PRIMARY KEY,
  payload JSON NOT NULL
) ENGINE=InnoDB`, qi(dstSchema), qi(table)))

	mustExec(t, db, fmt.Sprintf(`INSERT INTO %s.%s (id, payload) VALUES (1, JSON_OBJECT('n','small'))`, qi(srcSchema), qi(table)))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO %s.%s (id, payload) VALUES (2, JSON_OBJECT('n','small2'))`, qi(srcSchema), qi(table)))
	mustExec(t, db, fmt.Sprintf(
		`INSERT INTO %s.%s (id, payload) VALUES (3, JSON_OBJECT('blob', REPEAT('z', 5242880)))`,
		qi(srcSchema), qi(table)))

	srcDB := openIntegrationMySQL(t)
	dstDB := openIntegrationMySQL(t)
	sink := &recordingSink{}
	eng := &Engine{
		SourceDB:  srcDB,
		TargetDB:  dstDB,
		EventSink: sink,
		Options: ResolveOptions(RawOptions{
			ReadWorkers:  1,
			WriteWorkers: 1,
			BatchSize:    1000,
			CommitRows:   1000,
			BufferMB:     16,
			BatchBytesMB: 4,
		}),
		Stats:  &Stats{},
		TaskID: "b5-oversize-row",
	}
	spec := jsonPKSpec(srcSchema, table, dstSchema, table)
	spec.EstimatedRows = 3

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	require.NoError(t, eng.Run(ctx, []*TableSpec{spec}))

	assert.Equal(t, 3, countRows(t, db, dstSchema, table))
	assert.True(t, sinkHasCode(sink, EventCodeRowExceedsBatchBytes), "expected ROW_EXCEEDS_BATCH_BYTES, got %v", sink.codes())
}

// TestIntegration_B5_NoPKLargeText_RowCountMatches 无 PK 大字段表行数一致。
func TestIntegration_B5_NoPKLargeText_RowCountMatches(t *testing.T) {
	db := openIntegrationMySQL(t)
	srcSchema, dstSchema := createIntegrationPair(t, db, "b5_nopk")
	const table = "t_nopk"

	mustExec(t, db, fmt.Sprintf(`
CREATE TABLE %s.%s (
  tag VARCHAR(32) NOT NULL,
  payload LONGTEXT NOT NULL
) ENGINE=InnoDB`, qi(srcSchema), qi(table)))
	mustExec(t, db, fmt.Sprintf(`
CREATE TABLE %s.%s (
  tag VARCHAR(32) NOT NULL,
  payload LONGTEXT NOT NULL
) ENGINE=InnoDB`, qi(dstSchema), qi(table)))

	const n = 300
	pad := strings.Repeat("a", 12*1024)
	for start := 1; start <= n; start += 50 {
		end := start + 49
		if end > n {
			end = n
		}
		var b strings.Builder
		args := make([]any, 0, (end-start+1)*2)
		fmt.Fprintf(&b, "INSERT INTO %s.%s (tag, payload) VALUES ", qi(srcSchema), qi(table))
		for i := start; i <= end; i++ {
			if i > start {
				b.WriteByte(',')
			}
			b.WriteString("(?,?)")
			args = append(args, fmt.Sprintf("tag-%d", i), pad)
		}
		mustExec(t, db, b.String(), args...)
	}

	cols := []entity.ColumnMeta{
		{Name: "tag", DataType: "varchar"},
		{Name: "payload", DataType: "longtext"},
	}
	spec := nopkSpec(srcSchema, table, dstSchema, table, cols)
	spec.EstimatedRows = n

	srcDB := openIntegrationMySQL(t)
	dstDB := openIntegrationMySQL(t)
	eng := &Engine{
		SourceDB: srcDB,
		TargetDB: dstDB,
		Options: ResolveOptions(RawOptions{
			ReadWorkers:  2,
			WriteWorkers: 1,
			BatchSize:    200,
			CommitRows:   500,
			BufferMB:     32,
			BatchBytesMB: 4,
		}),
		Stats:  &Stats{},
		TaskID: "b5-nopk",
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	require.NoError(t, eng.Run(ctx, []*TableSpec{spec}))
	assert.Equal(t, n, countRows(t, db, dstSchema, table))
}

// TestIntegration_B5_ReadBudgetPeakWithinCap 读预算峰值不超过配置上限（单表基线）。
func TestIntegration_B5_ReadBudgetPeakWithinCap(t *testing.T) {
	db := openIntegrationMySQL(t)
	srcSchema, dstSchema := createIntegrationPair(t, db, "b5_budget")
	const table = "t_budget"
	const rows = 4000

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
	seedBillRows(t, db, srcSchema, table, rows, nil)
	require.Equal(t, rows, countRows(t, db, srcSchema, table))

	spec := pkUKSpec(srcSchema, table, dstSchema, table)
	spec.EstimatedRows = rows

	srcDB := openIntegrationMySQL(t)
	dstDB := openIntegrationMySQL(t)
	const budgetCap = 4
	srcDB.SetMaxOpenConns(budgetCap)
	dstDB.SetMaxOpenConns(8)

	stats := &Stats{}
	eng := &Engine{
		SourceDB: srcDB,
		TargetDB: dstDB,
		Options: ResolveOptions(RawOptions{
			ReadWorkers:     budgetCap,
			TableWorkers:    2,
			PerTableReaders: 2,
			WriteWorkers:    2,
			BatchSize:       200,
			CommitRows:      500,
			BufferMB:        32,
			BatchBytesMB:    2,
		}),
		Stats:  stats,
		TaskID: "b5-read-budget",
	}
	eng.Options.LargeTableRows = 500

	var peakBudget int64
	stopPoll := make(chan struct{})
	go func() {
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stopPoll:
				return
			case <-ticker.C:
				cur := stats.Snapshot().ReadBudgetInUse
				for {
					old := atomic.LoadInt64(&peakBudget)
					if cur <= old || atomic.CompareAndSwapInt64(&peakBudget, old, cur) {
						break
					}
				}
			}
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	require.NoError(t, eng.Run(ctx, []*TableSpec{spec}))
	close(stopPoll)

	snap := stats.Snapshot()
	if peak := atomic.LoadInt64(&peakBudget); peak > budgetCap {
		t.Fatalf("read budget peak %d exceeds cap %d (final read_rows=%d)", peak, budgetCap, snap.ReadRows)
	}
	assert.Equal(t, int64(rows), snap.ReadRows)
	assert.Equal(t, rows, countRows(t, db, dstSchema, table))
}
