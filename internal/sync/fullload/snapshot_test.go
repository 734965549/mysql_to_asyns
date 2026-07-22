package fullload

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"

	"mysql-to-sync/internal/metadata/domain/entity"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/go-mysql-org/go-mysql/mysql"
)

func TestOpenTableSnapshot_RejectsNonInnoDB(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	mock.ExpectQuery("SELECT ENGINE FROM information_schema.TABLES").
		WithArgs("s", "t").
		WillReturnRows(sqlmock.NewRows([]string{"ENGINE"}).AddRow("MyISAM"))

	_, err = openTableSnapshot(context.Background(), db, "s", "t", "id", SnapshotOptions{})
	if err == nil {
		t.Fatal("expected non-InnoDB error")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestOpenTableSnapshot_CaptureHWM(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.MonitorPingsOption(false))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.MatchExpectationsInOrder(false)

	expectInnoDBTable(mock, "s", "t")
	expectLockWaitTimeout(mock)
	mock.ExpectExec("FLUSH TABLES `s`.`t` WITH READ LOCK").WillReturnResult(sqlmock.NewResult(0, 0))
	expectInnoDBTable(mock, "s", "t") // 持锁后权威校验
	expectConsistentSnapshot(mock, "s", "t", "c")
	mock.ExpectQuery("SHOW MASTER STATUS").
		WillReturnRows(sqlmock.NewRows([]string{"File", "Position", "Binlog_Do_DB", "Binlog_Ignore_DB", "Executed_Gtid_Set"}).
			AddRow("bin.001", uint32(1234), "", "", ""))
	mock.ExpectExec("UNLOCK TABLES").WillReturnResult(sqlmock.NewResult(0, 0))
	expectRestoreLockWaitTimeout(mock)
	expectSnapshotCommit(mock)

	var gotSchema, gotTable string
	var gotPos mysql.Position
	snap, err := openTableSnapshot(context.Background(), db, "s", "t", "c", SnapshotOptions{
		CaptureHWM: true,
		OnReady: func(schema, table string, pos mysql.Position) error {
			gotSchema, gotTable, gotPos = schema, table, pos
			return nil
		},
	})
	if err != nil {
		t.Fatalf("open snapshot: %v", err)
	}
	if gotSchema != "s" || gotTable != "t" || gotPos.Name != "bin.001" || gotPos.Pos != 1234 {
		t.Fatalf("callback got %s.%s %s:%d", gotSchema, gotTable, gotPos.Name, gotPos.Pos)
	}
	if err := snap.commit(context.Background()); err != nil {
		t.Fatalf("commit: %v", err)
	}
	snap.close()
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestOpenTableSnapshot_CaptureHWMAbortsOnLockFailure(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.MonitorPingsOption(false))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	expectInnoDBTable(mock, "s", "t")
	expectLockWaitTimeout(mock)
	mock.ExpectExec("FLUSH TABLES `s`.`t` WITH READ LOCK").
		WillReturnError(errors.New("lock denied"))
	expectRestoreLockWaitTimeout(mock)

	_, err = openTableSnapshot(context.Background(), db, "s", "t", "c", SnapshotOptions{CaptureHWM: true})
	if err == nil {
		t.Fatal("expected lock failure")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestOpenTableSnapshot_RollsBackWhenHoldMDLFails(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.MonitorPingsOption(false))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	expectInnoDBTable(mock, "s", "t")
	mock.ExpectExec("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ").
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("START TRANSACTION WITH CONSISTENT SNAPSHOT, READ ONLY").
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery("SELECT `c` FROM `s`.`t` LIMIT 1").
		WillReturnError(errors.New("table missing"))
	// START 已成功但固定 ReadView 失败时必须 ROLLBACK，避免脏事务回池。
	mock.ExpectExec("ROLLBACK").WillReturnResult(sqlmock.NewResult(0, 0))

	_, err = openTableSnapshot(context.Background(), db, "s", "t", "c", SnapshotOptions{})
	if err == nil {
		t.Fatal("expected hold MDL failure")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestOpenTableSnapshot_ReassertsInnoDBAfterMDL(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.MonitorPingsOption(false))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// 预检通过，但拿到 MDL 后发现引擎已变为 MyISAM → fail-closed。
	expectInnoDBTable(mock, "s", "t")
	expectConsistentSnapshot(mock, "s", "t", "c")
	expectInnoDBTableEngine(mock, "s", "t", "MyISAM")
	mock.ExpectExec("ROLLBACK").WillReturnResult(sqlmock.NewResult(0, 0))

	_, err = openTableSnapshot(context.Background(), db, "s", "t", "c", SnapshotOptions{})
	if err == nil {
		t.Fatal("expected post-MDL non-InnoDB error")
	}
	if !strings.Contains(err.Error(), "MyISAM") {
		t.Fatalf("error=%v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestOpenTableSnapshot_ReassertsInnoDBAfterLock(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.MonitorPingsOption(false))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	expectInnoDBTable(mock, "s", "t")
	expectLockWaitTimeout(mock)
	mock.ExpectExec("FLUSH TABLES `s`.`t` WITH READ LOCK").WillReturnResult(sqlmock.NewResult(0, 0))
	expectInnoDBTableEngine(mock, "s", "t", "MyISAM")
	// releaseAcquired 关连接前恢复 lock_wait_timeout；连接关闭会释放表锁，不发 UNLOCK。
	expectRestoreLockWaitTimeout(mock)

	_, err = openTableSnapshot(context.Background(), db, "s", "t", "c", SnapshotOptions{CaptureHWM: true})
	if err == nil {
		t.Fatal("expected post-lock non-InnoDB error")
	}
	if !strings.Contains(err.Error(), "MyISAM") {
		t.Fatalf("error=%v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestChunkReaderUsesSnapshotQueryer(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	spec := &TableSpec{
		SourceSchema: "s", SourceTable: "t", TargetSchema: "d", TargetTable: "u",
		Identity: &entity.TableIdentity{
			Strategy: entity.PKStrategy, IdentifyCols: []string{"id"}, CursorCols: []string{"id"},
			Columns: []entity.ColumnMeta{{Name: "id"}, {Name: "payload"}},
		},
	}
	chunk := &Chunk{ID: "c1", Spec: spec, Start: []any{int64(0)}, End: []any{int64(10)}}
	mock.ExpectQuery("SELECT `id`, `payload` FROM `s`.`t` WHERE").
		WithArgs(int64(0), int64(10), 10).
		WillReturnRows(sqlmock.NewRows([]string{"id", "payload"}).AddRow(int64(1), "x"))

	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	cr, err := newChunkReader(conn, chunk, 10, defaultBatchBytes)
	if err != nil {
		t.Fatal(err)
	}
	defer cr.close()
	batch, err := cr.nextBatch(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(batch.Rows) != 1 {
		t.Fatalf("rows=%d", len(batch.Rows))
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

// TestReadChunksThroughTableSnapshot_ExcludesPostSnapshotPKSwapVersion 是 §12.1 的单元级近似。
// 场景：全量进行中源库「删旧 PK + 新 PK 重插同一唯一键」；表级 RR 快照下各 chunk 只应看到快照行（旧 PK），
// 不得混入快照后新行版本，否则目标端会出现重复唯一键、阶段3 重建索引报 1062。
// sqlmock 无法模拟 InnoDB ReadView，本测试断言：打开 tableSnapshot 后多 chunk 均经 snap.conn 读取，
// 且返回集只含快照版本（id=1），不含快照后版本（id=2, 同 uk）。真实 MySQL 并发集成仍为已知缺口。
func TestReadChunksThroughTableSnapshot_ExcludesPostSnapshotPKSwapVersion(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.MonitorPingsOption(false))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	expectInnoDBTable(mock, "s", "t")
	expectConsistentSnapshot(mock, "s", "t", "id")
	expectInnoDBTable(mock, "s", "t") // 持 MDL 后权威校验

	// 两个 chunk 覆盖整表区间；快照视图只返回旧 PK 行。若误走连接池非快照读，
	// 第二个区间本可能读到换 PK 后的 id=2（同 uk=bill-key）——此处刻意不返回该行。
	const snapUK = "bill-key"
	mock.ExpectQuery("SELECT `id`, `uk`, `payload` FROM `s`.`t` WHERE").
		WithArgs(int64(0), int64(50), 100).
		WillReturnRows(sqlmock.NewRows([]string{"id", "uk", "payload"}).
			AddRow(int64(1), snapUK, "v-old"))
	mock.ExpectQuery("SELECT `id`, `uk`, `payload` FROM `s`.`t` WHERE").
		WithArgs(int64(50), int64(100), 100).
		WillReturnRows(sqlmock.NewRows([]string{"id", "uk", "payload"})) // 空：新 PK=2 不在快照内
	expectSnapshotCommit(mock)

	snap, err := openTableSnapshot(context.Background(), db, "s", "t", "id", SnapshotOptions{})
	if err != nil {
		t.Fatalf("open snapshot: %v", err)
	}
	defer snap.close()

	spec := &TableSpec{
		SourceSchema: "s", SourceTable: "t", TargetSchema: "d", TargetTable: "u",
		Identity: &entity.TableIdentity{
			Strategy: entity.PKStrategy, IdentifyCols: []string{"id"}, CursorCols: []string{"id"},
			Columns: []entity.ColumnMeta{{Name: "id"}, {Name: "uk"}, {Name: "payload"}},
		},
	}
	chunks := []*Chunk{
		{ID: "c0", Spec: spec, Start: []any{int64(0)}, End: []any{int64(50)}},
		{ID: "c1", Spec: spec, Start: []any{int64(50)}, End: []any{int64(100)}},
	}

	var got [][]any
	ukSeen := map[string]int64{}
	for _, chunk := range chunks {
		cr, err := newChunkReader(snap.conn, chunk, 100, defaultBatchBytes)
		if err != nil {
			t.Fatalf("newChunkReader %s: %v", chunk.ID, err)
		}
		for {
			batch, err := cr.nextBatch(context.Background())
			if err != nil {
				cr.close()
				t.Fatalf("nextBatch %s: %v", chunk.ID, err)
			}
			if batch == nil {
				break
			}
			for _, row := range batch.Rows {
				got = append(got, row)
				uk, _ := row[1].(string)
				pk, _ := row[0].(int64)
				if prev, ok := ukSeen[uk]; ok && prev != pk {
					t.Fatalf("duplicate unique key %q from PK %d and %d (post-snapshot mix-in)", uk, prev, pk)
				}
				ukSeen[uk] = pk
			}
		}
		cr.close()
	}

	if len(got) != 1 {
		t.Fatalf("snapshot rows=%d want 1 (old PK only)", len(got))
	}
	if pk, _ := got[0][0].(int64); pk != 1 {
		t.Fatalf("got PK=%v want snapshot PK=1 (must not include post-swap PK=2)", got[0][0])
	}
	if uk, _ := got[0][1].(string); uk != snapUK {
		t.Fatalf("uk=%q want %q", uk, snapUK)
	}
	if _, hasNew := ukSeen[snapUK]; !hasNew {
		t.Fatal("expected snapshot unique key present")
	}
	// 显式排除快照后版本：若结果里出现 id=2 即失败（上文 len/pk 已覆盖，再断言一遍语义）。
	for _, row := range got {
		if pk, _ := row[0].(int64); pk == 2 {
			t.Fatal("must not mix post-snapshot PK=2 version into table snapshot read")
		}
	}

	if err := snap.commit(context.Background()); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestPlanIntegerRange_UsesPlanQueryer(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	spec := &TableSpec{
		SourceSchema: "s", SourceTable: "t", EstimatedRows: 100,
		Identity: &entity.TableIdentity{
			Strategy: entity.PKStrategy, IdentifyCols: []string{"id"}, CursorCols: []string{"id"},
			Columns: []entity.ColumnMeta{{Name: "id", DataType: "bigint"}},
		},
	}
	mock.ExpectQuery("SELECT MIN\\(`id`\\), MAX\\(`id`\\) FROM `s`.`t`").
		WillReturnRows(sqlmock.NewRows([]string{"min", "max"}).AddRow(int64(1), int64(100)))

	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	chunks, err := NewPlanner(conn).planTable(context.Background(), spec, 4)
	if err != nil {
		t.Fatal(err)
	}
	if len(chunks) != 4 {
		t.Fatalf("chunks=%d want 4", len(chunks))
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestGroupChunksByTable(t *testing.T) {
	specA := &TableSpec{SourceSchema: "s", SourceTable: "a"}
	specB := &TableSpec{SourceSchema: "s", SourceTable: "b"}
	grouped := groupChunksByTable([]*Chunk{
		{ID: "a#0", Spec: specA},
		{ID: "a#1", Spec: specA},
		{ID: "b#0", Spec: specB},
	})
	if len(grouped["s.a"]) != 2 || len(grouped["s.b"]) != 1 {
		t.Fatalf("unexpected grouping: %+v", grouped)
	}
}

func TestCommitSnapshots_IgnoresCanceledParentContext(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.MonitorPingsOption(false))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	expectInnoDBTable(mock, "s", "t")
	expectConsistentSnapshot(mock, "s", "t", "id")
	expectInnoDBTable(mock, "s", "t") // 无锁路径：持 MDL 后权威校验
	expectSnapshotCommit(mock)

	snap, err := openTableSnapshot(context.Background(), db, "s", "t", "id", SnapshotOptions{})
	if err != nil {
		t.Fatalf("open snapshot: %v", err)
	}
	defer snap.close()

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := commitSnapshots(canceled, []*tableSnapshot{snap}); err != nil {
		t.Fatalf("commit with canceled parent ctx: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestOpenAlignedTableSnapshots_MultiReaders(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.MonitorPingsOption(false))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.MatchExpectationsInOrder(false)

	expectInnoDBTable(mock, "s", "t")
	expectLockWaitTimeout(mock)
	mock.ExpectExec("FLUSH TABLES `s`.`t` WITH READ LOCK").WillReturnResult(sqlmock.NewResult(0, 0))
	expectInnoDBTable(mock, "s", "t") // 持锁后权威校验
	expectConsistentSnapshot(mock, "s", "t", "id")
	expectConsistentSnapshot(mock, "s", "t", "id")
	mock.ExpectExec("UNLOCK TABLES").WillReturnResult(sqlmock.NewResult(0, 0))
	expectRestoreLockWaitTimeout(mock)
	expectSnapshotCommit(mock)
	expectSnapshotCommit(mock)

	snaps, err := openAlignedTableSnapshots(context.Background(), db, "s", "t", "id", 2, SnapshotOptions{
		LockWaitTimeoutSec: 5,
	})
	if err != nil {
		t.Fatalf("open aligned: %v", err)
	}
	if len(snaps) != 2 {
		t.Fatalf("snaps=%d", len(snaps))
	}
	if err := commitSnapshots(context.Background(), snaps); err != nil {
		t.Fatal(err)
	}
	closeSnapshots(snaps)
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestDecideTableReaders(t *testing.T) {
	opt := ResolveOptions(RawOptions{ReadWorkers: 4})
	job := &tableReadJob{
		spec: &TableSpec{
			EstimatedRows: 50,
			Identity:      &entity.TableIdentity{Strategy: entity.PKStrategy, Columns: []entity.ColumnMeta{{Name: "id"}}},
		},
		chunks: []*Chunk{{ID: "1"}, {ID: "2"}, {ID: "3"}},
	}
	if got := decideTableReaders(job, opt); got != 1 {
		t.Fatalf("small table readers=%d want 1", got)
	}
	job.spec.EstimatedRows = defaultLargeTableRows
	if got := decideTableReaders(job, opt); got != 3 {
		t.Fatalf("large table readers=%d want 3 (min chunks)", got)
	}
	if got := decideTableReadersForSpec(job.spec, opt); got != 4 {
		t.Fatalf("large table pre-plan readers=%d want 4", got)
	}
	job.chunks = []*Chunk{{ID: "only"}}
	if got := decideTableReaders(job, opt); got != 1 {
		t.Fatalf("single chunk readers=%d want 1", got)
	}
}

func TestOpenTableSnapshotsWithLimiter_DegradeOnAlignLockFail(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.MonitorPingsOption(false))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.MatchExpectationsInOrder(false)

	// 首次多连接：取锁失败。
	expectInnoDBTable(mock, "s", "t")
	expectLockWaitTimeout(mock)
	mock.ExpectExec("FLUSH TABLES `s`.`t` WITH READ LOCK").WillReturnError(errors.New("timeout"))
	expectRestoreLockWaitTimeout(mock)

	// 降级单连接：无锁。
	expectInnoDBTable(mock, "s", "t")
	expectConsistentSnapshot(mock, "s", "t", "id")
	expectInnoDBTable(mock, "s", "t") // 持 MDL 后权威校验
	expectSnapshotCommit(mock)

	lim := newSnapshotLimiter(2, 8)
	stats := &Stats{}
	opt := ResolveOptions(RawOptions{ReadWorkers: 2})
	opt.DegradeOnAlignLockFail = true

	snaps, reserved, err := openTableSnapshotsWithLimiter(
		context.Background(), db, "s", "t", "id", 2,
		SnapshotOptions{LockWaitTimeoutSec: 5},
		lim, opt, stats,
	)
	if err != nil {
		t.Fatalf("degrade open: %v", err)
	}
	if len(snaps) != 1 || reserved != 1 {
		t.Fatalf("snaps=%d reserved=%d", len(snaps), reserved)
	}
	if atomic.LoadInt64(&stats.SnapshotAlignDegrades) != 1 {
		t.Fatalf("degrades=%d", stats.SnapshotAlignDegrades)
	}
	if err := commitSnapshots(context.Background(), snaps); err != nil {
		t.Fatal(err)
	}
	closeSnapshots(snaps)
	lim.releaseConns(reserved)
	atomic.AddInt64(&stats.ActiveSnapshotTxns, -1)
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestSetLockWaitTimeout_RestoresPreviousValue(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.MonitorPingsOption(false))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	mock.ExpectQuery("SELECT @@SESSION.lock_wait_timeout").
		WillReturnRows(sqlmock.NewRows([]string{"lock_wait_timeout"}).AddRow(42))
	mock.ExpectExec("SET SESSION lock_wait_timeout = 10").
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("SET SESSION lock_wait_timeout = 42").
		WillReturnResult(sqlmock.NewResult(0, 0))

	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	restore, err := setLockWaitTimeout(context.Background(), conn, 10)
	if err != nil {
		t.Fatalf("setLockWaitTimeout: %v", err)
	}
	if restore == nil {
		t.Fatal("expected restore func")
	}
	restore()
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
