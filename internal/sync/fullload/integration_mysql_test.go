//go:build integration

package fullload

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"mysql-to-sync/internal/metadata/domain/entity"

	_ "github.com/go-sql-driver/mysql"
)

// 真实 MySQL 并发集成（计划 §12.1）。
//
// 运行前启动仓库 docker-compose 中的 mysql-source（或任意 InnoDB MySQL），并设置：
//
//	TEST_MYSQL_DSN="root:root_password@tcp(127.0.0.1:3306)/?parseTime=true&multiStatements=true"
//
// 然后：
//
//	go test -tags=integration -count=1 -timeout=5m ./internal/sync/fullload/ -run TestIntegration
//
// 未设置 DSN 时跳过；默认 `go test ./...`（无 -tags=integration）不会编译本文件。

func TestIntegration_SnapshotExcludesPostSnapshotPKSwap(t *testing.T) {
	db := openIntegrationMySQL(t)
	srcSchema, _ := createIntegrationPair(t, db, "pk_swap")
	const table = "bill"
	mustExec(t, db, fmt.Sprintf(`
CREATE TABLE %s.%s (
  id BIGINT PRIMARY KEY,
  uk VARCHAR(64) NOT NULL,
  payload VARCHAR(64) NOT NULL,
  UNIQUE KEY uniq_uk (uk)
) ENGINE=InnoDB`, qi(srcSchema), qi(table)))

	const snapUK = "bill-key"
	seedBillRows(t, db, srcSchema, table, 200, map[int64][2]string{
		90: {snapUK, "v-old"},
	})

	ctx := context.Background()
	snap, err := openTableSnapshot(ctx, db, srcSchema, table, "id", SnapshotOptions{})
	if err != nil {
		t.Fatalf("open snapshot: %v", err)
	}
	defer snap.close()

	// 快照已建立后：删旧 PK + 新 PK 重插同一唯一键（复现 1062 根因场景）。
	mustExec(t, db, fmt.Sprintf("DELETE FROM %s.%s WHERE id=?", qi(srcSchema), qi(table)), int64(90))
	mustExec(t, db, fmt.Sprintf(
		"INSERT INTO %s.%s (id, uk, payload) VALUES (?, ?, ?)", qi(srcSchema), qi(table)),
		int64(250), snapUK, "v-new")

	spec := pkUKSpec(srcSchema, table, srcSchema, table)
	chunks, err := NewPlanner(snap.conn).planTable(ctx, spec, 8)
	if err != nil {
		t.Fatalf("plan: %v", err)
	}
	if len(chunks) < 2 {
		t.Fatalf("need >=2 chunks for boundary coverage, got %d", len(chunks))
	}

	rows := readAllChunks(t, ctx, snap.conn, chunks, 50)
	assertNoDuplicateUK(t, rows, 1)
	assertHasPKPayload(t, rows, 90, "v-old")
	assertMissingPK(t, rows, 250)

	if err := snap.commit(ctx); err != nil {
		t.Fatalf("commit snapshot: %v", err)
	}
}

func TestIntegration_SnapshotExcludesUniqueColumnRewrite(t *testing.T) {
	db := openIntegrationMySQL(t)
	srcSchema, _ := createIntegrationPair(t, db, "uk_rewrite")
	const table = "bill"
	mustExec(t, db, fmt.Sprintf(`
CREATE TABLE %s.%s (
  id BIGINT PRIMARY KEY,
  uk VARCHAR(64) NOT NULL,
  payload VARCHAR(64) NOT NULL,
  UNIQUE KEY uniq_uk (uk)
) ENGINE=InnoDB`, qi(srcSchema), qi(table)))

	mustExec(t, db, fmt.Sprintf(
		"INSERT INTO %s.%s (id, uk, payload) VALUES (1,'A','old-A'),(2,'B','old-B'),(3,'C','old-C')",
		qi(srcSchema), qi(table)))

	ctx := context.Background()
	snap, err := openTableSnapshot(ctx, db, srcSchema, table, "id", SnapshotOptions{})
	if err != nil {
		t.Fatalf("open snapshot: %v", err)
	}
	defer snap.close()

	// 快照后：把 id=1 的唯一列改成 id=2 已有值（业务上需先改/删冲突行；此处先改 2 再改 1）。
	mustExec(t, db, fmt.Sprintf("UPDATE %s.%s SET uk='B-moved' WHERE id=2", qi(srcSchema), qi(table)))
	mustExec(t, db, fmt.Sprintf("UPDATE %s.%s SET uk='B', payload='new-A-as-B' WHERE id=1", qi(srcSchema), qi(table)))

	spec := pkUKSpec(srcSchema, table, srcSchema, table)
	chunks, err := NewPlanner(snap.conn).planTable(ctx, spec, 4)
	if err != nil {
		t.Fatalf("plan: %v", err)
	}
	rows := readAllChunks(t, ctx, snap.conn, chunks, 10)
	assertNoDuplicateUK(t, rows, 1)
	ukByPK := map[int64]string{}
	for _, r := range rows {
		ukByPK[asInt64(t, r[0])] = asString(t, r[1])
	}
	if ukByPK[1] != "A" || ukByPK[2] != "B" {
		t.Fatalf("snapshot uk map=%v want id1=A id2=B", ukByPK)
	}
	if err := snap.commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}
}

func TestIntegration_SnapshotBoundaryCrossingPKMove(t *testing.T) {
	db := openIntegrationMySQL(t)
	srcSchema, _ := createIntegrationPair(t, db, "boundary")
	const table = "bill"
	mustExec(t, db, fmt.Sprintf(`
CREATE TABLE %s.%s (
  id BIGINT PRIMARY KEY,
  uk VARCHAR(64) NOT NULL,
  payload VARCHAR(64) NOT NULL,
  UNIQUE KEY uniq_uk (uk)
) ENGINE=InnoDB`, qi(srcSchema), qi(table)))

	const moveUK = "uk-90"
	seedBillRows(t, db, srcSchema, table, 200, map[int64][2]string{
		90: {moveUK, "at-low"},
	})

	ctx := context.Background()
	snap, err := openTableSnapshot(ctx, db, srcSchema, table, "id", SnapshotOptions{})
	if err != nil {
		t.Fatalf("open snapshot: %v", err)
	}
	defer snap.close()

	spec := pkUKSpec(srcSchema, table, srcSchema, table)
	chunks, err := NewPlanner(snap.conn).planTable(ctx, spec, 8)
	if err != nil {
		t.Fatalf("plan: %v", err)
	}
	if len(chunks) < 2 {
		t.Fatalf("need >=2 chunks, got %d", len(chunks))
	}

	// 跨边界：从低区间 PK=90 挪到高区间 PK=150（同唯一键）。
	mustExec(t, db, fmt.Sprintf("DELETE FROM %s.%s WHERE id=?", qi(srcSchema), qi(table)), int64(90))
	mustExec(t, db, fmt.Sprintf("DELETE FROM %s.%s WHERE id=?", qi(srcSchema), qi(table)), int64(150))
	mustExec(t, db, fmt.Sprintf(
		"INSERT INTO %s.%s (id, uk, payload) VALUES (?, ?, ?)", qi(srcSchema), qi(table)),
		int64(150), moveUK, "at-high")

	rows := readAllChunks(t, ctx, snap.conn, chunks, 50)
	assertNoDuplicateUK(t, rows, 1)
	assertHasPKPayload(t, rows, 90, "at-low")
	// 快照中 150 仍是原 uk-150，不是挪过来的新版本。
	found150 := false
	for _, r := range rows {
		if asInt64(t, r[0]) == 150 {
			found150 = true
			if asString(t, r[1]) == moveUK {
				t.Fatal("boundary move leaked into high chunk: snapshot must keep old PK=90 only")
			}
		}
	}
	if !found150 {
		t.Fatal("expected original PK=150 still visible in snapshot")
	}
	if err := snap.commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}
}

// TestIntegration_EngineConcurrentPKSwap_NoDuplicateUKOnIndexRestore
// 端到端：Engine.Run 灌数期间（首个 OnCommit 后）注入「换 PK 同唯一键」，
// 目标表无唯一索引写入，结束后 ADD UNIQUE 必须成功（无 1062）。
func TestIntegration_EngineConcurrentPKSwap_NoDuplicateUKOnIndexRestore(t *testing.T) {
	db := openIntegrationMySQL(t)
	srcSchema, dstSchema := createIntegrationPair(t, db, "engine_pk_swap")
	const table = "bill"

	mustExec(t, db, fmt.Sprintf(`
CREATE TABLE %s.%s (
  id BIGINT PRIMARY KEY,
  uk VARCHAR(64) NOT NULL,
  payload VARCHAR(64) NOT NULL,
  UNIQUE KEY uniq_uk (uk)
) ENGINE=InnoDB`, qi(srcSchema), qi(table)))
	// 目标侧模拟 OptimizeIndex：灌数前无唯一索引，仅保留主键。
	mustExec(t, db, fmt.Sprintf(`
CREATE TABLE %s.%s (
  id BIGINT PRIMARY KEY,
  uk VARCHAR(64) NOT NULL,
  payload VARCHAR(64) NOT NULL
) ENGINE=InnoDB`, qi(dstSchema), qi(table)))

	const snapUK = "bill-key"
	const oldPK, newPK int64 = 90, 5000
	n := 2000
	seedBillRows(t, db, srcSchema, table, n, map[int64][2]string{
		oldPK: {snapUK, "v-old"},
	})

	srcDB := openIntegrationMySQL(t) // 独立池，避免与 Engine 争用同一 *sql.DB 状态
	dstDB := openIntegrationMySQL(t)
	srcDB.SetMaxOpenConns(16)
	dstDB.SetMaxOpenConns(8)

	var mutateOnce sync.Once
	mutated := make(chan struct{})
	mutateErr := make(chan error, 1)
	eng := &Engine{
		SourceDB: srcDB,
		TargetDB: dstDB,
		Options: ResolveOptions(RawOptions{
			ReadWorkers:  4,
			WriteWorkers: 2,
			BatchSize:    100,
			CommitRows:   200,
			BufferMB:     16,
			BatchBytesMB: 1,
		}),
		Stats:  &Stats{},
		TaskID: "integration-pk-swap",
		OnCommit: func(schema, tableName string, rows, bytes int64) {
			mutateOnce.Do(func() {
				// 屏障：已有批次提交 ⇒ 表级快照已建立且读已开始。
				// 注意：OnCommit 在 writer goroutine 中调用，禁止 t.Fatalf。
				_, err := db.Exec(fmt.Sprintf("DELETE FROM %s.%s WHERE id=?", qi(srcSchema), qi(table)), oldPK)
				if err == nil {
					_, err = db.Exec(fmt.Sprintf(
						"INSERT INTO %s.%s (id, uk, payload) VALUES (?, ?, ?)", qi(srcSchema), qi(table)),
						newPK, snapUK, "v-new")
				}
				if err != nil {
					mutateErr <- err
				}
				close(mutated)
			})
		},
	}
	// 压低大表阈值，强制走多连接对齐快照并行读（P1 路径）。
	eng.Options.LargeTableRows = 1000
	eng.Options.TableParallelReaders = 4
	eng.Options.ChunkOvershoot = 8
	eng.Options.MaxSnapshotGroups = 2
	eng.Options.MaxSnapshotConns = 16
	eng.Options.LockWaitTimeoutSec = 10

	spec := pkUKSpec(srcSchema, table, dstSchema, table)
	spec.EstimatedRows = int64(n)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	if err := eng.Run(ctx, []*TableSpec{spec}); err != nil {
		t.Fatalf("Engine.Run: %v", err)
	}

	select {
	case <-mutated:
	default:
		t.Fatal("expected concurrent PK swap to run after first OnCommit")
	}
	select {
	case err := <-mutateErr:
		t.Fatalf("concurrent PK swap: %v", err)
	default:
	}

	var dstCount int
	if err := db.QueryRow(fmt.Sprintf(
		"SELECT COUNT(*) FROM %s.%s", qi(dstSchema), qi(table))).Scan(&dstCount); err != nil {
		t.Fatalf("count dst: %v", err)
	}
	if dstCount != n {
		t.Fatalf("dst rows=%d want %d (snapshot row count)", dstCount, n)
	}

	var dupUK int
	if err := db.QueryRow(fmt.Sprintf(`
SELECT COUNT(*) FROM (
  SELECT uk FROM %s.%s GROUP BY uk HAVING COUNT(*) > 1
) d`, qi(dstSchema), qi(table))).Scan(&dupUK); err != nil {
		t.Fatalf("dup uk query: %v", err)
	}
	if dupUK != 0 {
		t.Fatalf("target has %d duplicate unique-key groups", dupUK)
	}

	var hasNewPK int
	if err := db.QueryRow(fmt.Sprintf(
		"SELECT COUNT(*) FROM %s.%s WHERE id=?", qi(dstSchema), qi(table)), newPK).Scan(&hasNewPK); err != nil {
		t.Fatalf("new pk probe: %v", err)
	}
	if hasNewPK != 0 {
		t.Fatalf("post-snapshot PK=%d must not appear on target", newPK)
	}

	var hasOldPK int
	if err := db.QueryRow(fmt.Sprintf(
		"SELECT COUNT(*) FROM %s.%s WHERE id=?", qi(dstSchema), qi(table)), oldPK).Scan(&hasOldPK); err != nil {
		t.Fatalf("old pk probe: %v", err)
	}
	if hasOldPK != 1 {
		t.Fatalf("snapshot PK=%d missing on target", oldPK)
	}

	// 阶段3：重建唯一索引，不得 1062。
	if _, err := db.Exec(fmt.Sprintf(
		"ALTER TABLE %s.%s ADD UNIQUE KEY uniq_uk (uk)", qi(dstSchema), qi(table))); err != nil {
		t.Fatalf("restore unique index (expect no 1062): %v", err)
	}
}

func openIntegrationMySQL(t *testing.T) *sql.DB {
	t.Helper()
	dsn := strings.TrimSpace(os.Getenv("TEST_MYSQL_DSN"))
	if dsn == "" {
		t.Skip("TEST_MYSQL_DSN unset; skip real MySQL integration (see plans/consistent-snapshot-full-load-plan.md §12.1)")
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open mysql: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("ping mysql (%s): %v", redactDSN(dsn), err)
	}
	return db
}

func createIntegrationPair(t *testing.T, db *sql.DB, suffix string) (srcSchema, dstSchema string) {
	t.Helper()
	id := fmt.Sprintf("%d", time.Now().UnixNano())
	srcSchema = fmt.Sprintf("fl_it_src_%s_%s", suffix, id)
	dstSchema = fmt.Sprintf("fl_it_dst_%s_%s", suffix, id)
	mustExec(t, db, "CREATE DATABASE "+qi(srcSchema))
	mustExec(t, db, "CREATE DATABASE "+qi(dstSchema))
	t.Cleanup(func() {
		_, _ = db.Exec("DROP DATABASE IF EXISTS " + qi(srcSchema))
		_, _ = db.Exec("DROP DATABASE IF EXISTS " + qi(dstSchema))
	})
	return srcSchema, dstSchema
}

func pkUKSpec(srcSchema, srcTable, dstSchema, dstTable string) *TableSpec {
	return &TableSpec{
		SourceSchema: srcSchema, SourceTable: srcTable,
		TargetSchema: dstSchema, TargetTable: dstTable,
		Identity: &entity.TableIdentity{
			TableName:    srcTable,
			Strategy:     entity.PKStrategy,
			IdentifyCols: []string{"id"},
			CursorCols:   []string{"id"},
			HasPK:        true,
			HasUK:        true,
			Columns: []entity.ColumnMeta{
				{Name: "id", DataType: "bigint", IsPrimaryKey: true},
				{Name: "uk", DataType: "varchar", IsUnique: true},
				{Name: "payload", DataType: "varchar"},
			},
		},
	}
}

func readAllChunks(t *testing.T, ctx context.Context, q snapshotQueryer, chunks []*Chunk, batchRows int) [][]any {
	t.Helper()
	var out [][]any
	for _, chunk := range chunks {
		cr, err := newChunkReader(q, chunk, batchRows, defaultBatchBytes, Options{}, 1, nil)
		if err != nil {
			t.Fatalf("newChunkReader %s: %v", chunk.ID, err)
		}
		for {
			batch, err := cr.nextBatch(ctx)
			if err != nil {
				cr.close()
				t.Fatalf("nextBatch %s: %v", chunk.ID, err)
			}
			if batch == nil {
				break
			}
			out = append(out, batch.Rows...)
		}
		cr.close()
	}
	return out
}

func assertNoDuplicateUK(t *testing.T, rows [][]any, ukIdx int) {
	t.Helper()
	seen := map[string]int64{}
	for _, r := range rows {
		uk := asString(t, r[ukIdx])
		pk := asInt64(t, r[0])
		if prev, ok := seen[uk]; ok && prev != pk {
			t.Fatalf("duplicate unique key %q from PK %d and %d", uk, prev, pk)
		}
		seen[uk] = pk
	}
}

func assertHasPKPayload(t *testing.T, rows [][]any, pk int64, payload string) {
	t.Helper()
	for _, r := range rows {
		if asInt64(t, r[0]) == pk {
			if asString(t, r[2]) != payload {
				t.Fatalf("PK=%d payload=%q want %q", pk, asString(t, r[2]), payload)
			}
			return
		}
	}
	t.Fatalf("missing snapshot PK=%d", pk)
}

func assertMissingPK(t *testing.T, rows [][]any, pk int64) {
	t.Helper()
	for _, r := range rows {
		if asInt64(t, r[0]) == pk {
			t.Fatalf("post-snapshot PK=%d must not appear", pk)
		}
	}
}

func mustExec(t *testing.T, db *sql.DB, query string, args ...any) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := db.ExecContext(ctx, query, args...); err != nil {
		t.Fatalf("exec %q: %v", query, err)
	}
}

// seedBillRows 批量灌入 id=1..n；overrides[pk]=(uk,payload) 覆盖默认 uk-{id}/p-{id}。
func seedBillRows(t *testing.T, db *sql.DB, schema, table string, n int, overrides map[int64][2]string) {
	t.Helper()
	const batch = 200
	for start := 1; start <= n; start += batch {
		end := start + batch - 1
		if end > n {
			end = n
		}
		var b strings.Builder
		args := make([]any, 0, (end-start+1)*3)
		fmt.Fprintf(&b, "INSERT INTO %s.%s (id, uk, payload) VALUES ", qi(schema), qi(table))
		for i := start; i <= end; i++ {
			if i > start {
				b.WriteByte(',')
			}
			b.WriteString("(?,?,?)")
			uk := fmt.Sprintf("uk-%d", i)
			payload := fmt.Sprintf("p-%d", i)
			if ov, ok := overrides[int64(i)]; ok {
				uk, payload = ov[0], ov[1]
			}
			args = append(args, int64(i), uk, payload)
		}
		mustExec(t, db, b.String(), args...)
	}
}

func qi(name string) string {
	return "`" + strings.ReplaceAll(name, "`", "``") + "`"
}

func asInt64(t *testing.T, v any) int64 {
	t.Helper()
	switch x := v.(type) {
	case int64:
		return x
	case int32:
		return int64(x)
	case int:
		return int64(x)
	case []byte:
		var n int64
		if _, err := fmt.Sscan(string(x), &n); err == nil {
			return n
		}
	case string:
		var n int64
		if _, err := fmt.Sscan(x, &n); err == nil {
			return n
		}
	}
	t.Fatalf("cannot coerce %T (%v) to int64", v, v)
	return 0
}

func asString(t *testing.T, v any) string {
	t.Helper()
	switch x := v.(type) {
	case string:
		return x
	case []byte:
		return string(x)
	case nil:
		return ""
	default:
		return fmt.Sprint(x)
	}
}

func redactDSN(dsn string) string {
	if i := strings.Index(dsn, "@"); i > 0 {
		return "***@" + dsn[i+1:]
	}
	return dsn
}
