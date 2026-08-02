//go:build integration

package fullload

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"mysql-to-sync/internal/metadata/domain/entity"

	_ "github.com/go-sql-driver/mysql"
)

// 真实 MySQL 集成测试辅助（B5 / fault_injection）。
//
// 运行前启动 docker-compose mysql-source 或任意 InnoDB MySQL，并设置：
//
//	TEST_MYSQL_DSN="root:root_password@tcp(127.0.0.1:3306)/?parseTime=true&multiStatements=true"
//
//	go test -tags=integration -count=1 -timeout=10m -v ./internal/sync/fullload/ -run TestIntegration
//
// 未设置 DSN 时跳过；默认 `go test ./...` 不编译本文件。

func openIntegrationMySQL(t *testing.T) *sql.DB {
	t.Helper()
	dsn := strings.TrimSpace(os.Getenv("TEST_MYSQL_DSN"))
	if dsn == "" {
		t.Skip("TEST_MYSQL_DSN unset; skip real MySQL integration (see docs/testing/UNIT_TEST.md)")
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

func jsonPKSpec(srcSchema, srcTable, dstSchema, dstTable string) *TableSpec {
	return &TableSpec{
		SourceSchema: srcSchema, SourceTable: srcTable,
		TargetSchema: dstSchema, TargetTable: dstTable,
		Identity: &entity.TableIdentity{
			TableName:    srcTable,
			Strategy:     entity.PKStrategy,
			IdentifyCols: []string{"id"},
			CursorCols:   []string{"id"},
			HasPK:        true,
			Columns: []entity.ColumnMeta{
				{Name: "id", DataType: "bigint", IsPrimaryKey: true},
				{Name: "payload", DataType: "json"},
			},
		},
	}
}

func nopkSpec(srcSchema, srcTable, dstSchema, dstTable string, cols []entity.ColumnMeta) *TableSpec {
	identify := make([]string, len(cols))
	for i, c := range cols {
		identify[i] = c.Name
	}
	return &TableSpec{
		SourceSchema: srcSchema, SourceTable: srcTable,
		TargetSchema: dstSchema, TargetTable: dstTable,
		Identity: &entity.TableIdentity{
			TableName:    srcTable,
			Strategy:     entity.FullColumnsStrategy,
			IdentifyCols: identify,
			Columns:      cols,
		},
	}
}

func mustExec(t *testing.T, db *sql.DB, query string, args ...any) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
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

func seedJSONRows(t *testing.T, db *sql.DB, schema, table string, n int, jsonLen int) {
	t.Helper()
	const batch = 50
	pad := strings.Repeat("x", jsonLen)
	for start := 1; start <= n; start += batch {
		end := start + batch - 1
		if end > n {
			end = n
		}
		var b strings.Builder
		args := make([]any, 0, (end-start+1)*2)
		fmt.Fprintf(&b, "INSERT INTO %s.%s (id, payload) VALUES ", qi(schema), qi(table))
		for i := start; i <= end; i++ {
			if i > start {
				b.WriteByte(',')
			}
			b.WriteString("(?,JSON_OBJECT('k', ?, 'v', ?))")
			args = append(args, int64(i), fmt.Sprintf("k%d", i), pad)
		}
		mustExec(t, db, b.String(), args...)
	}
}

func countRows(t *testing.T, db *sql.DB, schema, table string) int {
	t.Helper()
	var n int
	q := fmt.Sprintf("SELECT COUNT(*) FROM %s.%s", qi(schema), qi(table))
	if err := db.QueryRow(q).Scan(&n); err != nil {
		t.Fatalf("count %s.%s: %v", schema, table, err)
	}
	return n
}

func qi(name string) string {
	return "`" + strings.ReplaceAll(name, "`", "``") + "`"
}

func redactDSN(dsn string) string {
	if i := strings.Index(dsn, "@"); i > 0 {
		return "***@" + dsn[i+1:]
	}
	return dsn
}

func sinkHasCode(sink *recordingSink, code string) bool {
	for _, c := range sink.codes() {
		if c == code {
			return true
		}
	}
	return false
}
