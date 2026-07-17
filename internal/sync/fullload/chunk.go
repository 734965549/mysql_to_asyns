package fullload

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"mysql-to-sync/internal/metadata/domain/entity"
)

// TableSpec 描述一张待全量同步的表（源与目标已就绪，仅差数据复制）。
type TableSpec struct {
	SourceSchema string
	TargetSchema string
	SourceTable  string
	TargetTable  string
	Identity     *entity.TableIdentity
	EstimatedRows int64
}

// Chunk 是任务级共享队列中的一个读取工作单元。
//
// 单列游标：Start 为排他下界（nil=表头），End 为含边界上界（nil=无上界）。
// Sequential=true 时整表顺序读取（复合主键或无主键表），不做值域切分。
type Chunk struct {
	ID         string
	Spec       *TableSpec
	Start      []any
	End        []any
	Sequential bool // 复合主键：整表 keyset 顺序读取
	NoPK       bool // 无主键：整表流式读取
}

// Planner 负责为每张表生成 chunk。
type Planner struct {
	sourceDB *sql.DB
}

// NewPlanner 创建 chunk 规划器。
func NewPlanner(sourceDB *sql.DB) *Planner {
	return &Planner{sourceDB: sourceDB}
}

// Plan 为一批表生成全部 chunk。targetChunks 是期望的总 chunk 数（读取 worker × overshoot）。
func (p *Planner) Plan(ctx context.Context, specs []*TableSpec, targetChunks int) ([]*Chunk, error) {
	if targetChunks < 1 {
		targetChunks = 1
	}
	var chunks []*Chunk
	for _, spec := range specs {
		tc, err := p.planTable(ctx, spec, targetChunks)
		if err != nil {
			return nil, fmt.Errorf("plan chunks for %s.%s: %w", spec.SourceSchema, spec.SourceTable, err)
		}
		chunks = append(chunks, tc...)
	}
	return chunks, nil
}

func (p *Planner) planTable(ctx context.Context, spec *TableSpec, targetChunks int) ([]*Chunk, error) {
	id := spec.Identity

	// 无主键表：单个流式 chunk。
	if id == nil || id.Strategy == entity.FullColumnsStrategy {
		return []*Chunk{{ID: chunkID(spec, 0), Spec: spec, NoPK: true, Sequential: true}}, nil
	}

	cursorCols := id.EffectiveCursorCols()

	// 复合游标：整表 keyset 顺序读取（保证正确性，暂不并行切分）。
	if len(cursorCols) != 1 {
		return []*Chunk{{ID: chunkID(spec, 0), Spec: spec, Sequential: true}}, nil
	}

	col := cursorCols[0]

	// 单列自增/数值主键：值域切分 + 过量分片消除长尾。
	if isIntegerColumn(id, col) {
		return p.planIntegerRange(ctx, spec, col, targetChunks)
	}

	// 单列字符串/其他类型：PK-only keyset 扫描生成近似等行数边界。
	return p.planKeysetBoundaries(ctx, spec, col, targetChunks)
}

// planIntegerRange 对连续/稀疏整数主键做值域等分；过量分片配合工作窃取降低倾斜影响。
func (p *Planner) planIntegerRange(ctx context.Context, spec *TableSpec, col string, targetChunks int) ([]*Chunk, error) {
	var minV, maxV sql.NullInt64
	q := fmt.Sprintf("SELECT MIN(`%s`), MAX(`%s`) FROM `%s`.`%s`", col, col, spec.SourceSchema, spec.SourceTable)
	if err := p.sourceDB.QueryRowContext(ctx, q).Scan(&minV, &maxV); err != nil {
		return nil, err
	}
	if !minV.Valid || !maxV.Valid || maxV.Int64 < minV.Int64 {
		// 空表：给一个空 chunk，reader 会立即读到 0 行。
		return []*Chunk{{ID: chunkID(spec, 0), Spec: spec}}, nil
	}

	span := maxV.Int64 - minV.Int64 + 1
	nChunks := int64(targetChunks)
	if nChunks > span {
		nChunks = span
	}
	if nChunks < 1 {
		nChunks = 1
	}
	step := span / nChunks
	if step < 1 {
		step = 1
	}

	var chunks []*Chunk
	idx := 0
	// 第一个 chunk 下界为表头（nil），保证覆盖 MIN；末尾 chunk 上界为 nil，覆盖 MAX 之外的空洞。
	var prevEnd int64 = minV.Int64 - 1
	for prevEnd < maxV.Int64 {
		end := prevEnd + step
		if end >= maxV.Int64 {
			// 末尾 chunk：无上界，吸收所有剩余行。
			var start []any
			if idx > 0 {
				start = []any{prevEnd}
			}
			chunks = append(chunks, &Chunk{ID: chunkID(spec, idx), Spec: spec, Start: start, End: nil})
			break
		}
		var start []any
		if idx > 0 {
			start = []any{prevEnd}
		}
		chunks = append(chunks, &Chunk{ID: chunkID(spec, idx), Spec: spec, Start: start, End: []any{end}})
		prevEnd = end
		idx++
	}
	if len(chunks) == 0 {
		chunks = append(chunks, &Chunk{ID: chunkID(spec, 0), Spec: spec})
	}
	return chunks, nil
}

// planKeysetBoundaries 通过一次 PK-only keyset 步进扫描生成近似等行数边界。
func (p *Planner) planKeysetBoundaries(ctx context.Context, spec *TableSpec, col string, targetChunks int) ([]*Chunk, error) {
	est := spec.EstimatedRows
	if est <= 0 {
		est = p.estimateCount(ctx, spec)
	}
	// 行数很少时不切分。
	if est <= int64(defaultBatchRows) || targetChunks <= 1 {
		return []*Chunk{{ID: chunkID(spec, 0), Spec: spec}}, nil
	}

	step := est / int64(targetChunks)
	if step < int64(defaultBatchRows) {
		step = int64(defaultBatchRows)
	}

	// 逐个取第 step 行的主键值作为边界。
	var boundaries []any
	var last any
	for {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		var v any
		var q string
		var args []any
		if last == nil {
			q = fmt.Sprintf("SELECT `%s` FROM `%s`.`%s` ORDER BY `%s` ASC LIMIT 1 OFFSET ?", col, spec.SourceSchema, spec.SourceTable, col)
			args = []any{step - 1}
		} else {
			q = fmt.Sprintf("SELECT `%s` FROM `%s`.`%s` WHERE `%s` > ? ORDER BY `%s` ASC LIMIT 1 OFFSET ?", col, spec.SourceSchema, spec.SourceTable, col, col)
			args = []any{last, step - 1}
		}
		row := p.sourceDB.QueryRowContext(ctx, q, args...)
		if err := row.Scan(&v); err != nil {
			if err == sql.ErrNoRows {
				break
			}
			return nil, err
		}
		nv := normalizeScanned(v)
		boundaries = append(boundaries, nv)
		last = nv
		// 边界过多时停止（防止极端情况下无限逼近）。
		if len(boundaries) >= targetChunks*4 {
			break
		}
	}

	if len(boundaries) == 0 {
		return []*Chunk{{ID: chunkID(spec, 0), Spec: spec}}, nil
	}

	var chunks []*Chunk
	var start []any
	for i, b := range boundaries {
		chunks = append(chunks, &Chunk{ID: chunkID(spec, i), Spec: spec, Start: start, End: []any{b}})
		start = []any{b}
	}
	// 末尾 chunk：最后一个边界之后无上界。
	chunks = append(chunks, &Chunk{ID: chunkID(spec, len(boundaries)), Spec: spec, Start: start, End: nil})
	return chunks, nil
}

func (p *Planner) estimateCount(ctx context.Context, spec *TableSpec) int64 {
	var c int64
	q := "SELECT TABLE_ROWS FROM information_schema.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?"
	_ = p.sourceDB.QueryRowContext(ctx, q, spec.SourceSchema, spec.SourceTable).Scan(&c)
	return c
}

func chunkID(spec *TableSpec, idx int) string {
	return fmt.Sprintf("%s.%s#%d", spec.SourceSchema, spec.SourceTable, idx)
}

// isIntegerColumn 判断游标列是否为整数类型（可安全做值域算术切分）。
func isIntegerColumn(id *entity.TableIdentity, col string) bool {
	for _, c := range id.Columns {
		if c.Name != col {
			continue
		}
		dt := strings.ToLower(strings.TrimSpace(c.DataType))
		if i := strings.IndexByte(dt, '('); i >= 0 {
			dt = dt[:i]
		}
		dt = strings.TrimSpace(strings.TrimSuffix(strings.TrimSuffix(dt, " zerofill"), " unsigned"))
		switch dt {
		case "tinyint", "smallint", "mediumint", "int", "integer", "bigint":
			return true
		}
		return false
	}
	return false
}

func normalizeScanned(v any) any {
	if b, ok := v.([]byte); ok {
		out := make([]byte, len(b))
		copy(out, b)
		return out
	}
	return v
}
