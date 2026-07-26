package binlog

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/go-mysql-org/go-mysql/mysql"
)

var binlogSeqSuffix = regexp.MustCompile(`(\d+)$`)

// MinBinlogPosition is the smallest valid binlog byte offset (first event header).
const MinBinlogPosition = uint32(4)

// FormatPosition serializes a binlog position as "file:pos". Empty name yields "".
func FormatPosition(pos mysql.Position) string {
	if pos.Name == "" {
		return ""
	}
	return fmt.Sprintf("%s:%d", pos.Name, pos.Pos)
}

// ParsePosition parses "file:pos" into mysql.Position.
func ParsePosition(s string) (mysql.Position, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return mysql.Position{}, fmt.Errorf("empty binlog position")
	}
	idx := strings.LastIndex(s, ":")
	if idx <= 0 || idx == len(s)-1 {
		return mysql.Position{}, fmt.Errorf("invalid binlog position %q", s)
	}
	pos, err := strconv.ParseUint(s[idx+1:], 10, 32)
	if err != nil {
		return mysql.Position{}, fmt.Errorf("invalid binlog position %q: %w", s, err)
	}
	file := s[:idx]
	p := mysql.Position{Name: file, Pos: uint32(pos)}
	if err := ValidatePosition(p); err != nil {
		return mysql.Position{}, err
	}
	return p, nil
}

// ValidatePosition rejects empty file names and zero/invalid offsets (e.g. file:0).
func ValidatePosition(pos mysql.Position) error {
	if pos.Name == "" {
		return fmt.Errorf("binlog file name is required")
	}
	if pos.Pos < MinBinlogPosition {
		return fmt.Errorf("binlog position offset must be >= %d, got %d", MinBinlogPosition, pos.Pos)
	}
	return nil
}

// ComparePosition compares two binlog positions.
// Returns -1 if a < b, 0 if a == b, 1 if a > b.
// File order prefers trailing numeric sequence when both names parse; otherwise falls back to lexical name order.
func ComparePosition(a, b mysql.Position) int {
	if a.Name != b.Name {
		if cmp := compareBinlogFileName(a.Name, b.Name); cmp != 0 {
			return cmp
		}
	}
	if a.Pos < b.Pos {
		return -1
	}
	if a.Pos > b.Pos {
		return 1
	}
	return 0
}

func compareBinlogFileName(a, b string) int {
	aSeq, aOk := parseBinlogSequence(a)
	bSeq, bOk := parseBinlogSequence(b)
	if aOk && bOk {
		if aSeq < bSeq {
			return -1
		}
		if aSeq > bSeq {
			return 1
		}
	}
	if a < b {
		return -1
	}
	if a > b {
		return 1
	}
	return 0
}

func parseBinlogSequence(name string) (uint64, bool) {
	m := binlogSeqSuffix.FindStringSubmatch(name)
	if len(m) < 2 {
		return 0, false
	}
	seq, err := strconv.ParseUint(m[1], 10, 64)
	if err != nil {
		return 0, false
	}
	return seq, true
}
