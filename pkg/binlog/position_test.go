package binlog

import (
	"testing"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestComparePositionSameFile(t *testing.T) {
	a := mysql.Position{Name: "mysql-bin.000001", Pos: 100}
	b := mysql.Position{Name: "mysql-bin.000001", Pos: 200}
	assert.Equal(t, -1, ComparePosition(a, b))
	assert.Equal(t, 1, ComparePosition(b, a))
	assert.Equal(t, 0, ComparePosition(a, a))
}

func TestComparePositionDifferentFileBySequence(t *testing.T) {
	a := mysql.Position{Name: "mysql-bin.000001", Pos: 9999}
	b := mysql.Position{Name: "mysql-bin.000002", Pos: 4}
	assert.Equal(t, -1, ComparePosition(a, b))
	assert.Equal(t, 1, ComparePosition(b, a))
}

func TestComparePositionFallsBackToLexicalFileName(t *testing.T) {
	a := mysql.Position{Name: "alpha-bin", Pos: 10}
	b := mysql.Position{Name: "beta-bin", Pos: 5}
	assert.Equal(t, -1, ComparePosition(a, b))
}

func TestParsePositionRejectsZeroOffset(t *testing.T) {
	_, err := ParsePosition("mysql-bin.000001:0")
	require.Error(t, err)
	assert.Contains(t, err.Error(), ">= 4")

	pos, err := ParsePosition("mysql-bin.000001:4")
	require.NoError(t, err)
	assert.Equal(t, mysql.Position{Name: "mysql-bin.000001", Pos: 4}, pos)
}

func TestValidatePosition(t *testing.T) {
	require.NoError(t, ValidatePosition(mysql.Position{Name: "mysql-bin.000001", Pos: 4}))
	require.Error(t, ValidatePosition(mysql.Position{Name: "", Pos: 4}))
	require.Error(t, ValidatePosition(mysql.Position{Name: "mysql-bin.000001", Pos: 0}))
}
