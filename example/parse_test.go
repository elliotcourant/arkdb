package example_test

import (
	"github.com/pingcap/parser"
	_ "github.com/pingcap/tidb/types/parser_driver"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestParse(t *testing.T) {
	p := parser.New()
	t.Run("select", func(t *testing.T) {
		stmtNodes, _, err := p.Parse("SELECT 1, true;", "", "")
		assert.NoError(t, err)
		assert.NotEmpty(t, stmtNodes)
	})

	t.Run("create table", func(t *testing.T) {
		stmtNodes, _, err := p.Parse("CREATE TABLE test (id BIGINT PRIMARY KEY, name TEXT);", "", "")
		assert.NoError(t, err)
		assert.NotEmpty(t, stmtNodes)
	})
}
