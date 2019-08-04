package example__test

import (
	"github.com/pingcap/parser"
	_ "github.com/pingcap/tidb/types/parser_driver"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestParse(t *testing.T) {
	p := parser.New()
	stmtNodes, _, err := p.Parse("SELECT 1, true;", "", "")
	assert.NoError(t, err)
	assert.NotEmpty(t, stmtNodes)
}
