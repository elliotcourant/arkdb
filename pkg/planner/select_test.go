package planner

import (
	"fmt"
	"github.com/elliotcourant/arkdb/internal/bargeutil"
	"github.com/elliotcourant/arkdb/pkg/parser"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestSelect(t *testing.T) {
	t.Run("simple", func(t *testing.T) {
		barge, cleanup := bargeutil.NewBarge(t)
		defer cleanup()

		tx, err := barge.Begin()
		assert.NoError(t, err)
		assert.NotNil(t, tx)

		nodes, err := parser.Parse("SELECT 1, 'test' AS name")
		assert.NoError(t, err)

		plan, err := CreatePlan(nodes)
		assert.NoError(t, err)
		assert.NotEmpty(t, plan)

		result, err := newExecuteContext(tx, plan).Execute()
		assert.NoError(t, err)
		assert.NotNil(t, result)

		fmt.Println(result.String())
	})

	t.Run("from table", func(t *testing.T) {
		barge, cleanup := bargeutil.NewBarge(t)
		defer cleanup()

		tx, err := barge.Begin()
		assert.NoError(t, err)
		assert.NotNil(t, tx)

		Query(tx, `CREATE TABLE foo (id BIGINT PRIMARY KEY, name TEXT);`)
		Query(tx, `INSERT INTO foo (id, name) VALUES(1, 'elliot'), (2, 'bob'), (3, 'rickster');`)

		result := Query(tx, `SELECT * FROM foo;`)
		fmt.Println(result.String())
	})
}
