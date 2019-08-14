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

		result, err := Execute(tx, plan)
		assert.NoError(t, err)
		assert.NotNil(t, result)

		fmt.Println(result.String())
	})
}
