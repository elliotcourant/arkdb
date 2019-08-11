package planner

import (
	"github.com/elliotcourant/arkdb/internal/bargeutil"
	"github.com/elliotcourant/arkdb/pkg/parser"
	"github.com/elliotcourant/timber"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestExecute(t *testing.T) {
	t.Run("create table", func(t *testing.T) {
		barge, cleanup := bargeutil.NewBarge(t)
		defer cleanup()
		nodes, err := parser.Parse("CREATE TABLE test (id BIGINT PRIMARY KEY, name TEXT);")
		assert.NoError(t, err)

		plan, err := CreatePlan(nodes)
		assert.NoError(t, err)
		assert.NotEmpty(t, plan)

		tx, err := barge.Begin()
		assert.NoError(t, err)
		assert.NotNil(t, tx)

		result, err := Execute(tx, plan)
		assert.NoError(t, err)
		assert.Nil(t, result)

		start := time.Now()
		result, err = Execute(tx, plan)
		timber.Debugf("execution time [%s]", time.Since(start))
		assert.Error(t, err)
		assert.Nil(t, result)
	})
}
