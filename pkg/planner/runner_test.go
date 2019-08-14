package planner

import (
	"fmt"
	"github.com/elliotcourant/arkdb/internal/bargeutil"
	"github.com/elliotcourant/arkdb/pkg/distribution"
	"github.com/elliotcourant/arkdb/pkg/parser"
	"github.com/elliotcourant/timber"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func Exec(t *testing.T, tx distribution.Transaction, query string) {
	start := time.Now()
	defer timber.Infof("test query execution time: %s", time.Since(start))
	nodes, err := parser.Parse(query)
	assert.NoError(t, err)

	plan, err := CreatePlan(nodes)
	assert.NoError(t, err)
	assert.NotEmpty(t, plan)

	result, err := Execute(tx, plan)
	assert.NoError(t, err)
	assert.NotNil(t, result)
}

func TestExecute(t *testing.T) {
	t.Run("create table", func(t *testing.T) {
		barge, cleanup := bargeutil.NewBarge(t)
		defer cleanup()

		tx, err := barge.Begin()
		assert.NoError(t, err)
		assert.NotNil(t, tx)

		Exec(t, tx, "CREATE TABLE test (id BIGINT PRIMARY KEY, name TEXT); CREATE TABLE thing (account_id BIGINT PRIMARY KEY, name TEXT, test_id BIGINT REFERENCES test (id));")

		err = tx.Commit()
		assert.NoError(t, err)
	})

	t.Run("runInsert", func(t *testing.T) {
		barge, cleanup := bargeutil.NewBarge(t)
		defer cleanup()

		tx, err := barge.Begin()
		assert.NoError(t, err)
		assert.NotNil(t, tx)

		Exec(t, tx, "CREATE TABLE test (id BIGINT PRIMARY KEY, name TEXT);")

		err = tx.Commit()
		assert.NoError(t, err)

		tx, err = barge.Begin()
		assert.NoError(t, err)
		assert.NotNil(t, tx)

		for i := 0; i < 100; i++ {
			Exec(t, tx, fmt.Sprintf("INSERT INTO test (id, name) VALUES (%d, 'elliot');", i+1))
		}

		err = tx.Commit()
		assert.NoError(t, err)
	})
}
