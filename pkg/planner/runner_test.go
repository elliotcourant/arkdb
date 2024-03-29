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

	result, err := newExecuteContext(tx, plan).Execute()
	assert.NoError(t, err)
	assert.NotNil(t, result)
}

func Query(tx distribution.Transaction, query string) Results {
	start := time.Now()
	defer timber.Infof("test query execution time: %s", time.Since(start))
	nodes, err := parser.Parse(query)
	if err != nil {
		panic(err)
	}

	plan, err := CreatePlan(nodes)
	if err != nil {
		panic(err)
	}

	result, err := newExecuteContext(tx, plan).Execute()
	if err != nil {
		panic(err)
	}
	return result
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

func TestExecuteContext_GetColumnsEx(t *testing.T) {
	t.Run("get all columns", func(t *testing.T) {
		barge, cleanup := bargeutil.NewBarge(t)
		defer cleanup()

		tx, err := barge.Begin()
		assert.NoError(t, err)
		assert.NotNil(t, tx)

		nodes, err := parser.Parse("CREATE TABLE test (id BIGINT PRIMARY KEY, name TEXT);")
		if err != nil {
			panic(err)
		}

		plan, err := CreatePlan(nodes)
		if err != nil {
			panic(err)
		}

		e := newExecuteContext(tx, plan)

		_, err = e.Execute()
		assert.NoError(t, err)

		columns, err := e.getColumnsEx(1, "")
		assert.NoError(t, err)
		assert.NotEmpty(t, columns)
		assert.Len(t, columns, 2)

		columns, err = e.getColumnsEx(1, "name")
		assert.NoError(t, err)
		assert.NotEmpty(t, columns)
		assert.Len(t, columns, 1)
	})
}
