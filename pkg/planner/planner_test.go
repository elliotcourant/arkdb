package planner

import (
	"github.com/elliotcourant/arkdb/pkg/parser"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestCreatePlan(t *testing.T) {
	t.Run("create table", func(t *testing.T) {
		nodes, err := parser.Parse("CREATE TABLE test (id BIGINT PRIMARY KEY, name TEXT);")
		assert.NoError(t, err)

		plan, err := CreatePlan(nodes)
		assert.NoError(t, err)
		assert.NotEmpty(t, plan)
	})
}
