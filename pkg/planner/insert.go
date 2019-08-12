package planner

import (
	"github.com/pingcap/parser/ast"
)

type insertPlanner struct {
	columnNames []string
	list        [][]interface{}
	tableName   string
	schemaName  string
}

func (p *planContext) insertPlanner(stmt *ast.InsertStmt) insertPlanner {
	return insertPlanner{}
}
