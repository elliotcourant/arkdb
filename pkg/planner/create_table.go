package planner

import (
	"github.com/elliotcourant/arkdb/pkg/storage"
	"github.com/pingcap/parser/ast"
)

type createTablePlan struct {
	table   storage.Table
	columns []addColumnPlan
}

func (p *planContext) createTablePlanner(stmt *ast.CreateTableStmt) createTablePlan {
	table := storage.Table{
		TableID:    0,
		DatabaseID: 1,
		SchemaID:   1,
		TableName:  stmt.Table.Name.String(),
	}

	columns := make([]addColumnPlan, len(stmt.Cols))
	for i, column := range stmt.Cols {
		columns[i] = p.addColumnPlanner(storage.Column{
			ColumnID:   uint8(i + 1),
			DatabaseID: 1,
			SchemaID:   1,
			TableID:    0,
			ColumnName: column.Name.Name.String(),
			ColumnType: column.Tp.Tp,
		})
		columns[i].checkExisting = false
	}

	return createTablePlan{
		table:   table,
		columns: columns,
	}
}
