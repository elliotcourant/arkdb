package planner

import (
	"fmt"
	"github.com/elliotcourant/arkdb/pkg/storage"
	"github.com/elliotcourant/arkdb/pkg/types"
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
			ColumnID:   0,
			DatabaseID: 1,
			SchemaID:   1,
			TableID:    0,
			ColumnName: column.Name.Name.String(),
			ColumnType: types.Type(column.Tp.Tp),
			PrimaryKey: i == 0,
		})
		columns[i].checkExisting = false
	}

	return createTablePlan{
		table:   table,
		columns: columns,
	}
}

func (e *executeContext) createTable(plan createTablePlan) error {
	_, exists, err := e.getTable(plan.table.DatabaseID, plan.table.SchemaID, plan.table.TableName)
	if err != nil {
		return err
	}
	if exists {
		return fmt.Errorf("table with name [%s] already exists", plan.table.TableName)
	}

	tableId, err := e.tx.NextObjectID(plan.table.ObjectIdPrefix())
	if err != nil {
		return err
	}

	plan.table.TableID = tableId

	e.SetItem(plan.table)

	for _, column := range plan.columns {
		column.column.TableID = tableId
		if err := e.executeItem(column); err != nil {
			return err
		}
	}
	return nil
}
