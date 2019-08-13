package planner

import (
	"fmt"
	"github.com/ahmetb/go-linq/v3"
	"github.com/elliotcourant/arkdb/pkg/storage"
	"github.com/elliotcourant/arkdb/pkg/types"
	"github.com/pingcap/parser/ast"
)

type insertPlanner struct {
	columnNames []string
	list        [][]interface{}
	table       storage.Table
	schemaName  string
}

func (p *planContext) insertPlanner(stmt *ast.InsertStmt) insertPlanner {
	plan := insertPlanner{
		columnNames: make([]string, len(stmt.Columns)),
		list:        make([][]interface{}, len(stmt.Lists)),
		table: storage.Table{
			DatabaseID: 1,
			SchemaID:   1,
		},
	}

	switch table := stmt.Table.TableRefs.Left.(type) {
	case *ast.TableSource:
		plan.table.TableName = table.Source.(*ast.TableName).Name.String()
	}

	for i := range plan.columnNames {
		plan.columnNames[i] = stmt.Columns[i].Name.String()
	}

	for y, row := range stmt.Lists {
		plan.list[y] = make([]interface{}, len(row))
		for x, datum := range row {
			plan.list[y][x] = datum.(ast.ValueExpr).GetValue()
		}
	}

	return plan
}

func (e *executeContext) insert(plan insertPlanner) error {
	table, tableExists, err := e.getTable(plan.table.DatabaseID, plan.table.SchemaID, plan.table.TableName)
	if err != nil {
		return err
	}
	if !tableExists {
		return fmt.Errorf("table [%s] does not exist", plan.table.TableName)
	}

	cols, err := e.getColumns(table.DatabaseID, table.SchemaID, table.TableID, plan.columnNames...)
	if err != nil {
		return err
	}

	primaryKeyIndex := linq.From(cols).IndexOf(func(i interface{}) bool {
		col, ok := i.(storage.Column)
		return ok && col.PrimaryKey
	})

	primaryKeys := make([]uint64, len(plan.list))
	switch primaryKeyIndex {
	case -1:
		return fmt.Errorf("cannot insert without primary key")
	default:
		for k, row := range plan.list {
			cell := row[primaryKeyIndex]
			switch i := cell.(type) {
			case int64:
				primaryKeys[k] = uint64(i)
			}
		}
	}

	for k, row := range plan.list {
		primaryKey := primaryKeys[k]
		for c, val := range row {
			col := cols[c]
			datum := storage.Datum{
				PrimaryKeyID: primaryKey,
				SchemaID:     col.SchemaID,
				TableID:      col.TableID,
				ColumnID:     col.ColumnID,
			}
			v, err := types.Encode(col.ColumnType, val)
			if err != nil {
				return err
			}
			datum.Value = v
			e.SetItem(datum)
		}
	}

	return nil
}
