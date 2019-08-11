package planner

import (
	"github.com/elliotcourant/arkdb/pkg/storage"
	"github.com/pingcap/parser/ast"
)

type CreateTablePlanner interface {
	NamePrefix() []byte
	SetObjectID(id uint8)
	Path() []byte
	Encode() []byte

	Columns() []AddColumnPlanner

	After() []PlanStep
}

type createTableBase struct {
	table   storage.Table
	columns []AddColumnPlanner
}

func (i *createTableBase) Columns() []AddColumnPlanner {
	return i.columns
}

func (i *createTableBase) After() []PlanStep {
	return nil
}

func (i *createTableBase) NamePrefix() []byte {
	return i.table.Prefix()
}

func (i *createTableBase) SetObjectID(id uint8) {
	i.table.TableID = id
}

func (i *createTableBase) Path() []byte {
	return i.table.Path()
}

func (i *createTableBase) Encode() []byte {
	return i.table.Encode()
}

func (p *planContext) createTablePlanner(stmt *ast.CreateTableStmt) CreateTablePlanner {
	table := storage.Table{
		TableID:    0,
		DatabaseID: 1,
		SchemaID:   1,
		TableName:  stmt.Table.Name.String(),
	}

	columns := make([]AddColumnPlanner, len(stmt.Cols))
	for i, column := range stmt.Cols {
		columns[i] = p.addColumnPlanner(storage.Column{
			ColumnID:   uint8(i + 1),
			DatabaseID: 1,
			SchemaID:   1,
			TableID:    0,
			ColumnName: column.Name.Name.String(),
			ColumnType: column.Tp.Tp,
		})
		columns[i].SetCheckExisting(false)
	}

	return &createTableBase{
		table:   table,
		columns: columns,
	}
}
