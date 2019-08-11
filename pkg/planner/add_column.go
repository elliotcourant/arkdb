package planner

import (
	"github.com/elliotcourant/arkdb/pkg/storage"
)

type AddColumnPlanner interface {
	NamePrefix() []byte
	SetTableID(id uint8)
	Path() []byte
	Encode() []byte

	After() []PlanStep
}

type addColumnBase struct {
	column storage.Column
}

func (i *addColumnBase) NamePrefix() []byte {
	return i.column.Prefix()
}

func (i *addColumnBase) SetTableID(id uint8) {
	i.column.TableID = id
}

func (i *addColumnBase) Path() []byte {
	return i.column.Path()
}

func (i *addColumnBase) Encode() []byte {
	return make([]byte, 0)
}

func (i *addColumnBase) After() []PlanStep {
	return nil
}

func (p *planContext) addColumnPlanner(column storage.Column) AddColumnPlanner {
	return &addColumnBase{
		column: column,
	}
}
