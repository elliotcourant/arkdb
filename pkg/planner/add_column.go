package planner

import (
	"github.com/elliotcourant/arkdb/pkg/storage"
)

type AddColumnPlanner interface {
	SetCheckExisting(do bool)
	CheckExisting() bool
	NamePrefix() []byte
	SetTableID(id uint8)
	Path() []byte
	Encode() []byte

	After() []PlanStep
}

type addColumnBase struct {
	checkExisting bool
	column        storage.Column
}

func (i *addColumnBase) SetCheckExisting(do bool) {
	i.checkExisting = do
}

func (i *addColumnBase) CheckExisting() bool {
	return i.checkExisting
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
		column:        column,
		checkExisting: true,
	}
}
