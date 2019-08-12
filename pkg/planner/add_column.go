package planner

import (
	"github.com/elliotcourant/arkdb/pkg/storage"
)

type addColumnPlan struct {
	checkExisting bool
	column        storage.Column
}

func (p *planContext) addColumnPlanner(column storage.Column) addColumnPlan {
	return addColumnPlan{
		column:        column,
		checkExisting: true,
	}
}
