package planner

import (
	"fmt"
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

func (e *executeContext) addColumn(plan addColumnPlan) error {
	if plan.checkExisting {
		exists, err := e.doesExist(plan.column.Prefix())
		if err != nil {
			return err
		}
		if exists {
			return fmt.Errorf("a column with matching name already exists")
		}
	}

	columnId, err := e.tx.NextObjectID(plan.column.ObjectIdPrefix())
	if err != nil {
		return err
	}

	plan.column.ColumnID = columnId

	e.SetItem(plan.column)
	return nil
}
