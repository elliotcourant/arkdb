package planner

import (
	"fmt"
	"github.com/elliotcourant/arkdb/pkg/distribution"
)

func Execute(tx distribution.Transaction, plan Plan) (interface{}, error) {
	for _, step := range plan.Steps {
		_, err := executeItem(tx, step)
		if err != nil {
			return nil, err
		}
	}

	return nil, nil
}

func doesExist(tx distribution.Transaction, prefix []byte) (exists bool, err error) {
	err = func(tx distribution.Transaction, prefix []byte) error {
		itr := tx.GetKeyIterator(prefix, false)
		defer itr.Close()
		for itr.Seek(prefix); itr.ValidForPrefix(prefix); {
			exists = true
			return nil
		}
		return nil
	}(tx, prefix)
	return exists, err
}

func executeItem(tx distribution.Transaction, step PlanStep) (interface{}, error) {
	switch item := step.(type) {
	case AddColumnPlanner:
		if item.CheckExisting() {
			exists, err := doesExist(tx, item.NamePrefix())
			if err != nil {
				return nil, err
			}
			if exists {
				return nil, fmt.Errorf("a column with matching name already exists")
			}
		}
		if err := tx.Set(item.Path(), item); err != nil {
			return nil, err
		}
	case CreateTablePlanner:
		exists, err := doesExist(tx, item.NamePrefix())
		if err != nil {
			return nil, err
		}
		if exists {
			return nil, fmt.Errorf("a table with the same name already exists")
		}

		tableId := uint8(3)

		item.SetObjectID(tableId)

		if err = tx.Set(item.Path(), item); err != nil {
			return nil, err
		}

		for _, column := range item.Columns() {
			column.SetTableID(tableId)
			if _, err := executeItem(tx, column); err != nil {
				return nil, err
			}
		}
	}
	return nil, nil
}
