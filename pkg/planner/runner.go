package planner

import (
	"fmt"
	"github.com/elliotcourant/arkdb/pkg/distribution"
	"github.com/elliotcourant/timber"
)

func Execute(tx distribution.Transaction, plan Plan) (interface{}, error) {
	for _, step := range plan.Steps {
		switch item := step.(type) {
		case CreateTablePlanner:
			err := func(tx distribution.Transaction, item CreateTablePlanner) error {
				itr := tx.GetKeyIterator(item.NamePrefix(), false)
				defer itr.Close()
				for itr.Seek(item.NamePrefix()); itr.ValidForPrefix(item.NamePrefix()); {
					timber.Debugf("found table with matching name")
					return fmt.Errorf("a table with the same name already exists")
				}
				return nil
			}(tx, item)
			if err != nil {
				return nil, err
			}

			tableId := uint8(3)

			item.SetObjectID(tableId)

			if err = tx.Set(item.Path(), item); err != nil {
				return nil, err
			}

			for _, column := range item.Columns() {
				column.SetTableID(tableId)
				if err = tx.Set(column.Path(), column); err != nil {
					return nil, err
				}
			}
		}
	}

	return nil, nil
}
