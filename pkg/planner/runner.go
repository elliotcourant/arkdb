package planner

import (
	"fmt"
	"github.com/elliotcourant/arkdb/pkg/distribution"
	"github.com/elliotcourant/arkdb/pkg/storage"
	"github.com/elliotcourant/timber"
	"sort"
	"time"
)

type setItem interface {
	Path() []byte
	Encode() []byte
}

type executeContext struct {
	start         time.Time
	tx            distribution.Transaction
	pendingWrites map[string][]byte
}

func Execute(tx distribution.Transaction, plan Plan) (interface{}, error) {
	e := &executeContext{
		start:         time.Now(),
		tx:            tx,
		pendingWrites: map[string][]byte{},
	}
	defer timber.Tracef("query execution took: %s", time.Since(e.start))
	for _, step := range plan.Steps {
		err := e.executeItem(step)
		if err != nil {
			return nil, err
		}
	}
	return nil, e.commit()
}

func (e *executeContext) SetItem(item setItem) {
	e.Set(item.Path(), item.Encode())
}

func (e *executeContext) Set(key, value []byte) {
	e.pendingWrites[string(key)] = value
}

func (e *executeContext) Delete(key []byte) {
	e.pendingWrites[string(key)] = nil
}

func (e *executeContext) commit() error {
	for k, v := range e.pendingWrites {
		if v == nil {
			if err := e.tx.Delete([]byte(k)); err != nil {
				return err
			}
		} else {
			if err := e.tx.SetRaw([]byte(k), v); err != nil {
				return err
			}
		}
	}
	return nil
}

func (e *executeContext) doesExist(prefix []byte) (exists bool, err error) {
	err = func(tx distribution.Transaction, prefix []byte) error {
		itr := tx.GetKeyIterator(prefix, true, false)
		defer itr.Close()
		for itr.Seek(prefix); itr.ValidForPrefix(prefix); {
			exists = true
			return nil
		}
		return nil
	}(e.tx, prefix)
	return exists, err
}

func (e *executeContext) getTable(databaseId, schemaId uint8, tableName string) (*storage.Table, bool, error) {
	tbl := &storage.Table{
		DatabaseID: databaseId,
		SchemaID:   schemaId,
		TableName:  tableName,
	}
	var exists bool
	table := &storage.Table{}
	err := func(tx distribution.Transaction, prefix []byte) error {
		itr := tx.GetKeyIterator(prefix, false, false)
		defer itr.Close()
		for itr.Seek(prefix); itr.ValidForPrefix(prefix); {
			exists = true
			return itr.Item().Value(func(val []byte) error {
				return table.Decode(val)
			})
		}
		return nil
	}(e.tx, tbl.Prefix())
	return table, exists, err
}

func (e *executeContext) getColumns(databaseId, schemaId, tableId uint8, columnNames ...string) ([]storage.Column, error) {
	col := &storage.Column{
		DatabaseID: databaseId,
		SchemaID:   schemaId,
		TableID:    tableId,
	}
	sort.Strings(columnNames)
	columns := make([]storage.Column, len(columnNames))
	err := func(tx distribution.Transaction, prefix []byte) error {
		itr := tx.GetKeyIterator(prefix, false, false)
		defer itr.Close()
		for i, columnName := range columnNames {
			col.ColumnName = columnName
			colPrefix := col.Prefix()
			if err := func() error {
				for itr.Seek(colPrefix); itr.ValidForPrefix(colPrefix); {
					return itr.Item().Value(func(val []byte) error {
						err := col.Decode(val)
						columns[i] = *col
						return err
					})
				}
				return fmt.Errorf("could not find column [%s]", columnName)
			}(); err != nil {
				return err
			}
		}
		return nil
	}(e.tx, col.ObjectIdPrefix())
	return columns, err
}

func (e *executeContext) executeItem(step PlanStep) error {
	switch item := step.(type) {
	case addColumnPlan:
		return e.addColumn(item)
	case createTablePlan:
		return e.createTable(item)
	case insertPlanner:
		return e.insert(item)
	default:
		return fmt.Errorf("cannot execute plan for [%T]", item)
	}
	return nil
}
