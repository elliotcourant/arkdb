package planner

import (
	"fmt"
	"github.com/dgraph-io/badger"
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
	results       *results

	// table -> column index -> primary key -> value
	stmtCache map[string][]map[uint64][]byte
}

func Execute(tx distribution.Transaction, plan Plan) (Results, error) {
	e := &executeContext{
		start:         time.Now(),
		tx:            tx,
		pendingWrites: map[string][]byte{},
		results: &results{
			sets: make([]*set, len(plan.Steps)),
		},
	}
	defer timber.Tracef("query execution took: %s", time.Since(e.start))
	for i, step := range plan.Steps {
		e.stmtCache = map[string][]map[uint64][]byte{}
		e.results.sets[i] = &set{}
		err := e.executeItem(step, e.results.sets[i])
		if err != nil {
			return nil, err
		}
		err = e.commitStatement()
		if err != nil {
			return nil, err
		}
	}
	return e.results, nil
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

func (e *executeContext) commitStatement() error {
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

func (e *executeContext) getTable(tableName string) (*storage.Table, bool, error) {
	tbl := &storage.Table{
		TableName: tableName,
	}
	err := e.tx.Get(tbl.Path(), tbl)
	if err != nil && err != badger.ErrKeyNotFound {
		return nil, false, err
	}
	return tbl, tbl.TableID > 0, nil
}

func (e *executeContext) getColumns(tableId uint8, columnNames ...string) ([]storage.Column, error) {
	col := &storage.Column{
		TableID: tableId,
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

func (e *executeContext) executeItem(step PlanStep, s *set) error {
	switch item := step.(type) {
	case addColumnPlan:
		return e.runAddColumn(item)
	case createTablePlan:
		return e.runCreateTable(item, s)
	case insertPlanner:
		return e.runInsert(item)
	case *selectPlanner:
		return e.runSelect(item, s)
	default:
		return fmt.Errorf("cannot execute plan for [%T]", item)
	}
	return nil
}
