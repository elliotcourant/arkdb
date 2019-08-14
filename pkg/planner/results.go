package planner

import (
	"bytes"
	"fmt"
	"github.com/olekukonko/tablewriter"
	"sync"
)

type Results interface {
	Error() error
	Next() bool
	Scan(dst ...interface{}) error
	NextResultSet() bool
	String() string
}

type results struct {
	currentSync sync.Mutex
	currentSet  int
	currentRow  int
	sets        []*set
	err         error
}

func (r *results) Error() error {
	return r.err
}

func (r *results) NextResultSet() bool {
	r.currentSync.Lock()
	defer r.currentSync.Unlock()
	if r.currentSet+1 == len(r.sets) {
		return false
	}
	r.currentSet++
	r.currentRow = 0
	return true
}

func (r *results) Next() bool {
	r.currentSync.Lock()
	defer r.currentSync.Unlock()
	if len(r.sets) == 0 {
		return false
	}
	if r.currentRow+1 == len(r.sets[r.currentSet].datums) {
		return false
	}
	r.currentRow++
	return true
}

func (r *results) Scan(dst ...interface{}) error {
	return nil
}

func (r *results) String() string {
	writer := bytes.NewBuffer(nil)
	formatted := tablewriter.NewWriter(writer)
	for _, resultSet := range r.sets {
		formatted.SetHeader(resultSet.columns)
		table := make([][]string, len(resultSet.datums))
		for i, row := range resultSet.datums {
			table[i] = make([]string, len(row))
			for x, cell := range row {
				table[i][x] = fmt.Sprintf("%v", cell)
			}
		}
		formatted.AppendBulk(table)
		formatted.Render()
	}
	return writer.String()
}

type set struct {
	columns []string
	datums  [][]interface{}
}

func (s *set) setNumberOfColumns(count int) {
	s.columns = make([]string, count)
}

func (s *set) setColumnName(index int, name string) {
	s.columns[index] = name
}

func (s *set) addRow(datum ...interface{}) {
	s.datums = append(s.datums, datum)
}
