package planner

import (
	"fmt"
	"github.com/elliotcourant/arkdb/pkg/storage"
	"github.com/pingcap/parser/ast"
)

type selectPlanNode interface {
	do(e *executeContext, s *set) error
}

type selectPlanner struct {
	fields []selectField
	steps  []selectPlanNode
	tables map[string]uint8
	limit  int
}

func (p *planContext) selectPlanner(stmt *ast.SelectStmt) *selectPlanner {
	plan := &selectPlanner{
		fields: make([]selectField, len(stmt.Fields.Fields)),
	}

	for i, field := range stmt.Fields.Fields {
		f := &selectField{
			name: field.AsName.String(),
		}
		if field.WildCard != nil {
			f.wildcardTable = field.WildCard.Table.String()
			f.isWildcard = true
			continue
		}

		switch e := field.Expr.(type) {
		case ast.ValueExpr:
			f.expr = valueExpression{
				value: e.GetValue(),
			}
		case *ast.FuncCallExpr:
			// not handling this yet
		}
		plan.fields[i] = *f
	}

	if stmt.From == nil {
		plan.limit = 1
		plan.steps = append(plan.steps, selectSimplePlanner{
			fields: plan.fields,
		})
		return plan
	}

	return plan
}

func (e *executeContext) runSelect(plan *selectPlanner, s *set) error {
	return plan.do(e, s)
}

func (p *selectPlanner) do(e *executeContext, s *set) error {
	for tableName, _ := range p.tables {
		table, ok, _ := e.getTable(tableName)
		if !ok {
			return fmt.Errorf("table [%s] does not exist", tableName)
		}
		p.tables[tableName] = table.TableID
	}

	s.setNumberOfColumns(len(p.fields))
	for i, field := range p.fields {
		name := "??column??"
		if len(field.name) > 0 {
			name = field.name
		}
		s.setColumnName(i, name)
	}

	for _, step := range p.steps {
		if err := step.do(e, s); err != nil {
			return err
		}
	}

	return nil
}

type joinPlanner struct {
}

type wherePlanner struct {
	columns map[string][]storage.Column
}

type selectField struct {
	expr          expression
	name          string
	wildcardTable string
	isWildcard    bool
}

type selectSimplePlanner struct {
	fields []selectField
}

func (p selectSimplePlanner) do(e *executeContext, s *set) error {
	cells := make([]interface{}, len(p.fields))
	for i, field := range p.fields {
		val, err := field.expr.eval(e, nil)
		if err != nil {
			return err
		}
		cells[i] = val
	}
	s.addRow(cells...)
	return nil
}
