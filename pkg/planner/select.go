package planner

import (
	"github.com/elliotcourant/arkdb/pkg/storage"
	"github.com/pingcap/parser/ast"
)

type selectPlanNode interface {
	do(e *executeContext, s *set) error
}

type selectPlanner struct {
	fields []selectField
	steps  []selectPlanNode
	limit  int
}

func (p *planContext) selectPlanner(stmt *ast.SelectStmt) selectPlanner {
	plan := selectPlanner{
		fields: make([]selectField, len(stmt.Fields.Fields)),
	}

	for i, field := range stmt.Fields.Fields {
		f := &selectField{
			name: field.AsName.String(),
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

type joinPlanner struct {
}

type wherePlanner struct {
	columns map[string][]storage.Column
}

type selectField struct {
	expr expression
	name string
}

func (p selectPlanner) do(e *executeContext, s *set) error {
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

func (e *executeContext) runSelect(plan selectPlanner, s *set) error {
	return plan.do(e, s)
}
