package planner

import (
	"fmt"
	"github.com/pingcap/parser/ast"
)

type planContext struct {
	Plan Plan
}

type Plan struct {
	Steps []PlanStep
}

type PlanStep interface {
	After() []PlanStep
}

func CreatePlan(nodes []ast.StmtNode) (Plan, error) {
	ptx := &planContext{
		Plan: Plan{
			Steps: make([]PlanStep, len(nodes)),
		},
	}
	for i, node := range nodes {
		switch n := node.(type) {
		case *ast.CreateTableStmt:
			ptx.Plan.Steps[i] = ptx.createTablePlanner(n)
		default:
			return ptx.Plan, fmt.Errorf("cannot create plan for stmt node type [%T]", n)
		}
	}

	return ptx.Plan, nil
}
