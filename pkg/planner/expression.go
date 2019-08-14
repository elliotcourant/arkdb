package planner

type expression interface {
	eval(e *executeContext, value interface{}) (interface{}, error)
}

type valueExpression struct {
	value interface{}
}

func (v valueExpression) eval(e *executeContext, value interface{}) (interface{}, error) {
	return v.value, nil
}
