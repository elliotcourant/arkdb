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
	fields  []selectField
	steps   []selectPlanNode
	limit   int
	aliases map[string]string
	tables  map[string]uint8
}

type tableItem struct {
	tableName string
	asName    string
}

func (p *planContext) selectPlanner(stmt *ast.SelectStmt) *selectPlanner {
	plan := &selectPlanner{
		fields:  make([]selectField, len(stmt.Fields.Fields)),
		aliases: map[string]string{},
		tables:  map[string]uint8{},
	}

	for i, field := range stmt.Fields.Fields {
		f := &selectField{
			asName: field.AsName.String(),
		}

		if field.WildCard != nil {
			f.isWildcard = true
		} else {
			switch e := field.Expr.(type) {
			case ast.ValueExpr:
				f.expr = valueExpression{
					value: e.GetValue(),
				}
			case *ast.FuncCallExpr:
			// not handling this yet
			case *ast.ColumnNameExpr:
				f.name = e.Name.Name.String()
				f.table = e.Name.Table.String()
			}
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

	tables := p.getTables(stmt.From.TableRefs)
	for _, table := range tables {
		plan.aliases[table.asName] = table.tableName
		plan.tables[table.tableName] = 0
	}

	return plan
}

func (p *planContext) getTables(stmt *ast.Join) []tableItem {
	tables := make([]tableItem, 0)
	switch l := stmt.Left.(type) {
	case *ast.TableSource:
		tableName := l.Source.(*ast.TableName).Name.String()
		asName := l.AsName.String()
		if len(asName) == 0 {
			asName = tableName
		}
		tables = append(tables, tableItem{
			tableName: tableName,
			asName:    asName,
		})
	case *ast.Join:
		tables = append(tables, p.getTables(l)...)
	}
	if stmt.Right != nil {
		tableName := stmt.Right.(*ast.TableSource).Source.(*ast.TableName).Name.String()
		asName := stmt.Right.(*ast.TableSource).AsName.String()
		if len(asName) == 0 {
			asName = tableName
		}
		tables = append(tables, tableItem{
			tableName: tableName,
			asName:    asName,
		})
	}
	return tables
}

func (e *executeContext) runSelect(plan *selectPlanner, s *set) error {
	return plan.do(e, s)
}

func (p *selectPlanner) do(e *executeContext, s *set) error {
	// Pull table Ids
	for tableName := range p.tables {
		table, ok, _ := e.getTable(tableName)
		if !ok {
			return fmt.Errorf("table [%s] does not exist", tableName)
		}
		p.tables[tableName] = table.TableID
	}

	fieldNames := make([]string, 0)
	if len(p.tables) > 0 {
		type columnAndIndex struct {
			columnName string
			index      int
			isWildcard bool
			asName     string
		}
		tableAndColumnNames := map[uint8]map[string]int{}
		// Pull columns for tables.
		for i, column := range p.fields {
			tableId, columnName := uint8(0), column.name

			// If they directly specify a table then resolve that table or its alias to a table ID.
			if len(column.table) > 0 {
				tableName, ok := p.aliases[column.table]
				if !ok {
					return fmt.Errorf("cannot resolve table [%s] for column [%s]", column.table, column.name)
				}
				tableId, ok = p.tables[tableName]
				if !ok {
					return fmt.Errorf("could not resolve table [%s] for column [%s]", column.table, column.name)
				}
			}

			// If this is a wildcard column set the column name to an empty string
			// this is for doing prefix scans. An empty string will resolve to every
			// column in a table.
			if column.isWildcard {
				columnName = ""
			}

			if len(column.name) == 0 && !column.isWildcard {
				continue
			}
			_, ok := tableAndColumnNames[tableId]
			if !ok {
				tableAndColumnNames[tableId] = map[string]int{}
			}
			tableAndColumnNames[tableId][columnName] = i
		}

		numberOfColumns := 0
		ambiguousColumns := map[string]uint8{}
		resolveColumns := func(ambiguous bool, tableId uint8, cols map[string]int) error {
			names := make([]string, 0)
			for name := range cols {
				names = append(names, name)
			}

			columns, err := e.getColumnsEx(tableId, names...)
			if err != nil {
				return fmt.Errorf("could not resolve columns: %v", err)
			}

			for columnName, column := range columns {
				if ambiguous {
					_, ok := ambiguousColumns[columnName]
					if ok {
						return fmt.Errorf("column [%s] is ambiguous", columnName)
					}
					ambiguousColumns[columnName] = column.TableID
				}

				name := columnName
				fieldIndex, ok := cols[columnName]
				if ok {
					as := p.fields[fieldIndex].asName
					if len(as) > 0 {
						name = as
					}
				}
				fieldNames = append(fieldNames, name)
				e.columns = append(e.columns, column)
				numberOfColumns++
			}

			return nil
		}

		for tableId, columnMap := range tableAndColumnNames {
			switch tableId {
			case 0:
				for _, tId := range p.tables {
					if err := resolveColumns(true, tId, columnMap); err != nil {
						return err
					}
				}
			default:
				if err := resolveColumns(false, tableId, columnMap); err != nil {
					return err
				}
			}
		}
	} else {
		for _, field := range p.fields {
			name := field.name
			if len(field.asName) > 0 {
				name = field.asName
			}
			fieldNames = append(fieldNames, name)
		}
	}

	s.setNumberOfColumns(len(fieldNames))
	for i, field := range fieldNames {
		name := "?column?"
		if len(field) > 0 {
			name = field
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
	expr       expression
	name       string
	table      string
	asName     string
	isWildcard bool
	column     storage.Column
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
