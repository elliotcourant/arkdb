package parser

import (
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	_ "github.com/pingcap/tidb/types/parser_driver"
	"sync"
)

const (
	NumberOfParsers = 8
)

var (
	parserPool = sync.Pool{
		New: newParser,
	}
)

func init() {
	for i := 0; i < NumberOfParsers; i++ {
		parserPool.Put(newParser())
	}
}

type parseBase struct {
	p *parser.Parser
}

func (p *parseBase) Parse(query string) ([]ast.StmtNode, error) {
	nodes, _, err := p.p.Parse(query, "", "")
	return nodes, err
}

func (p *parseBase) Release() {
	parserPool.Put(p.p)
}

func Parse(query string) ([]ast.StmtNode, error) {
	p := GetParser()
	defer p.Release()
	return p.Parse(query)
}

func GetParser() Parser {
	p := parserPool.Get().(*parser.Parser)
	return &parseBase{
		p: p,
	}
}

type Parser interface {
	Parse(query string) ([]ast.StmtNode, error)
	Release()
}

func newParser() interface{} {
	return parser.New()
}
