.PHONY: generated parser

generated: parser

parser:
	goyacc -o pkg/ast/ast.go pkg/ast/ast.y