.PHONY: generated

generated:
	@echo nothing to generate

test:
	./coverage.sh

strings:
	stringer -type Type -output pkg/types/types.string.go pkg/types/types.go