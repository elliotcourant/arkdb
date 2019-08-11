package parser

import (
	"github.com/stretchr/testify/assert"
	"sync"
	"testing"
)

func TestGetParser(t *testing.T) {
	t.Run("concurrent", func(t *testing.T) {
		numberOfThreads := NumberOfParsers * 2
		var wg sync.WaitGroup
		wg.Add(numberOfThreads)
		for i := 0; i < numberOfThreads; i++ {
			go func() {
				defer wg.Done()
				for x := 0; x < 100; x++ {
					func() {
						p := GetParser()
						defer p.Release()
						nodes, err := p.Parse("SELECT 1")
						assert.NoError(t, err)
						assert.NotEmpty(t, nodes)
					}()
				}
			}()
		}
		wg.Wait()
	})
}
