package storage

import (
	"encoding/hex"
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestSchema_Path(t *testing.T) {
	t.Run("simple", func(t *testing.T) {
		item := &Schema{
			SchemaID:   23,
			DatabaseID: 12,
			SchemaName: "public",
		}
		path := item.Path()
		assert.NotEmpty(t, path)
		fmt.Println(hex.Dump(path))
	})
}
