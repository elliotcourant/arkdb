package storage

import (
	"encoding/hex"
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestTable_Path(t *testing.T) {
	t.Run("simple", func(t *testing.T) {
		item := &Table{
			TableID:    34,
			SchemaID:   23,
			DatabaseID: 12,
			TableName:  "accounts",
		}
		path := item.Path()
		assert.NotEmpty(t, path)
		fmt.Println(hex.Dump(path))
	})
}
