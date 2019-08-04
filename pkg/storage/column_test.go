package storage

import (
	"encoding/hex"
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestColumn_Path(t *testing.T) {
	t.Run("simple", func(t *testing.T) {
		item := &Column{
			ColumnID:   8,
			TableID:    34,
			SchemaID:   23,
			DatabaseID: 12,
			ColumnName: "account_id",
			ColumnType: 105,
		}
		path := item.Path()
		assert.NotEmpty(t, path)
		fmt.Println(hex.Dump(path))
	})
}

func TestColumn_Size(t *testing.T) {
	t.Run("simple", func(t *testing.T) {
		item := &Column{
			ColumnID:   8,
			TableID:    34,
			SchemaID:   23,
			DatabaseID: 12,
			ColumnName: "account_id",
			ColumnType: 105,
		}
		size := item.Size()
		path := item.Path()
		assert.NotEmpty(t, path)
		assert.Equal(t, size, len(path))
	})
}
