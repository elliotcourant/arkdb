package storage

import (
	"encoding/hex"
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestDatum_Path(t *testing.T) {
	t.Run("simple", func(t *testing.T) {
		item := &Datum{
			PrimaryKeyID: 124929424,
			ColumnID:     8,
			TableID:      34,
		}
		path := item.Path()
		assert.NotEmpty(t, path)
		fmt.Println(hex.Dump(path))
	})
}
