package storage

import (
	"encoding/hex"
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestDatabase_Path(t *testing.T) {
	t.Run("simple", func(t *testing.T) {
		d := &Database{
			DatabaseID:   12,
			DatabaseName: "public",
		}
		path := d.Path()
		assert.NotEmpty(t, path)
		fmt.Println(hex.Dump(path))
	})
}
