package wire

import (
	"encoding/hex"
	"errors"
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestErrorResponse_Server(t *testing.T) {
	ErrorResponse{}.Server()
}

func TestErrorResponse(t *testing.T) {
	t.Run("encode and decode", func(t *testing.T) {
		i := ErrorResponse{
			Error: errors.New("test"),
		}
		encoded := i.Encode()
		fmt.Println(hex.Dump(encoded))
		d := ErrorResponse{}
		err := d.Decode(encoded)
		assert.NoError(t, err)
		assert.Equal(t, i, d)
	})
}
