package wire

import (
	"encoding/hex"
	"fmt"
	"github.com/elliotcourant/arkdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestApplyTransactionRequest(t *testing.T) {
	t.Run("encode and decode", func(t *testing.T) {
		appendEntry := ApplyTransactionRequest{
			NodeID: "1234",
			Transaction: &storage.Transaction{
				Actions: []storage.Action{
					{
						Type:  storage.ActionTypeSet,
						Key:   []byte("test"),
						Value: []byte("value"),
					},
					{
						Type:  storage.ActionTypeDelete,
						Key:   []byte("test2"),
						Value: nil,
					},
				},
			},
		}
		encoded := appendEntry.Encode()
		fmt.Println(hex.Dump(encoded))
		decodeEntry := ApplyTransactionRequest{}
		err := decodeEntry.Decode(encoded)
		assert.NoError(t, err)
		assert.Equal(t, appendEntry, decodeEntry)
	})
}
