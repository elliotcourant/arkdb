package wire

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestWriteWireMessage(t *testing.T) {
	t.Run("appendEntriesRequest", func(t *testing.T) {
		b := writeWireMessage(&AppendEntriesRequest{})
		assert.NotEmpty(t, b)
	})

	t.Run("appendEntriesResponse", func(t *testing.T) {
		b := writeWireMessage(&AppendEntriesResponse{})
		assert.NotEmpty(t, b)
	})

	t.Run("requestVoteRequest", func(t *testing.T) {
		b := writeWireMessage(&RequestVoteRequest{})
		assert.NotEmpty(t, b)
	})

	t.Run("requestVoteResponse", func(t *testing.T) {
		b := writeWireMessage(&RequestVoteResponse{})
		assert.NotEmpty(t, b)
	})

	t.Run("installSnapshotRequest", func(t *testing.T) {
		b := writeWireMessage(&InstallSnapshotRequest{})
		assert.NotEmpty(t, b)
	})

	t.Run("installSnapshotResponse", func(t *testing.T) {
		b := writeWireMessage(&InstallSnapshotResponse{})
		assert.NotEmpty(t, b)
	})

	t.Run("errorResponse", func(t *testing.T) {
		b := writeWireMessage(&ErrorResponse{})
		assert.NotEmpty(t, b)
	})

	t.Run("panics", func(t *testing.T) {
		assert.Panics(t, func() {
			var test Message
			writeWireMessage(test)
		})
	})
}
