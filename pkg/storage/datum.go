package storage

import (
	"github.com/elliotcourant/buffers"
)

type Datum struct {
	PrimaryKeyID      uint64
	ShardPrimaryKeyID uint64
	SchemaID          uint8
	TableID           uint8
	ColumnID          uint8
}

func (i Datum) Path() []byte {
	buf := buffers.NewBytesBuffer()
	buf.AppendUint8(i.SchemaID)
	buf.AppendUint8(i.TableID)
	buf.AppendUint64(i.ShardPrimaryKeyID)
	buf.AppendUint8(i.ColumnID)
	buf.AppendUint64(i.PrimaryKeyID)
	return buf.Bytes()
}
