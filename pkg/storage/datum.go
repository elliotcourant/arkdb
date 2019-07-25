package storage

import (
	"github.com/elliotcourant/arkdb/pkg/buffers"
)

type Datum struct {
	PrimaryKeyID      uint64
	ShardPrimaryKeyID uint64
	SchemaID          uint8
	TableID           uint8
	ColumnID          uint8
}

func (i Datum) Path() []byte {
	buf := buffers.NewAllocatedBytesBuffer(19)
	buf.Append(i.SchemaID, i.TableID)
	buf.AppendUint64(i.ShardPrimaryKeyID)
	buf.Append(i.ColumnID)
	buf.AppendUint64(i.PrimaryKeyID)
	return buf.Bytes()
}
