package storage

import (
	"github.com/elliotcourant/arkdb/pkg/buffers"
)

type Schema struct {
	DatabaseID uint8
	SchemaID   uint8
	SchemaName string
}

func (s Schema) Path() []byte {
	l := len(s.SchemaName)
	buf := buffers.NewAllocatedBytesBuffer(4 + l)
	buf.Append(MetaPrefix_Schema, s.DatabaseID, uint8(l))
	buf.AppendString(s.SchemaName)
	buf.Append(s.SchemaID)
	return buf.Bytes()
}
