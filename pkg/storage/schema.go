package storage

import (
	"github.com/elliotcourant/buffers"
)

type Schema struct {
	DatabaseID uint8
	SchemaID   uint8
	SchemaName string
}

func (s Schema) Path() []byte {
	buf := buffers.NewBytesBuffer()
	buf.AppendByte(MetaPrefix_Schema)
	buf.AppendUint8(s.DatabaseID)
	buf.AppendString(s.SchemaName)
	buf.AppendUint8(s.SchemaID)
	return buf.Bytes()
}
