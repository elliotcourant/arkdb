package storage

import (
	"github.com/elliotcourant/buffers"
)

type Datum struct {
	PrimaryKeyID uint64
	SchemaID     uint8
	TableID      uint8
	ColumnID     uint8

	Value []byte
}

func (i Datum) Encode() []byte {
	return i.Value
}

func (i Datum) Path() []byte {
	buf := buffers.NewBytesBuffer()
	buf.AppendByte(MetaPrefix_Datum)
	buf.AppendUint8(i.SchemaID)
	buf.AppendUint8(i.TableID)
	buf.AppendUint8(i.ColumnID)
	buf.AppendUint64(i.PrimaryKeyID)
	return buf.Bytes()
}
