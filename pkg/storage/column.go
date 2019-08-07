package storage

import (
	"github.com/elliotcourant/buffers"
)

var (
	columnMinimumSize = 10
)

type Column struct {
	ColumnID   uint8
	DatabaseID uint8
	SchemaID   uint8
	TableID    uint8
	ColumnName string
	ColumnType uint16
}

func (i Column) Size() int {
	return columnMinimumSize + len(i.ColumnName)
}

func (i Column) Path() []byte {
	buf := buffers.NewBytesBuffer()
	buf.AppendByte(MetaPrefix_Column)
	buf.AppendUint8(i.DatabaseID)
	buf.AppendUint8(i.SchemaID)
	buf.AppendUint8(i.TableID)
	buf.AppendString(i.ColumnName)
	buf.AppendUint16(i.ColumnType)
	return buf.Bytes()
}
