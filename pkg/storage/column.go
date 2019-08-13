package storage

import (
	"github.com/elliotcourant/buffers"
)

var (
	columnMinimumSize = 9
)

type Column struct {
	ColumnID   uint8
	DatabaseID uint8
	SchemaID   uint8
	TableID    uint8
	ColumnName string
	ColumnType uint8

	PrimaryKey bool
}

func (i Column) Encode() []byte {
	buf := buffers.NewBytesBuffer()
	buf.AppendUint8(i.ColumnID)
	buf.AppendUint8(i.DatabaseID)
	buf.AppendUint8(i.SchemaID)
	buf.AppendUint8(i.TableID)
	buf.AppendString(i.ColumnName)
	buf.AppendUint8(i.ColumnType)
	buf.AppendBool(i.PrimaryKey)
	return buf.Bytes()
}

func (i *Column) Decode(src []byte) error {
	*i = Column{}
	buf := buffers.NewBytesReader(src)
	i.ColumnID = buf.NextUint8()
	i.DatabaseID = buf.NextUint8()
	i.SchemaID = buf.NextUint8()
	i.TableID = buf.NextUint8()
	i.ColumnName = buf.NextString()
	i.ColumnType = buf.NextUint8()
	i.PrimaryKey = buf.NextBool()
	return nil
}

func (i Column) Size() int {
	return columnMinimumSize + len(i.ColumnName)
}

func (i Column) ObjectIdPrefix() []byte {
	buf := buffers.NewBytesBuffer()
	buf.AppendByte(MetaPrefix_Column)
	buf.AppendUint8(i.DatabaseID)
	buf.AppendUint8(i.SchemaID)
	buf.AppendUint8(i.TableID)
	return buf.Bytes()
}

func (i Column) Prefix() []byte {
	buf := buffers.NewBytesBuffer()
	buf.AppendByte(MetaPrefix_Column)
	buf.AppendUint8(i.DatabaseID)
	buf.AppendUint8(i.SchemaID)
	buf.AppendUint8(i.TableID)
	buf.AppendString(i.ColumnName)
	return buf.Bytes()
}

func (i Column) Path() []byte {
	buf := buffers.NewBytesBuffer()
	buf.AppendByte(MetaPrefix_Column)
	buf.AppendUint8(i.DatabaseID)
	buf.AppendUint8(i.SchemaID)
	buf.AppendUint8(i.TableID)
	buf.AppendString(i.ColumnName)
	buf.AppendUint8(i.ColumnType)
	return buf.Bytes()
}
