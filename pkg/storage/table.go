package storage

import (
	"github.com/elliotcourant/buffers"
)

type Table struct {
	TableID    uint8
	DatabaseID uint8
	SchemaID   uint8
	TableName  string
}

func (i Table) Encode() []byte {
	buf := buffers.NewBytesBuffer()
	buf.AppendUint8(i.TableID)
	buf.AppendUint8(i.DatabaseID)
	buf.AppendUint8(i.SchemaID)
	buf.AppendString(i.TableName)
	return buf.Bytes()
}

func (i *Table) Decode(src []byte) error {
	*i = Table{}
	buf := buffers.NewBytesReader(src)
	i.TableID = buf.NextUint8()
	i.DatabaseID = buf.NextUint8()
	i.SchemaID = buf.NextUint8()
	i.TableName = buf.NextString()
	return nil
}

func (i Table) Path() []byte {
	buf := buffers.NewBytesBuffer()
	buf.AppendByte(MetaPrefix_Table)
	buf.AppendUint8(i.DatabaseID)
	buf.AppendUint8(i.SchemaID)
	buf.AppendString(i.TableName)
	buf.AppendUint8(i.TableID)
	return buf.Bytes()
}

func TablesByNamePrefix(databaseId, schemaId uint8, tableName string) []byte {
	buf := buffers.NewBytesBuffer()
	buf.AppendByte(MetaPrefix_Table)
	buf.AppendUint8(databaseId)
	buf.AppendUint8(schemaId)
	buf.AppendString(tableName)
	return buf.Bytes()
}
