package storage

import (
	"github.com/elliotcourant/arkdb/pkg/buffers"
)

type Table struct {
	TableID    uint8
	DatabaseID uint8
	SchemaID   uint8
	TableName  string
}

func (i Table) Path() []byte {
	l := len(i.TableName)
	buf := buffers.NewAllocatedBytesBuffer(5 + l)
	buf.Append(MetaPrefix_Table, i.DatabaseID, i.SchemaID, uint8(l))
	buf.AppendString(i.TableName)
	buf.Append(i.TableID)
	return buf.Bytes()
}

func TablesByNamePrefix(databaseId, schemaId uint8, tableName string) []byte {
	l := len(tableName)
	buf := buffers.NewAllocatedBytesBuffer(4 + l)
	buf.Append(MetaPrefix_Table, databaseId, schemaId, uint8(l))
	buf.AppendString(tableName)
	return buf.Bytes()
}
