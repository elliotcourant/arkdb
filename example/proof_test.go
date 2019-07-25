package example__test

import (
	"encoding/hex"
	"fmt"
	"github.com/dgraph-io/badger"
	"github.com/elliotcourant/arkdb/internal/testutils"
	"github.com/elliotcourant/arkdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func BenchmarkBadgerMassiveInsert(b *testing.B) {
	db, cleanup := testutils.NewDB(b)
	defer cleanup()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tx := db.NewTransaction(true)
		if err := tx.Set(
			[]byte(fmt.Sprintf("this would end up being a very large path %d", i)),
			[]byte(fmt.Sprintf("asghjsakjgdasgds"))); !assert.NoError(b, err) {
			panic(err)
		}
		tx.Commit()
	}
}

func TestBadgerScenario(t *testing.T) {
	db, cleanup := testutils.NewDB(t)
	defer cleanup()

	tx := db.NewTransaction(true)
	_ = tx.Set(storage.Database{
		DatabaseID:   1,
		DatabaseName: "ark",
	}.Path(), []byte{})

	_ = tx.Set(storage.Schema{
		DatabaseID: 1,
		SchemaID:   1,
		SchemaName: "public",
	}.Path(), []byte{})

	for i := 0; i < 100; i++ {
		_ = tx.Set(storage.Table{
			DatabaseID: 1,
			SchemaID:   1,
			TableID:    uint8(i + 1),
			TableName:  fmt.Sprintf("accounts_%d", i),
		}.Path(), []byte{})
	}

	_ = tx.Set(storage.Column{
		DatabaseID: 1,
		SchemaID:   1,
		TableID:    1,
		ColumnID:   1,
		ColumnName: "account_id",
		ColumnType: 105,
	}.Path(), []byte{})

	_ = tx.Set(storage.Column{
		DatabaseID: 1,
		SchemaID:   1,
		TableID:    1,
		ColumnID:   2,
		ColumnName: "name",
		ColumnType: 25,
	}.Path(), []byte{})

	_ = tx.Commit()

	tableSearch := storage.TablesByNamePrefix(1, 1, "accounts_48")
	fmt.Println("performing search on path:")
	fmt.Println(hex.Dump(tableSearch))
	_ = db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.IteratorOptions{
			PrefetchValues: false,
			Reverse:        false,
			AllVersions:    false,
			Prefix:         tableSearch,
		})
		defer it.Close()
		startTime := time.Now()
		tableId := func(it *badger.Iterator) uint8 {
			defer func() {
				fmt.Println("search took:", time.Now().Sub(startTime))
			}()
			for it.Rewind(); it.Valid(); it.Next() {
				return it.Item().Key()[len(tableSearch)]
			}
			return 0
		}(it)
		assert.Equal(t, uint8(49), tableId)
		return nil
	})
}
