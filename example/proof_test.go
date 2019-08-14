package example_test

import (
	"fmt"
	"github.com/elliotcourant/arkdb/internal/testutils"
	"github.com/stretchr/testify/assert"
	"testing"
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
