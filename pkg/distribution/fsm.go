package distribution

import (
	"bytes"
	"github.com/dgraph-io/badger"
	"github.com/elliotcourant/arkdb/pkg/storage"
	"github.com/hashicorp/raft"
	"io"
)

type raftFsmStore struct {
	db *badger.DB
}

func (r *raftFsmStore) Apply(log *raft.Log) interface{} {
	var transaction storage.Transaction
	if err := transaction.Decode(log.Data); err != nil {
		return err
	}
	err := r.db.Update(func(txn *badger.Txn) error {
		for _, action := range transaction.Actions {
			switch action.Type {
			case storage.ActionTypeSet:
				if err := txn.Set(action.Key, action.Value); err != nil {
					return err
				}
			case storage.ActionTypeDelete:
				if err := txn.Delete(action.Key); err != nil {
					return err
				}
			}
		}
		return nil
	})
	return err
}

func (r *raftFsmStore) Restore(rc io.ReadCloser) error {
	return r.db.Load(rc, 8)
}

func (r *raftFsmStore) Snapshot() (raft.FSMSnapshot, error) {
	w := bytes.NewBuffer(nil)
	if _, err := r.db.Backup(w, 0); err != nil {
		return nil, err
	}
	return &raftSnapshot{data: w.Bytes()}, nil
}
