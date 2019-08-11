package distribution

import (
	"fmt"
	"github.com/dgraph-io/badger"
	"github.com/elliotcourant/arkdb/pkg/storage"
	"github.com/hashicorp/raft"
	"sync"
	"time"
)

var (
	ErrBargeStopped = fmt.Errorf("barge has been stopped")
)

type Encoder interface {
	Encode() []byte
}

type Decoder interface {
	Decode(src []byte) error
}

type Barge interface {
	Start() error
	WaitForLeader(timeout time.Duration) (string, bool, error)
	IsLeader() bool
	Begin() (Transaction, error)
	Stop() error
	IsStopped() bool
	NodeID() raft.ServerID
}

func (r *boat) Begin() (Transaction, error) {
	if r.IsStopped() {
		return nil, ErrBargeStopped
	}
	return &transaction{
		txn:           r.db.NewTransaction(true),
		boat:          r,
		pendingWrites: map[string][]byte{},
	}, nil
}

type Transaction interface {
	Get(key []byte, value Decoder) error
	Set(key []byte, value Encoder) error

	Rollback() error
	Commit() error
}

type transaction struct {
	closed            bool
	closedSync        sync.RWMutex
	boat              *boat
	txn               *badger.Txn
	pendingWrites     map[string][]byte
	pendingWritesSync sync.RWMutex
}

func (t *transaction) Get(key []byte, value Decoder) error {
	if t.isFinished() {
		return fmt.Errorf("transaction closed")
	}
	item, err := t.txn.Get(key)
	if err != nil {
		return err
	}
	val := make([]byte, item.ValueSize())
	val, err = item.ValueCopy(val)
	if err != nil {
		return err
	}
	return value.Decode(val)
}

func (t *transaction) Set(key []byte, value Encoder) error {
	if t.isFinished() {
		return fmt.Errorf("transaction closed")
	}
	val := value.Encode()
	if err := t.txn.Set(key, val); err != nil {
		return err
	}
	t.addPendingWrite(key, val)
	return nil
}

func (t *transaction) Rollback() error {
	if t.isFinished() {
		return fmt.Errorf("transaction closed")
	}
	defer t.finishTransaction()
	t.txn.Discard()
	return nil
}

func (t *transaction) Commit() error {
	if t.boat.IsStopped() {
		return ErrBargeStopped
	}
	startTime := time.Now()
	defer t.boat.logger.Verbosef("time to commit: %s", time.Since(startTime))
	if t.isFinished() {
		return fmt.Errorf("transaction closed")
	}
	defer t.finishTransaction()
	if t.getNumberOfPendingWrites() == 0 {
		t.txn.Discard()
		return nil
	}
	rtx := storage.Transaction{
		Timestamp: uint64(time.Now().UTC().UnixNano()),
		Actions:   make([]storage.Action, 0),
	}
	t.pendingWritesSync.RLock()
	defer t.pendingWritesSync.RUnlock()
	t.boat.logger.Verbosef("preparing to commit %d pending write(s)", len(t.pendingWrites))
	for k, v := range t.pendingWrites {
		actionType := storage.ActionTypeSet
		if v == nil {
			actionType = storage.ActionTypeDelete
		}
		rtx.Actions = append(rtx.Actions, storage.Action{
			Type:  actionType,
			Key:   []byte(k),
			Value: v,
		})
	}
	return t.boat.apply(rtx)
}

func (t *transaction) addPendingWrite(key []byte, value []byte) {
	t.pendingWritesSync.Lock()
	defer t.pendingWritesSync.Unlock()
	t.pendingWrites[string(key)] = value
}

func (t *transaction) getNumberOfPendingWrites() uint32 {
	t.pendingWritesSync.RLock()
	defer t.pendingWritesSync.RUnlock()
	return uint32(len(t.pendingWrites))
}

func (t *transaction) finishTransaction() {
	t.closedSync.Lock()
	defer t.closedSync.Unlock()
	t.closed = true
}

func (t *transaction) isFinished() bool {
	t.closedSync.RLock()
	defer t.closedSync.RUnlock()
	return t.closed
}
