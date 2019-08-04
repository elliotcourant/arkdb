package distribution

import (
	"encoding/binary"
	"github.com/ahmetb/go-linq/v3"
	"github.com/dgraph-io/badger"
	"github.com/elliotcourant/arkdb/pkg/network"
	"github.com/elliotcourant/arkdb/pkg/storage"
	"github.com/elliotcourant/arkdb/pkg/transport"
	"github.com/elliotcourant/timber"
	"sort"
)

type Options struct {
	Directory     string
	ListenAddress string
	Peers         []string
	EnableSingle  bool
	Transport     transport.Transport
}

type Raft struct {
	db      *badger.DB
	options *Options
	logger  timber.Logger
}

func NewDistributor(options *Options, logger timber.Logger) (*Raft, error) {
	dbOptions := badger.DefaultOptions(options.Directory)
	db, err := badger.OpenManaged(dbOptions)
	if err != nil {
		return nil, err
	}
	return &Raft{
		options: options,
		db:      db,
		logger:  logger,
	}, nil
}

func (r *Raft) Start() error {
	return nil
	// r.logger.Debugf("")
}

func (r *Raft) determineNodeId() (uint64, error) {
	var val []byte
	if err := r.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(storage.GetMyNodeIdPath())
		if err == badger.ErrKeyNotFound {
			val = make([]byte, 0)
			return nil
		} else if err != nil {
			return err
		}
		_, err = item.ValueCopy(val)
		return err
	}); err != nil {
		return 0, err
	}

	if len(val) == 8 {
		id := binary.BigEndian.Uint64(val)
		return id, nil
	}

	myParsedAddress, err := network.ResolveAddress(r.options.ListenAddress)
	if err != nil {
		return 0, err
	}
	peers := make([]string, len(r.options.Peers))
	for i, peer := range r.options.Peers {
		addr, err := network.ResolveAddress(peer)
		if err != nil {
			return 0, err
		}
		peers[i] = addr
	}
	peers = append(peers, myParsedAddress)

	linq.From(peers).Distinct().ToSlice(&peers)

	sort.Strings(peers)

	id := linq.From(peers).IndexOf(func(i interface{}) bool {
		addr, ok := i.(string)
		return ok && addr == myParsedAddress
	}) + 1
	return uint64(id), nil
}
