package distribution

import (
	"encoding/binary"
	"fmt"
	"github.com/ahmetb/go-linq/v3"
	"github.com/dgraph-io/badger"
	"github.com/elliotcourant/arkdb/pkg/network"
	"github.com/elliotcourant/arkdb/pkg/storage"
	"github.com/elliotcourant/arkdb/pkg/transport"
	"github.com/elliotcourant/timber"
	"github.com/hashicorp/raft"
	"os"
	"sort"
	"time"
)

var (
	ErrNotFound = fmt.Errorf("not found")
)

type NodeID uint64

func (n NodeID) RaftID() raft.ServerID {
	return raft.ServerID(fmt.Sprintf("%d", n))
}

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
	raft    *raft.Raft
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
	nodeId, newNode, err := r.determineNodeId()
	if err != nil {
		return err
	}

	r.logger = r.logger.Prefix(fmt.Sprintf("%d", nodeId))

	r.logger.Info("starting node")

	raftTransport := raft.NewNetworkTransportWithConfig(&raft.NetworkTransportConfig{
		Stream:  r.options.Transport,
		MaxPool: 4,
		Timeout: time.Second * 5,
	})

	notifyChannel := make(chan bool, 0)

	config := &raft.Config{
		ProtocolVersion:    raft.ProtocolVersionMax,
		HeartbeatTimeout:   time.Millisecond * 500,
		ElectionTimeout:    time.Millisecond * 500,
		CommitTimeout:      time.Millisecond * 500,
		MaxAppendEntries:   64,
		ShutdownOnRemove:   true,
		TrailingLogs:       128,
		SnapshotInterval:   time.Minute * 5,
		SnapshotThreshold:  512,
		LeaderLeaseTimeout: time.Second * 5,
		LocalID:            nodeId.RaftID(),
		NotifyCh:           notifyChannel,
	}

	snapshots, err := raft.NewFileSnapshotStore(r.options.Directory, 8, os.Stderr)
	if err != nil {
		return fmt.Errorf("could not create snapshot store: %v", err)
	}

	stableStore := r.stableStore()

	raftLog, err := raft.NewLogCache(64, r.logStore())
	if err != nil {
		return fmt.Errorf("could not create raft log store: %v", err)
	}

	rft, err := raft.NewRaft(
		config,
		r.fsmStore(),
		raftLog,
		stableStore,
		snapshots,
		raftTransport)
	if err != nil {
		return err
	}

	if len(r.options.Peers) == 1 && newNode {
		r.logger.Infof("bootstrapping")
		configuration := raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      config.LocalID,
					Address: raftTransport.LocalAddr(),
				},
			},
		}
		rft.BootstrapCluster(configuration)
	}

	r.raft = rft
	return nil
}

func (r *Raft) stableStore() raft.StableStore {
	return &raftStableStore{
		db: r.db,
	}
}

func (r *Raft) logStore() raft.LogStore {
	return &raftLogStore{
		db: r.db,
	}
}

func (r *Raft) fsmStore() raft.FSM {
	return &raftFsmStore{
		db: r.db,
	}
}

func (r *Raft) determineNodeId() (id NodeID, newNode bool, err error) {
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
		return 0, true, err
	}

	if len(val) == 8 {
		id = NodeID(binary.BigEndian.Uint64(val))
		return id, false, nil
	}

	myParsedAddress, err := network.ResolveAddress(r.options.ListenAddress)
	if err != nil {
		return 0, true, err
	}
	peers := make([]string, len(r.options.Peers))
	for i, peer := range r.options.Peers {
		addr, err := network.ResolveAddress(peer)
		if err != nil {
			return 0, true, err
		}
		peers[i] = addr
	}
	peers = append(peers, myParsedAddress)

	linq.From(peers).Distinct().ToSlice(&peers)

	sort.Strings(peers)

	id = NodeID(linq.From(peers).IndexOf(func(i interface{}) bool {
		addr, ok := i.(string)
		return ok && addr == myParsedAddress
	}) + 1)
	return id, true, nil
}
