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

const (
	leaderWaitDelay   = 50 * time.Millisecond
	leaderWaitTimeout = 500 * time.Millisecond
	applyTimeout      = 1000 * time.Millisecond
)

var (
	ErrNotFound = fmt.Errorf("not found")
)

type nodeId uint64

func (n nodeId) RaftID() raft.ServerID {
	return raft.ServerID(fmt.Sprintf("%d", n))
}

type Options struct {
	Directory       string
	ListenAddress   string
	Peers           []string
	Join            bool
	Transport       transport.Transport
	LeaderWaitDelay time.Duration
}

type boat struct {
	db      *badger.DB
	options *Options
	logger  timber.Logger
	raft    *raft.Raft
}

func NewDistributor(options *Options, logger timber.Logger) (Barge, error) {
	dbOptions := badger.DefaultOptions(options.Directory)
	db, err := badger.OpenManaged(dbOptions)
	if err != nil {
		return nil, err
	}
	return &boat{
		options: options,
		db:      db,
		logger:  logger,
	}, nil
}

func (r *boat) Start() error {
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

	if len(r.options.Peers) == 1 && newNode && !r.options.Join {
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
	r.logger.Info("raft started")
	r.raft = rft
	return nil
}

func (r *boat) WaitForLeader(timeout time.Duration) (string, bool, error) {
	address, err := func(timeout time.Duration) (string, error) {
		l := string(r.raft.Leader())
		if len(l) > 0 {
			return l, nil
		}

		ticker := time.NewTicker(leaderWaitDelay)
		defer ticker.Stop()
		timer := time.NewTimer(timeout)
		defer timer.Stop()
		for {
			select {
			case <-ticker.C:
				l := string(r.raft.Leader())
				if len(l) > 0 {
					return l, nil
				}
			case <-timer.C:
				return "", fmt.Errorf("wait for leader timeout expired")
			}
		}
	}(timeout)
	if err != nil {
		return "", false, err
	}
	address, err = network.ResolveAddress(address)
	return address, true, err
}

func (r *boat) IsLeader() bool {
	_, ok, _ := r.WaitForLeader(leaderWaitTimeout)
	return ok && r.raft.State() == raft.Leader
}

func (r *boat) waitForAmILeader(timeout time.Duration) (string, bool, error) {
	addr, ok, err := r.WaitForLeader(timeout)
	if !ok || err != nil {
		return "", false, fmt.Errorf("failed to wait to find out if this is the leader: %v", err)
	}
	return addr, r.raft.State() == raft.Leader, nil
}

func (r *boat) apply(tx storage.Transaction) error {
	leaderAddress, amLeader, err := r.waitForAmILeader(leaderWaitTimeout)
	if err != nil {
		return fmt.Errorf("could not apply transaction: %v", err)
	}
	// If I am not the leader then we need to forward this transaction to the leader.
	if !amLeader {

		return nil
	}

	applyFuture := r.raft.Apply(tx.Encode(), applyTimeout)
	if err := applyFuture.Error(); err != nil {
		return err
	}
	response := applyFuture.Response()
	r.logger.Debugf("apply response: (%T) %v", response, response)
	return nil
}

func (r *boat) stableStore() raft.StableStore {
	return &raftStableStore{
		db: r.db,
	}
}

func (r *boat) logStore() raft.LogStore {
	return &raftLogStore{
		db: r.db,
	}
}

func (r *boat) fsmStore() raft.FSM {
	return &raftFsmStore{
		db: r.db,
	}
}

func (r *boat) determineNodeId() (id nodeId, newNode bool, err error) {
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
		id = nodeId(binary.BigEndian.Uint64(val))
		return id, false, nil
	}

	// If we are joining a cluster we should reach out to each one of
	// the peers provided and try to see what state it is in. If we
	// find a peer that is part of a cluster then we want to get its'
	// leader and do a join request. But if we find two different
	// leader addresses in the discovery phase then we want to panic
	// because that indicates a split-brain scenario or that there
	// are multiple established clusters currently active and we
	// cannot determine which cluster we should join.
	if r.options.Join {

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

	id = nodeId(linq.From(peers).IndexOf(func(i interface{}) bool {
		addr, ok := i.(string)
		return ok && addr == myParsedAddress
	}) + 1)
	return id, true, nil
}
