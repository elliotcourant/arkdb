package arkdb

import (
	"github.com/elliotcourant/arkdb/pkg/transport"
	"github.com/elliotcourant/timber"
)

type Options struct {
	Directory    string
	Peers        []string
	EnableSingle bool
	Transport    transport.Transport
}

type DB interface {
	Start() error
}

func NewDB(options *Options) DB {
	return &store{
		options: options,
		logger:  timber.New(),
	}
}

type store struct {
	options *Options
	logger  timber.Logger
}

func (db *store) Start() error {

	return nil
}
