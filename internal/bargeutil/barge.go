package bargeutil

import (
	"github.com/elliotcourant/arkdb/internal/testutils"
	"github.com/elliotcourant/arkdb/pkg/distribution"
	"github.com/elliotcourant/arkdb/pkg/transport"
	"github.com/elliotcourant/timber"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func NewBarge(t *testing.T) (distribution.Barge, func()) {
	tempDir, cleanup := testutils.NewTempDirectory(t)

	ln, err := transport.NewTransport(":")
	assert.NoError(t, err)

	d, err := distribution.NewDistributor(ln, &distribution.Options{
		Directory:     tempDir,
		ListenAddress: ln.Addr().String(),
		Peers:         []string{ln.Addr().String()},
		Join:          false,
	}, timber.With(timber.Keys{
		"test": t.Name(),
	}))
	if !assert.NoError(t, err) {
		panic(err)
	}
	assert.NotNil(t, d)

	err = d.Start()
	if !assert.NoError(t, err) {
		panic(err)
	}

	d.WaitForLeader(time.Second * 5)
	return d, func() {
		defer cleanup()
		d.Stop()
	}
}
