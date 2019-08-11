package distribution

import (
	"github.com/elliotcourant/arkdb/internal/testutils"
	"github.com/elliotcourant/arkdb/pkg/transport"
	"github.com/elliotcourant/timber"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestNewDistributor(t *testing.T) {
	t.Run("single", func(t *testing.T) {
		tempDir, cleanup := testutils.NewTempDirectory(t)

		ln, err := transport.NewTransport(":")
		assert.NoError(t, err)

		defer cleanup()
		d, err := NewDistributor(&Options{
			Directory:     tempDir,
			ListenAddress: ln.Addr().String(),
			Peers:         []string{ln.Addr().String()},
			Join:          false,
			Transport:     ln,
		}, timber.With(timber.Keys{
			"test": t.Name(),
		}))
		assert.NoError(t, err)
		assert.NotNil(t, d)

		err = d.Start()
		assert.NoError(t, err)

		time.Sleep(time.Second * 5)
	})

	t.Run("multiple", func(t *testing.T) {
		numberOfNodes := 9

		listeners := make([]transport.Transport, numberOfNodes)
		peers := make([]string, numberOfNodes)
		for i := range listeners {
			ln, err := transport.NewTransport(":")
			assert.NoError(t, err)
			listeners[i] = ln
			peers[i] = ln.Addr().String()
		}

		cleanups := make([]func(), numberOfNodes)
		nodes := make([]Barge, numberOfNodes)

		for i := 0; i < numberOfNodes; i++ {
			func() {
				tempDir, cleanup := testutils.NewTempDirectory(t)
				cleanups[i] = cleanup

				d, err := NewDistributor(&Options{
					Directory:     tempDir,
					ListenAddress: listeners[i].Addr().String(),
					Peers:         peers,
					Join:          false,
					Transport:     listeners[i],
				}, timber.With(timber.Keys{
					"test": t.Name(),
				}))
				assert.NoError(t, err)
				assert.NotNil(t, d)

				nodes[i] = d
			}()
		}

		defer func(cleanups []func()) {
			for _, cleanup := range cleanups {
				cleanup()
			}
		}(cleanups)

		timber.Debugf("created %d node(s), starting now", numberOfNodes)

		for _, node := range nodes {
			go func(node Barge) {
				err := node.Start()
				assert.NoError(t, err)
			}(node)
		}

		time.Sleep(5 * time.Second)
	})
}
