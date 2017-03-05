package ephertest

import (
	"math/rand"
	"testing"

	"github.com/aybabtme/epher/cluster"
	"github.com/aybabtme/epher/merkle"
	"github.com/aybabtme/epher/service"
)

func StartService(t *testing.T, r *rand.Rand, rc cluster.RemoteCluster, store merkle.Store) {

	service.Start(r, store)
}
