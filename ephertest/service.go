package ephertest

import (
	"math/rand"
	"net/http"
	"testing"

	"github.com/aybabtme/epher/cluster"
	"github.com/aybabtme/epher/codec"
	"github.com/aybabtme/epher/merkle"
	"github.com/aybabtme/epher/service"
	"github.com/aybabtme/epher/store"
)

func startService(t *testing.T, r *rand.Rand, rc cluster.RemoteCluster, st merkle.Store) service.Svc {
	cd := codec.Binary()
	svc, err := service.Start(r, rc, cd, st, func(nd cluster.Node) merkle.Store {
		return store.HTTPClient(nd.Addr, cd, &http.Client{})
	})
	if err != nil {
		t.Fatal(err)
	}
	return svc
}
