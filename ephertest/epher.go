package ephertest

import (
	"math/rand"
	"testing"

	"github.com/aybabtme/epher/merkle"
	"github.com/aybabtme/epher/service"
)

func Start(t *testing.T, n int, r *rand.Rand) (stores []merkle.Store, done func()) {
	sd := ServiceDiscovery(t)

	rc, err := sd.Discover()
	if err != nil {
		t.Fatal(err)
	}

	var svcs []service.Svc
	// create N-1 nodes
	for i := 0; i < n; i++ {
		svc := startService(t, r, rc, startStore(t))
		svcs = append(svcs, svc)
		stores = append(stores, svc.Store())
	}

	return stores, func() {
		for _, svc := range svcs {
			if err := svc.Close(); err != nil {
				t.Log(err)
			}
		}
	}
}
