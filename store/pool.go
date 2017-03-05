package store

import (
	"github.com/aybabtme/epher/cluster"
	"github.com/aybabtme/epher/merkle"
)

// A Pool returns merkle.Stores when asked for.
type Pool func() []merkle.Store

// ClusterPool is a pool of remote merkle.Store which are
// dynamically discovered and created with the dialer.
func ClusterPool(lc cluster.Cluster, dial func(cluster.Node) merkle.Store) Pool {
	return newStorePool(lc, func(nd cluster.Node) merkle.Store {
		return SingleFlight(dial(nd))
	})
}

func newStorePool(
	rc cluster.Cluster,
	dial func(cluster.Node) merkle.Store,
) Pool {
	return func() []merkle.Store {
		self := rc.Self()
		nodes := rc.Members()
		stores := make([]merkle.Store, 0, len(nodes)-1)
		for _, nd := range nodes {
			if nd == self {
				continue
			}
			stores = append(stores, dial(nd))
		}
		return stores
	}
}
