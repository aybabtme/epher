package service

import (
	"math/rand"
	"net"
	"net/http"

	"github.com/aybabtme/epher/cluster"
	"github.com/aybabtme/epher/codec"
	"github.com/aybabtme/epher/merkle"
	"github.com/aybabtme/epher/store"
)

type Svc interface {
	Close() error
}

func Start(r *rand.Rand, rc cluster.RemoteCluster, codec codec.Codec, local merkle.Store) (Svc, error) {

	var l net.Listener
	lc, err := rc.Join(func(ip string) (net.Addr, error) {
		var err error
		l, err = net.Listen("tcp", ip+":0")
		if err != nil {
			return nil, err
		}
		return l.Addr(), nil
	})
	if err != nil {
		return nil, err
	}


	// we want to serve from our local store if we can
	// otherwise we'll do a random search with our neighbours
	// TODO: use informed search instead of random search

	aggregate := store.Layer(
		// first ping our local store
		local,
		// then ping a few people
		store.Race(
			store.RaceRandomOf(r, store.GrowthLog2, 3),
			pool,
		),
		// then ping a lot more people!
		store.Race(
			store.RaceRandomOf(r, store.GrowthLog2Square, 9),
			pool,
		),
	)

	aggregate = store.SingleFlight(aggregate)

	svc := &service{
		srv: &http.Server{
			Handler: store.HTTPServer(
				codec,
				store.SingleFlight(local),
			),
		},
		l: l,
	}

	go svc.srv.Serve(l)

	return svc, nil
}

type service struct {
	cluster cluster.Cluster
	srv     *http.Server

	l net.Listener
}

func (svc *service) Close() error {
	if err := svc.cluster.Leave(); err != nil {
		_ = svc.l.Close()
		return err
	}
	if err := svc.srv.Close(); err != nil {
		_ = svc.l.Close()
		return err
	}
	return svc.l.Close()
}
