package service

import (
	"math/rand"
	"net"
	"net/http"

	"github.com/aybabtme/epher/cluster"
	"github.com/aybabtme/epher/codec"
	"github.com/aybabtme/epher/merkle"
	"github.com/aybabtme/epher/store"
	"github.com/aybabtme/log"
)

type Svc interface {
	Store() merkle.Store
	Close() error
}

type Dialer func(cluster.Node) merkle.Store

func Start(r *rand.Rand, rc cluster.RemoteCluster, codec codec.Codec, local merkle.Store, dialFn Dialer) (Svc, error) {

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

	pool := store.ClusterPool(lc, dialFn)
	local = store.Log(log.KV("store", "local"), local)

	aggregate := store.Log(
		log.KV("store", "singleflight"),
		store.SingleFlight(
			store.Log(
				log.KV("store", "layer"),
				store.Layer(
					// first ping our local store
					local,
					// then ping a few people
					store.Log(
						log.KV("store", "race-few"),
						store.Race(
							store.RaceRandomOf(r, store.GrowthLog2, 3),
							pool,
						),
					),
					// then ping a lot more people!
					store.Log(
						log.KV("store", "race-many"),
						store.Race(
							store.RaceRandomOf(r, store.GrowthLog2Square, 9),
							pool,
						),
					),
				),
			),
		),
	)

	svc := &service{
		local:   aggregate,
		cluster: lc,
		srv: &http.Server{
			Handler: store.HTTPServer(
				codec,
				aggregate,
			),
		},
		l: l,
	}

	go svc.srv.Serve(l)

	return svc, nil
}

type service struct {
	local   merkle.Store
	cluster cluster.Cluster
	srv     *http.Server

	l net.Listener
}

func (svc *service) Store() merkle.Store { return svc.local }

func (svc *service) Close() error {
	if svc.cluster == nil {
		panic("ugh")
	}
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
