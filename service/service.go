package service

import (
	"context"
	"io"
	"net"

	"github.com/aybabtme/epher/cluster"
	"github.com/aybabtme/epher/merkle"
	"github.com/aybabtme/epher/thash"
)

func Start(sd cluster.Discovery, store merkle.Store) error {

	// create a listeners for our RPC
	l, err := net.Listen("tcp", sd.Self().Addr.String()+":0")
	if err != nil {
		return err
	}

	svc := &service{
		sd:    sd,
		store: store,
		l:     l,
	}

	return svc.run()
}

type service struct {
	sd    cluster.Discovery
	store merkle.Store

	l net.Listener
}

func (svc *service) run() error {

	// tell people where to reach us
	return nil
}

func (svc *service) GetBlob(ctx context.Context, sum thash.Sum, w io.Writer) error {
	tree, err := merkle.RetrieveTree(ctx, sum, svc.store)
	if err != nil {
		return err
	}
	invalid, err := tree.Retrieve(ctx, w, svc.store)

	go svc.repairBlob(invalid)

	return err
}

func (svc *service) repairBlob(invalidNodes []*merkle.Tree) {
	// TODO: fetch the repaired blobs
}
