package store

import (
	"context"

	"github.com/aybabtme/epher/merkle"
	"github.com/aybabtme/epher/thash"
	"github.com/aybabtme/log"
)

func Log(ll *log.Log, store merkle.Store) merkle.Store {
	return &intercept{
		around: func(ctx context.Context, method string, fn func(context.Context) error) error {
			ll := ll.KV("method", method)
			ll.Info("start")
			err := fn(ctx)
			if err != nil {
				ll.Err(err).Error("failed")
			} else {
				ll.Info("done")
			}
			return err
		},
		wrap: store,
	}
}

type intercept struct {
	around func(ctx context.Context, method string, fn func(context.Context) error) error
	wrap   merkle.Store
}

func (icept *intercept) PutNode(ctx context.Context, node merkle.Node) error {
	err := icept.around(ctx, "PutNode", func(ctx context.Context) error {
		err := icept.wrap.PutNode(ctx, node)
		return err
	})
	return err
}

func (icept *intercept) GetNode(ctx context.Context, sum thash.Sum) (nd merkle.Node, ok bool, err error) {
	err = icept.around(ctx, "GetNode", func(ctx context.Context) error {
		nd, ok, err = icept.wrap.GetNode(ctx, sum)
		return err
	})
	return nd, ok, err
}

func (icept *intercept) PutBlob(ctx context.Context, sum thash.Sum, data []byte) error {
	err := icept.around(ctx, "PutBlob", func(ctx context.Context) error {
		err := icept.wrap.PutBlob(ctx, sum, data)
		return err
	})
	return err
}

func (icept *intercept) GetBlob(ctx context.Context, sum thash.Sum) (blob []byte, ok bool, err error) {
	err = icept.around(ctx, "GetBlob", func(ctx context.Context) error {
		blob, ok, err = icept.wrap.GetBlob(ctx, sum)
		return err
	})
	return blob, ok, err
}

func (icept *intercept) InfoBlob(ctx context.Context, sum thash.Sum) (bi merkle.BlobInfo, ok bool, err error) {
	err = icept.around(ctx, "InfoBlob", func(ctx context.Context) error {
		bi, ok, err = icept.wrap.InfoBlob(ctx, sum)
		return err
	})
	return bi, ok, err
}
