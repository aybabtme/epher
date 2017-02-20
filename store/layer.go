package store

import (
	"context"

	"github.com/aybabtme/epher/merkle"
	"github.com/aybabtme/epher/thash"
)

func Layer(inOrder ...merkle.Store) merkle.Store {
	return &layered{inOrder: inOrder}
}

type layered struct {
	inOrder []merkle.Store
}

func (ly *layered) cascade(ctx context.Context, fn func(ctx context.Context, store merkle.Store) bool) bool {
	for _, st := range ly.inOrder {
		done := fn(ctx, st)
		if done {
			return true
		}
	}
	return false
}

func (ly *layered) PutNode(ctx context.Context, node merkle.Node) error {
	var err error
	_ = ly.cascade(ctx, func(ctx context.Context, store merkle.Store) bool {
		err = store.PutNode(ctx, node)
		return err == nil
	})
	return err
}

func (ly *layered) GetNode(ctx context.Context, sum thash.Sum) (merkle.Node, bool, error) {
	var (
		node merkle.Node
		err  error
	)
	done := ly.cascade(ctx, func(ctx context.Context, store merkle.Store) bool {
		var found bool
		node, found, err = store.GetNode(ctx, sum)
		return found && err == nil
	})
	return node, done, err
}

func (ly *layered) PutBlob(ctx context.Context, sum thash.Sum, data []byte) error {
	var err error
	_ = ly.cascade(ctx, func(ctx context.Context, store merkle.Store) bool {
		err = store.PutBlob(ctx, sum, data)
		return err == nil
	})
	return err
}

func (ly *layered) GetBlob(ctx context.Context, sum thash.Sum) ([]byte, bool, error) {
	var (
		data []byte
		err  error
	)
	found := ly.cascade(ctx, func(ctx context.Context, store merkle.Store) bool {
		var found bool
		data, found, err = store.GetBlob(ctx, sum)
		return found && err == nil
	})
	return data, found, err
}

func (ly *layered) InfoBlob(ctx context.Context, sum thash.Sum) (merkle.BlobInfo, bool, error) {
	var (
		info merkle.BlobInfo
		err  error
	)
	found := ly.cascade(ctx, func(ctx context.Context, store merkle.Store) bool {
		var found bool
		info, found, err = store.InfoBlob(ctx, sum)
		return found && err == nil
	})
	return info, found, err
}
