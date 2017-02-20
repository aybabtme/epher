package store

import (
	"context"
	"math/rand"
	"sync/atomic"

	"github.com/aybabtme/epher/merkle"
	"github.com/aybabtme/epher/thash"
)

type LBStrategy func([]merkle.Store) merkle.Store

func LBRandom(r *rand.Rand) LBStrategy {
	return func(in []merkle.Store) merkle.Store {
		return in[r.Intn(len(in))]
	}
}

func LBRoundRobin(r *rand.Rand) LBStrategy {
	idx := r.Int63n(1e6)
	return func(in []merkle.Store) merkle.Store {
		i := atomic.AddInt64(&idx, 1)
		return in[int(i)%len(in)]
	}
}

func LB(strategy LBStrategy, pool ...merkle.Store) merkle.Store {
	return &loadBalance{strategy: strategy, pool: pool}
}

type loadBalance struct {
	strategy LBStrategy
	pool     []merkle.Store
}

func (lb *loadBalance) pick(ctx context.Context, fn func(ctx context.Context, store merkle.Store) bool) bool {
	for {
		done := fn(ctx, lb.strategy(lb.pool))
		if done {
			return true
		}
	}
	return false
}

func (lb *loadBalance) PutNode(ctx context.Context, node merkle.Node) error {
	var err error
	_ = lb.pick(ctx, func(ctx context.Context, store merkle.Store) bool {
		err = store.PutNode(ctx, node)
		return err == nil
	})
	return err
}

func (lb *loadBalance) GetNode(ctx context.Context, sum thash.Sum) (merkle.Node, bool, error) {
	var (
		node merkle.Node
		err  error
	)
	done := lb.pick(ctx, func(ctx context.Context, store merkle.Store) bool {
		var found bool
		node, found, err = store.GetNode(ctx, sum)
		return found && err == nil
	})
	return node, done, err
}

func (lb *loadBalance) PutBlob(ctx context.Context, sum thash.Sum, data []byte) error {
	var err error
	_ = lb.pick(ctx, func(ctx context.Context, store merkle.Store) bool {
		err = store.PutBlob(ctx, sum, data)
		return err == nil
	})
	return err
}

func (lb *loadBalance) GetBlob(ctx context.Context, sum thash.Sum) ([]byte, bool, error) {
	var (
		data []byte
		err  error
	)
	found := lb.pick(ctx, func(ctx context.Context, store merkle.Store) bool {
		var found bool
		data, found, err = store.GetBlob(ctx, sum)
		return found && err == nil
	})
	return data, found, err
}

func (lb *loadBalance) InfoBlob(ctx context.Context, sum thash.Sum) (merkle.BlobInfo, bool, error) {
	var (
		info merkle.BlobInfo
		err  error
	)
	found := lb.pick(ctx, func(ctx context.Context, store merkle.Store) bool {
		var found bool
		info, found, err = store.InfoBlob(ctx, sum)
		return found && err == nil
	})
	return info, found, err
}
