package store

import (
	"context"
	"time"

	"github.com/aybabtme/epher/merkle"
	"github.com/aybabtme/epher/thash"
	"github.com/eapache/go-resiliency/breaker"
)

func CircuitBreak(primary, fallback merkle.Store) merkle.Store {
	return &circuitBreak{
		primary:  primary,
		fallback: fallback,
		breaker:  breaker.New(3, 1, 5*time.Second),
	}
}

type circuitBreak struct {
	breaker  *breaker.Breaker
	primary  merkle.Store
	fallback merkle.Store
}

func (cb *circuitBreak) pick(ctx context.Context, fn func(ctx context.Context, store merkle.Store) error) error {
	err := cb.breaker.Run(func() error {
		return fn(ctx, cb.primary)
	})
	switch err {
	case breaker.ErrBreakerOpen:
		return fn(ctx, cb.fallback)
	default:
		return err
	}
}

func (cb *circuitBreak) PutNode(ctx context.Context, node merkle.Node) error {
	return cb.pick(ctx, func(ctx context.Context, store merkle.Store) error {
		return store.PutNode(ctx, node)
	})
}

func (cb *circuitBreak) GetNode(ctx context.Context, sum thash.Sum) (merkle.Node, bool, error) {
	var (
		node  merkle.Node
		found bool
	)
	err := cb.pick(ctx, func(ctx context.Context, store merkle.Store) error {
		var err error
		node, found, err = store.GetNode(ctx, sum)
		return err
	})
	return node, found, err
}

func (cb *circuitBreak) PutBlob(ctx context.Context, sum thash.Sum, data []byte) error {
	return cb.pick(ctx, func(ctx context.Context, store merkle.Store) error {
		return store.PutBlob(ctx, sum, data)
	})
}

func (cb *circuitBreak) GetBlob(ctx context.Context, sum thash.Sum) ([]byte, bool, error) {
	var (
		data  []byte
		found bool
	)
	err := cb.pick(ctx, func(ctx context.Context, store merkle.Store) error {
		var err error
		data, found, err = store.GetBlob(ctx, sum)
		return err
	})
	return data, found, err
}

func (cb *circuitBreak) InfoBlob(ctx context.Context, sum thash.Sum) (merkle.BlobInfo, bool, error) {
	var (
		info  merkle.BlobInfo
		found bool
	)
	err := cb.pick(ctx, func(ctx context.Context, store merkle.Store) error {
		var err error
		info, found, err = store.InfoBlob(ctx, sum)
		return err
	})
	return info, found, err
}
