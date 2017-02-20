package store

import (
	"context"

	"sync"

	"github.com/aybabtme/epher/merkle"
	"github.com/aybabtme/epher/thash"
)

// Race makes multiple stores race together. The first answer to any
// query is returned, while the other ones are cancelled.
func Race(
	selection func([]merkle.Store) []merkle.Store,
	concurrent ...merkle.Store,
) merkle.Store {
	return &raced{selection: selection, concurrent: concurrent}
}

type raced struct {
	selection  func([]merkle.Store) []merkle.Store
	concurrent []merkle.Store
}

func (race *raced) PutNode(ctx context.Context, node merkle.Node) error {
	_, err := race.first(ctx, func(ctx context.Context, store merkle.Store) (interface{}, error) {
		return nil, store.PutNode(ctx, node)
	})
	return err
}

func (race *raced) GetNode(ctx context.Context, sum thash.Sum) (merkle.Node, bool, error) {
	type res struct {
		node  merkle.Node
		found bool
	}
	iface, err := race.first(ctx, func(ctx context.Context, store merkle.Store) (interface{}, error) {
		node, found, err := store.GetNode(ctx, sum)
		return &res{node: node, found: found}, err
	})
	out := iface.(*res)
	return out.node, out.found, err

}

func (race *raced) InfoBlob(ctx context.Context, sum thash.Sum) (int64, bool, error) {
	type res struct {
		size  int64
		found bool
	}
	iface, err := race.first(ctx, func(ctx context.Context, store merkle.Store) (interface{}, error) {
		size, found, err := store.InfoBlob(ctx, sum)
		return &res{size: size, found: found}, err
	})
	out := iface.(*res)
	return out.size, out.found, err
}

func (race *raced) PutBlob(ctx context.Context, sum thash.Sum, blob []byte) error {
	_, err := race.first(ctx, func(ctx context.Context, store merkle.Store) (interface{}, error) {
		return nil, store.PutBlob(ctx, sum, blob)
	})
	return err
}

func (race *raced) GetBlob(ctx context.Context, sum thash.Sum) ([]byte, bool, error) {
	type res struct {
		data  []byte
		found bool
	}
	iface, err := race.first(ctx, func(ctx context.Context, store merkle.Store) (interface{}, error) {
		data, found, err := store.GetBlob(ctx, sum)
		return &res{data: data, found: found}, err
	})
	out := iface.(*res)
	return out.data, out.found, err
}

// first calls all the backend stores concurrently and returns the first
// answer it received.
func (race *raced) first(
	ctx context.Context,
	fn func(context.Context, merkle.Store) (interface{}, error),
) (interface{}, error) {

	// pick the backing stores that will be concurring
	concurrent := race.concurrent
	if race.selection != nil {
		concurrent = race.selection(race.concurrent)
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	first := make(chan interface{}, 1)
	errc := make(chan error, len(concurrent))

	var wg sync.WaitGroup
	for _, cc := range concurrent {
		wg.Add(1)
		go func(cc merkle.Store) {
			defer wg.Done()

			out, err := fn(ctx, cc)
			if err != nil {
				errc <- err
			} else {
				select {
				case first <- out:
				default:
				}
			}

		}(cc)
	}

	go func() {
		wg.Wait()
		close(first)
		close(errc)
	}()

	return <-first, <-errc
}
