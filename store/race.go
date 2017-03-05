package store

import (
	"context"
	"math/rand"

	"sync"

	"math"

	"github.com/aybabtme/epher/merkle"
	"github.com/aybabtme/epher/thash"
)

func GrowthLog2(i int) int {
	// log2(n):
	// 10    -> 3
	// 100   -> 6
	// 1k    -> 9
	// 10k   -> 13
	// 100k  -> 16
	// 1M    -> 20
	f := float64(i)
	lg2 := math.Log2(f)
	return int(math.Floor(lg2))
}

func GrowthLog2Square(i int) int {
	// log2(n)^2:
	// 0..16 -> 0..16
	// 20    -> 18
	// 40    -> 28
	// 60    -> 34
	// 80    -> 39
	// 100   -> 44
	// 1k    -> 99
	// 10k   -> 176
	// 100k  -> 275
	// 1M    -> 397
	f := float64(i)
	lg2sq := math.Pow(math.Log2(f), 2)
	return int(math.Floor(lg2sq))
}

// RaceRandomOf will select a random concurrents from the given
// list, with a minimum of `minCount`, growing with `growth` of the size
// of the list of concurrents.
func RaceRandomOf(r *rand.Rand, growth func(int) int, min int) func([]merkle.Store) []merkle.Store {

	return func(in []merkle.Store) []merkle.Store {
		n := len(in)

		picks := growth(n)
		if picks < min {
			picks = min
		}
		if picks > n {
			return in
		}
		selected := make([]merkle.Store, 0, picks)
		for i := 0; i < picks; i++ {
			selected = append(selected,
				in[rand.Intn(len(in))],
			)
		}
		return selected
	}
}

// Race makes multiple stores race together. The first answer to any
// query is returned, while the other ones are cancelled.
func Race(
	selection func([]merkle.Store) []merkle.Store,
	concurrent Pool,
) merkle.Store {
	return &raced{selection: selection, concurrent: concurrent}
}

type raced struct {
	selection  func([]merkle.Store) []merkle.Store
	concurrent Pool
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

func (race *raced) InfoBlob(ctx context.Context, sum thash.Sum) (merkle.BlobInfo, bool, error) {
	type res struct {
		info  merkle.BlobInfo
		found bool
	}
	iface, err := race.first(ctx, func(ctx context.Context, store merkle.Store) (interface{}, error) {
		info, found, err := store.InfoBlob(ctx, sum)
		return &res{info: info, found: found}, err
	})
	out := iface.(*res)
	return out.info, out.found, err
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
	concurrent := race.concurrent()
	if race.selection != nil {
		concurrent = race.selection(concurrent)
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

	var err error
	for {
		select {
		case out := <-first:
			return out, err
		case anErr := <-errc:
			if err != nil {
				err = anErr
			}
		}
	}
}
