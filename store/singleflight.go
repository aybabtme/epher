package store

import (
	"context"

	"github.com/aybabtme/epher/merkle"
	"github.com/aybabtme/epher/thash"
	"github.com/golang/groupcache/singleflight"
)

func SingleFlight(store merkle.Store) merkle.Store {
	return &singlef{
		group: new(singleflight.Group),
		store: store,
	}
}

type singlef struct {
	group *singleflight.Group

	store merkle.Store
}

func sumKey(sum thash.Sum) string {
	return string(sum.Type) + "|" + sum.Sum
}

func (sf *singlef) PutNode(ctx context.Context, node merkle.Node) error {
	_, err := sf.group.Do("PutNode/"+sumKey(node.Sum), func() (interface{}, error) {
		return nil, sf.store.PutNode(ctx, node)
	})
	return err
}

func (sf *singlef) GetNode(ctx context.Context, sum thash.Sum) (merkle.Node, bool, error) {
	type res struct {
		node  merkle.Node
		found bool
	}
	iface, err := sf.group.Do("GetNode/"+sumKey(sum), func() (interface{}, error) {
		node, found, err := sf.store.GetNode(ctx, sum)
		return &res{node: node, found: found}, err
	})
	out := iface.(*res)
	return out.node, out.found, err

}

func (sf *singlef) InfoBlob(ctx context.Context, sum thash.Sum) (merkle.BlobInfo, bool, error) {
	type res struct {
		info  merkle.BlobInfo
		found bool
	}
	iface, err := sf.group.Do("InfoBlob/"+sumKey(sum), func() (interface{}, error) {
		info, found, err := sf.store.InfoBlob(ctx, sum)
		return &res{info: info, found: found}, err
	})
	out := iface.(*res)
	return out.info, out.found, err
}

func (sf *singlef) PutBlob(ctx context.Context, sum thash.Sum, blob []byte) error {
	_, err := sf.group.Do("PutBlob/"+sumKey(sum), func() (interface{}, error) {
		return nil, sf.store.PutBlob(ctx, sum, blob)
	})
	return err
}

func (sf *singlef) GetBlob(ctx context.Context, sum thash.Sum) ([]byte, bool, error) {
	type res struct {
		data  []byte
		found bool
	}
	iface, err := sf.group.Do("GetBlob/"+sumKey(sum), func() (interface{}, error) {
		data, found, err := sf.store.GetBlob(ctx, sum)
		return &res{data: data, found: found}, err
	})
	out := iface.(*res)
	return out.data, out.found, err
}
