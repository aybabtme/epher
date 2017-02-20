package store

import (
	"context"

	"github.com/aybabtme/epher/merkle"
	"github.com/aybabtme/epher/thash"
)

type MemoryStore struct {
	node map[thash.Sum]merkle.Node
	data map[thash.Sum][]byte
}

func NewMemoryStore() merkle.Store {
	return &MemoryStore{
		node: make(map[thash.Sum]merkle.Node),
		data: make(map[thash.Sum][]byte),
	}
}

func (mem *MemoryStore) PutNode(ctx context.Context, node merkle.Node) error {
	mem.node[node.Sum] = node
	return nil
}

func (mem *MemoryStore) GetNode(ctx context.Context, sum thash.Sum) (merkle.Node, bool, error) {
	node, ok := mem.node[sum]
	if !ok {
		return merkle.Node{}, false, nil
	}
	return node, true, nil
}

func (mem *MemoryStore) PutBlob(ctx context.Context, sum thash.Sum, data []byte) error {
	cp := make([]byte, len(data))
	copy(cp, data)
	mem.data[sum] = cp
	return nil
}

func (mem *MemoryStore) GetBlob(ctx context.Context, sum thash.Sum) ([]byte, bool, error) {
	data, ok := mem.data[sum]
	return data, ok, nil
}

func (mem *MemoryStore) InfoBlob(ctx context.Context, sum thash.Sum) (merkle.BlobInfo, bool, error) {
	data, ok := mem.data[sum]
	if !ok {
		return merkle.BlobInfo{}, false, nil
	}
	return merkle.BlobInfo{Size: int64(len(data)), Sum: sum}, true, nil
}
