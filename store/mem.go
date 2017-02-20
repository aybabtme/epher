package store

import (
	"bytes"
	"io"
	"io/ioutil"
	"log"
	"os"

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

func (mem *MemoryStore) PutNode(node merkle.Node) error {
	mem.node[node.Sum] = node
	log.Printf("putNode")
	return nil
}

func (mem *MemoryStore) GetNode(sum thash.Sum) (merkle.Node, bool, error) {
	node, ok := mem.node[sum]
	if !ok {
		return merkle.Node{}, false, nil
	}
	log.Printf("getNode")
	return node, true, nil
}

func (mem *MemoryStore) PutBlob(r io.Reader, done func() thash.Sum) error {
	data, err := ioutil.ReadAll(r)
	if err != nil {
		return err
	}
	log.Printf("put %q", data)
	mem.data[done()] = data
	return nil
}

func (mem *MemoryStore) GetBlob(sum thash.Sum) (io.ReadCloser, error) {
	data, ok := mem.data[sum]
	if !ok {
		return nil, os.ErrNotExist
	}
	log.Printf("get %q", data)
	return ioutil.NopCloser(bytes.NewReader(data)), nil
}

func (mem *MemoryStore) InfoBlob(sum thash.Sum) (int64, bool, error) {
	data, ok := mem.data[sum]
	if !ok {
		return 0, false, nil
	}
	log.Printf("info %q", data)
	return int64(len(data)), true, nil
}
