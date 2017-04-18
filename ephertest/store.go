package ephertest

import (
	"testing"

	"github.com/aybabtme/epher/merkle"
	"github.com/aybabtme/epher/store"
)

func startStore(t *testing.T) merkle.Store {
	return store.NewMemoryStore()
}
