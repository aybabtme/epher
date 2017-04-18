package store

import "testing"

func TestMemoryStore(t *testing.T) { testStore(t, NewMemoryStore) }
