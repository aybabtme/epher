package store

import (
	"bytes"
	"context"
	"testing"

	"github.com/aybabtme/epher/merkle"
	"github.com/stretchr/testify/assert"
)

func testStore(t *testing.T, mkStore func() merkle.Store) {
	tests := []struct {
		name   string
		testFn func(t *testing.T, mkStore func() merkle.Store)
	}{
		{"access_by_tree", testStoreByTree},
		{"access_by_hash_sum", testStoreByHashSum},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.testFn(t, mkStore)
		})
	}
}

func testStoreByTree(t *testing.T, mkStore func() merkle.Store) {
	ctx := context.Background()

	store := mkStore()

	want := []byte("123456789")

	tree, _, err := merkle.Build(
		ctx,
		bytes.NewReader(want),
		store,
		merkle.WithBlobSize(1),
	)
	if err != nil {
		t.Fatal(err)
	}

	buf := bytes.NewBuffer(nil)

	invalid, err := tree.Retrieve(ctx, buf, store)
	if err != nil {
		t.Fatal(err)
	}
	if len(invalid) != 0 {
		panic(invalid)
	}

	got := buf.Bytes()

	assert.Equal(t, want, got)
}

func testStoreByHashSum(t *testing.T, mkStore func() merkle.Store) {
	ctx := context.Background()

	store := mkStore()

	want := []byte("123456789")

	_, sum, err := merkle.Build(
		ctx,
		bytes.NewReader(want),
		store,
		merkle.WithBlobSize(1),
	)
	if err != nil {
		t.Fatal(err)
	}

	tree, err := merkle.RetrieveTree(ctx, sum, store)
	if err != nil {
		t.Fatal(err)
	}

	buf := bytes.NewBuffer(nil)

	invalid, err := tree.Retrieve(ctx, buf, store)
	if err != nil {
		t.Fatal(err)
	}
	if len(invalid) != 0 {
		panic(invalid)
	}

	got := buf.Bytes()

	assert.Equal(t, want, got)
}
