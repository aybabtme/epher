package merkle_test

import (
	"bytes"
	"fmt"

	"github.com/aybabtme/epher/merkle"
	"github.com/aybabtme/epher/store"
)

func ExampleTree() {

	store := store.NewMemoryStore()

	want := []byte("123456789")

	tree, _, err := merkle.Build(
		bytes.NewReader(want),
		store,
		merkle.WithBlobSize(1),
	)
	if err != nil {
		panic(err)
	}

	fmt.Printf("tree represents %d bytes\n", tree.SizeByte)

	buf := bytes.NewBuffer(nil)

	invalid, err := tree.Retrieve(buf, store)
	if err != nil {
		panic(err)
	}
	if len(invalid) != 0 {
		panic(invalid)
	}

	got := buf.Bytes()

	fmt.Println(string(got))
	// Output:
	// tree represents 9 bytes
	// 123456789
}

func ExampleTree_by_hash() {

	store := store.NewMemoryStore()

	want := []byte("123456789")

	_, sum, err := merkle.Build(
		bytes.NewReader(want),
		store,
		merkle.WithBlobSize(1),
	)
	if err != nil {
		panic(err)
	}

	tree, err := merkle.RetrieveTree(sum, store)
	if err != nil {
		panic(err)
	}

	fmt.Printf("tree represents %d bytes\n", tree.SizeByte)

	buf := bytes.NewBuffer(nil)

	invalid, err := tree.Retrieve(buf, store)
	if err != nil {
		panic(err)
	}
	if len(invalid) != 0 {
		panic(invalid)
	}

	got := buf.Bytes()

	fmt.Println(string(got))
	// Output:
	// tree represents 9 bytes
	// 123456789
}
