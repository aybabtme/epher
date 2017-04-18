package ephertest

import (
	"bytes"
	"context"
	"math/rand"
	"testing"

	"time"

	"github.com/aybabtme/epher/merkle"
	"github.com/stretchr/testify/assert"
)

func TestStart(t *testing.T) {
	r := rand.New(rand.NewSource(42))

	stores, done := Start(t, 10, r)
	defer done()

	ctx, donectx := context.WithTimeout(context.Background(), 10*time.Second)
	defer donectx()

	local := stores[0]

	want := []byte("123456789")

	tree, _, err := merkle.Build(
		ctx,
		bytes.NewReader(want),
		local,
		merkle.WithBlobSize(1),
	)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("tree represents %d bytes", tree.SizeByte)

	checkRetrieveFromStore := func(store merkle.Store) {
		buf := bytes.NewBuffer(nil)
		invalid, err := tree.Retrieve(ctx, buf, store)
		if err != nil {
			t.Fatal(err)
		}
		if len(invalid) != 0 {
			t.Fatal(invalid)
		}

		got := buf.Bytes()
		assert.Equal(t, want, got)
	}

	t.Logf("can retrieve it from local node")
	// checkRetrieveFromStore(local)
	for _, remoteStore := range stores[1:2] {
		t.Logf("can retrieve it from remote nodes")
		checkRetrieveFromStore(remoteStore)
	}
}
