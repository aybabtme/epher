package merkle

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"strings"

	"github.com/aybabtme/epher/thash"
)

type Store interface {
	PutNode(context.Context, Node) error
	GetNode(context.Context, thash.Sum) (Node, bool, error)
	PutBlob(context.Context, thash.Sum, []byte) error
	GetBlob(context.Context, thash.Sum) ([]byte, bool, error)
	InfoBlob(context.Context, thash.Sum) (BlobInfo, bool, error)
}

type Option func(*config)

type config struct {
	HashType thash.Type
	BlobSize int64
}

func newConfig(opts []Option) *config {
	def := &config{
		HashType: thash.Blake2B512,
		BlobSize: 4 << 20, // 4MiB
	}
	for _, o := range opts {
		o(def)
	}
	return def
}

func WithBlobSize(sz int64) Option      { return func(opts *config) { opts.BlobSize = sz } }
func WithHashType(ht thash.Type) Option { return func(opts *config) { opts.HashType = ht } }

func Build(ctx context.Context, r io.Reader, store Store, opts ...Option) (*Tree, thash.Sum, error) {

	config := newConfig(opts)

	rdbuf := bytes.NewBuffer(nil)

	var bis []BlobInfo

	reachedEOF := false
	for !reachedEOF {
		rdbuf.Reset()

		n, err := io.CopyN(rdbuf, r, config.BlobSize)
		if err != nil && err != io.EOF {
			return nil, thash.Sum{}, err
		}
		reachedEOF = (err == io.EOF)
		if n == 0 {
			break
		}

		data := rdbuf.Bytes()

		sum, n, err := copyBlob(config.HashType, ioutil.Discard, rdbuf)
		if err != nil {
			return nil, thash.Sum{}, err
		}

		if err := store.PutBlob(ctx, sum, data); err != nil {
			return nil, thash.Sum{}, err
		}

		bis = append(bis, BlobInfo{Sum: sum, Size: n})
	}

	tree := newTree(bis)
	if err := tree.persist(ctx, store); err != nil {
		return nil, thash.Sum{}, err
	}
	return tree, tree.HashSum, nil
}

func copyBlob(t thash.Type, w io.Writer, r io.Reader) (thash.Sum, int64, error) {

	h := thash.New(t)

	n, err := io.Copy(w, io.TeeReader(r, h))
	if err != nil {
		return thash.Sum{}, n, err
	}

	return thash.MakeSum(h), n, err
}

var (
	errMalformedTree = errors.New("tree is malformed")
	errDataMissing   = errors.New("data described by the tree can't be found in store")
)

// Node is a node in a merkle tree. A node is sufficient
// to retrieve the whole of a merkle tree rooted in this node.
type Node struct {
	Sum        thash.Sum
	Start, End thash.Sum
}

func RetrieveTree(ctx context.Context, sum thash.Sum, store Store) (*Tree, error) {
	root, found, err := store.GetNode(ctx, sum)
	if err != nil {
		return nil, err
	}
	if !found {
		return &Tree{HashSum: sum}, nil
	}
	start, err := RetrieveTree(ctx, root.Start, store)
	if err != nil {
		return nil, err
	}
	end, err := RetrieveTree(ctx, root.End, store)
	if err != nil {
		return nil, err
	}
	tree := &Tree{Start: start, End: end, HashSum: sum}

	// set the sizes
	err = walk(tree, func(branch *Tree) error {
		branch.SizeByte = branch.Start.SizeByte + branch.End.SizeByte
		return nil
	}, func(leaf *Tree) error {
		bi, _, err := store.InfoBlob(ctx, leaf.HashSum)
		leaf.SizeByte = bi.Size
		return err
	})

	return tree, err
}

// Tree is a concrete merkle tree.
type Tree struct {
	Start *Tree `json:"start"`
	End   *Tree `json:"end"`

	SizeByte int64     `json:"size_byte"`
	HashSum  thash.Sum `json:"hash_sum"`
}

func walk(tree *Tree, onBranch, onLeaf func(*Tree) error) error {
	switch {
	case tree.Start != nil && tree.End != nil:
		if err := walk(tree.Start, onBranch, onLeaf); err != nil {
			return err
		}
		if err := walk(tree.End, onBranch, onLeaf); err != nil {
			return err
		}
		return onBranch(tree)

	case tree.Start == nil && tree.End == nil:
		return onLeaf(tree)

	case tree == nil,
		tree.Start == nil && tree.End != nil, // can't have an end without a start
		tree.Start != nil && tree.End == nil: // we should have been a data node
		return errMalformedTree
	default:
		panic(fmt.Sprintf("unhandled case: %#v", tree))
	}
}

func (tree *Tree) Retrieve(ctx context.Context, wr io.Writer, store Store) (invalid []*Tree, err error) {
	if wr == nil {
		wr = ioutil.Discard
	}
	return tree.retrieve(ctx, wr, store)
}

func (tree *Tree) retrieve(ctx context.Context, wr io.Writer, store Store) (invalid []*Tree, err error) {
	onBranch := func(branch *Tree) error {
		// verify that this.sum == sum(start.sum, end.sum)
		// then verify:
		//   - start
		//   - end

		// we're a sum of hash sum
		got := sumHashWithTree(branch.Start, branch.End)
		if !branch.HashSum.Equal(got) {
			invalid = append(invalid, branch) // we're invalid
		}
		return nil
	}
	onLeaf := func(leaf *Tree) error {
		blob, found, err := store.GetBlob(ctx, leaf.HashSum)
		if err != nil {
			invalid = []*Tree{leaf}
			return err
		}
		if !found {
			invalid = []*Tree{leaf}
			return errDataMissing
		}

		got, _, err := copyBlob(leaf.HashSum.Type, wr, bytes.NewReader(blob))
		if err != nil {
			invalid = []*Tree{leaf}
			return err
		}
		if !leaf.HashSum.Equal(got) {
			invalid = []*Tree{leaf}
			return fmt.Errorf("want sum %x, got %x", leaf.HashSum.Sum, got.Sum, blob)
		}
		return nil
	}
	return invalid, walk(tree, onBranch, onLeaf)
}

func (tree *Tree) persist(ctx context.Context, store Store) error {

	onBranch := func(branch *Tree) error {
		if err := branch.Start.persist(ctx, store); err != nil {
			return err
		}
		if err := branch.End.persist(ctx, store); err != nil {
			return err
		}
		return store.PutNode(ctx, Node{
			Sum:   branch.HashSum,
			Start: branch.Start.HashSum,
			End:   branch.End.HashSum,
		})
	}
	onLeaf := func(leaf *Tree) error {
		return nil
	}
	return walk(tree, onBranch, onLeaf)
}

type BlobInfo struct {
	Sum  thash.Sum
	Size int64
}

func newTree(bis []BlobInfo) *Tree {
	switch n := len(bis); n {
	case 0: // no data
		return nil
	case 1: // we're a leaf
		bi := bis[0]
		return &Tree{
			HashSum:  bi.Sum,
			SizeByte: bi.Size,
		}
	default:

		var (
			start = newTree(bis[:n/2])
			end   = newTree(bis[n/2:])
		)
		if start == nil {
			panic("should not be possible")
		}
		if end == nil {
			// we're a tree of odd size
			return start
		}

		// we have two child nodes, so compute the hash sum of their appended hash sums

		return &Tree{
			Start:    start,
			End:      end,
			SizeByte: start.SizeByte + end.SizeByte,
			HashSum:  sumHashWithTree(start, end),
		}
	}
}

func sumHashWithTree(start, end *Tree) thash.Sum {
	appendedSums := start.HashSum.Sum + end.HashSum.Sum
	h := thash.New(start.HashSum.Type)
	_, err := io.Copy(h, strings.NewReader(appendedSums))
	if err != nil {
		panic(err) // should never happen
	}
	return thash.MakeSum(h)
}
