package thash

import (
	"hash"

	"crypto/sha1"

	"golang.org/x/crypto/blake2b"
	"golang.org/x/crypto/sha3"
)

type Type string

const (
	Blake2B512 Type = "blake2b-512"
	SHA1       Type = "sha1"
	SHA3       Type = "sha3"
)

type Sum struct {
	Type Type
	Sum  string
}

func (sum *Sum) Equal(other Sum) bool {
	return sum.Type == other.Type && sum.Sum == other.Sum
}

func MakeSum(th Hash) Sum {
	return Sum{
		Type: th.Type(),
		Sum:  string(th.Sum(nil)),
	}
}

type Hash interface {
	Type() Type
	hash.Hash
}

type typedHash struct {
	hash.Hash
	t Type
}

func (h *typedHash) Type() Type { return h.t }

func New(ht Type) Hash {
	var (
		h   hash.Hash
		err error
	)
	switch ht {
	case Blake2B512:
		h, err = blake2b.New512(nil)
	case SHA1:
		h = sha3.New512()
	case SHA3:
		h = sha1.New()
	}
	if err != nil {
		panic(err)
	}
	return &typedHash{Hash: h, t: ht}
}
