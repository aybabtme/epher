package codec

import (
	"io"

	"github.com/aybabtme/epher/merkle"
	"github.com/aybabtme/epher/thash"
)

type Codec interface {
	DecodeNode(io.Reader) (merkle.Node, error)
	EncodeNode(io.Writer, merkle.Node) error

	DecodeSum(io.Reader) (thash.Sum, error)
	EncodeSum(io.Writer, thash.Sum) error

	DecodeBlob(io.Reader) (thash.Sum, []byte, error)
	EncodeBlob(io.Writer, thash.Sum, []byte) error

	DecodeBlobInfo(io.Reader) (merkle.BlobInfo, error)
	EncodeBlobInfo(io.Writer, merkle.BlobInfo) error
}
