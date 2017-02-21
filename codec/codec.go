package codec

import (
	"io"

	"encoding/binary"

	"bytes"

	"github.com/aybabtme/epher/merkle"
	"github.com/aybabtme/epher/thash"
)

type Codec interface {
	DecodeNode(io.Reader, *merkle.Node) error
	EncodeNode(io.Writer, merkle.Node) error

	DecodeSum(io.Reader, *thash.Sum) error
	EncodeSum(io.Writer, thash.Sum) error

	DecodeBlob(io.Reader, *thash.Sum, io.Writer) error
	EncodeBlob(io.Writer, thash.Sum, []byte) error

	DecodeBlobInfo(io.Reader, *merkle.BlobInfo) error
	EncodeBlobInfo(io.Writer, merkle.BlobInfo) error
}

func Binary() Codec {
	return bin{}
}

type bin struct{}

func (b bin) DecodeNode(r io.Reader, node *merkle.Node) error {
	if err := b.DecodeSum(r, &node.Sum); err != nil {
		return err
	}
	if err := b.DecodeSum(r, &node.Start); err != nil {
		return err
	}
	if err := b.DecodeSum(r, &node.End); err != nil {
		return err
	}
	return nil
}

func (b bin) EncodeNode(w io.Writer, node merkle.Node) error {
	if err := b.EncodeSum(w, node.Sum); err != nil {
		return err
	}
	if err := b.EncodeSum(w, node.Start); err != nil {
		return err
	}
	if err := b.EncodeSum(w, node.End); err != nil {
		return err
	}
	return nil
}

func (b bin) DecodeSum(r io.Reader, sum *thash.Sum) error {
	if err := binary.Read(r, binary.LittleEndian, &sum.Type); err != nil {
		return err
	}
	buf := bytes.NewBuffer(nil)
	if err := b.decodeBytes(r, buf); err != nil {
		return err
	}
	sum.Sum = buf.String()
	return nil
}

func (b bin) EncodeSum(w io.Writer, sum thash.Sum) error {
	if err := binary.Write(w, binary.LittleEndian, sum.Type); err != nil {
		return err
	}
	if err := b.encodeBytes(w, []byte(sum.Sum)); err != nil {
		return err
	}
	return nil
}

func (b bin) DecodeBlobInfo(r io.Reader, info *merkle.BlobInfo) error {
	if err := b.DecodeSum(r, &info.Sum); err != nil {
		return err
	}
	if err := binary.Read(r, binary.LittleEndian, &info.Size); err != nil {
		return err
	}
	return nil
}

func (b bin) EncodeBlobInfo(w io.Writer, info merkle.BlobInfo) error {
	if err := b.EncodeSum(w, info.Sum); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, info.Size); err != nil {
		return err
	}
	return nil
}

func (b bin) DecodeBlob(r io.Reader, sum *thash.Sum, w io.Writer) error {
	if err := b.DecodeSum(r, sum); err != nil {
		return err
	}
	if err := b.decodeBytes(r, w); err != nil {
		return err
	}
	return nil
}

func (b bin) EncodeBlob(w io.Writer, sum thash.Sum, data []byte) error {
	if err := b.EncodeSum(w, sum); err != nil {
		return err
	}
	if err := b.encodeBytes(w, data); err != nil {
		return err
	}
	return nil
}

func (bin) decodeBytes(r io.Reader, w io.Writer) error {
	var l int64
	if err := binary.Read(r, binary.LittleEndian, &l); err != nil {
		return err
	}
	n, err := io.CopyN(w, r, l)
	if err == io.EOF {
		if n != l {
			return io.ErrUnexpectedEOF
		}
		return nil
	}
	return err
}

func (bin) encodeBytes(w io.Writer, data []byte) error {
	if err := binary.Write(w, binary.LittleEndian, int64(len(data))); err != nil {
		return err
	}
	_, err := w.Write(data)
	return err
}
