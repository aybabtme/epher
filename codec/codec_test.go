package codec

import (
	"bytes"
	"testing"

	"reflect"

	"github.com/aybabtme/epher/merkle"
	"github.com/aybabtme/epher/thash"
)

func TestBinary(t *testing.T) { testCodec(t, Binary()) }

func makeBlob(data []byte) (thash.Sum, []byte) {
	h := thash.New(thash.Blake2B512)
	h.Write(data)
	return thash.MakeSum(h), data
}

func testCodec(t *testing.T, codec Codec) {

	t.Run("codec node", func(t *testing.T) {
		tree, _ := makeBlob([]byte("tree"))
		start, _ := makeBlob([]byte("start"))
		end, _ := makeBlob([]byte("end"))

		want := merkle.Node{Sum: tree, Start: start, End: end}

		buf := bytes.NewBuffer(nil)
		if err := codec.EncodeNode(buf, want); err != nil {
			t.Fatal(err)
		}
		var got merkle.Node

		if err := codec.DecodeNode(buf, &got); err != nil {
			t.Fatal(err)
		}

		if !reflect.DeepEqual(want, got) {
			t.Errorf("want=%v", want)
			t.Errorf(" got=%v", got)
		}
	})

	t.Run("codec sum", func(t *testing.T) {
		want, _ := makeBlob([]byte("want"))

		buf := bytes.NewBuffer(nil)
		if err := codec.EncodeSum(buf, want); err != nil {
			t.Fatal(err)
		}
		var got thash.Sum

		if err := codec.DecodeSum(buf, &got); err != nil {
			t.Fatal(err)
		}

		if !reflect.DeepEqual(want, got) {
			t.Errorf("want=%v", want)
			t.Errorf(" got=%v", got)
		}
	})

	t.Run("codec blob", func(t *testing.T) {
		wantSum, wantBlob := makeBlob([]byte("hello world"))
		buf := bytes.NewBuffer(nil)
		if err := codec.EncodeBlob(buf, wantSum, wantBlob); err != nil {
			t.Fatal(err)
		}
		var (
			gotSum     thash.Sum
			gotBlobBuf = bytes.NewBuffer(nil)
		)
		if err := codec.DecodeBlob(buf, &gotSum, gotBlobBuf); err != nil {
			t.Fatal(err)
		}
		gotBlob := gotBlobBuf.Bytes()

		if !reflect.DeepEqual(wantSum, gotSum) {
			t.Errorf("want sum=%v", wantSum)
			t.Errorf(" got sum=%v", gotSum)
		}

		if !reflect.DeepEqual(wantBlob, gotBlob) {
			t.Errorf("want blob=%v", wantBlob)
			t.Errorf(" got blob=%v", gotBlob)
		}
	})

	t.Run("codec blob info", func(t *testing.T) {
		sum, blob := makeBlob([]byte("hello world"))
		want := merkle.BlobInfo{
			Sum: sum, Size: int64(len(blob)),
		}

		buf := bytes.NewBuffer(nil)
		if err := codec.EncodeBlobInfo(buf, want); err != nil {
			t.Fatal(err)
		}
		var got merkle.BlobInfo
		if err := codec.DecodeBlobInfo(buf, &got); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(want, got) {
			t.Errorf("want=%v", want)
			t.Errorf(" got=%v", got)
		}
	})
}
