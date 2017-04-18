package store

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"

	"net/url"

	"github.com/aybabtme/epher/codec"
	"github.com/aybabtme/epher/merkle"
	"github.com/aybabtme/epher/thash"
	"github.com/aybabtme/log"
	"github.com/julienschmidt/httprouter"
	"github.com/opentracing-contrib/go-stdlib/nethttp"
	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"
)

type rpcClient struct {
	baseURL *url.URL
	codec   codec.Codec
	cl      *http.Client
}

func HTTPClient(addr string, codec codec.Codec, cl *http.Client) merkle.Store {
	if cl == nil {
		cl = new(http.Client)
	}
	cl.Transport = &nethttp.Transport{cl.Transport}

	u := &url.URL{
		Scheme: "http", // don't use clear text =/
		Host:   addr,
	}

	return &rpcClient{baseURL: u, codec: codec, cl: cl}
}

func (rpc *rpcClient) do(
	ctx context.Context,
	method, pathStr string,
	onReq func(io.Writer) error,
	onResp func(r io.Reader) error,
) error {

	var body io.Reader
	if onReq != nil {
		buf := bytes.NewBuffer(nil)
		if err := onReq(buf); err != nil {
			return err
		}
		body = buf
	}
	u, err := rpc.baseURL.Parse(pathStr)
	if err != nil {
		return err
	}

	req, err := http.NewRequest(method, u.String(), body)
	if err != nil {
		return err
	}
	req = req.WithContext(ctx)

	tracer := opentracing.GlobalTracer()
	req, ht := nethttp.TraceRequest(tracer, req)
	defer ht.Finish()

	resp, err := rpc.cl.Do(req)
	if err != nil {
		ext.Error.Set(ht.Span(), true)
		ht.Span().LogKV("err", err)
		return err
	}
	defer resp.Body.Close()
	if onResp == nil {
		return nil
	}
	if resp.StatusCode == http.StatusNotFound {
		return nil
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status %q", resp.Status)
	}
	err = onResp(resp.Body)
	if err != nil {
		ext.Error.Set(ht.Span(), true)
		ht.Span().LogKV("err", err)
	}
	return err
}

func (rpc *rpcClient) PutNode(ctx context.Context, node merkle.Node) error {
	return rpc.do(ctx, "PUT", "/v1/nodes",
		func(w io.Writer) error {
			return rpc.codec.EncodeNode(w, node)
		},
		nil,
	)
}
func (rpc *rpcClient) GetNode(ctx context.Context, sum thash.Sum) (merkle.Node, bool, error) {
	var (
		node  merkle.Node
		found bool
	)
	return node, found, rpc.do(ctx, "GET", "/v1/nodes",
		func(w io.Writer) error {
			return rpc.codec.EncodeSum(w, sum)
		},
		func(resp io.Reader) error {
			err := rpc.codec.DecodeNode(resp, &node)
			found = true
			return err
		},
	)
}
func (rpc *rpcClient) PutBlob(ctx context.Context, sum thash.Sum, data []byte) error {
	return rpc.do(ctx, "PUT", "/v1/blobs",
		func(w io.Writer) error {
			return rpc.codec.EncodeBlob(w, sum, data)
		},
		nil,
	)
}
func (rpc *rpcClient) GetBlob(ctx context.Context, sum thash.Sum) ([]byte, bool, error) {
	var (
		buf   = bytes.NewBuffer(nil)
		found bool
	)
	err := rpc.do(ctx, "GET", "/v1/blobs",
		func(w io.Writer) error {
			return rpc.codec.EncodeSum(w, sum)
		},
		func(resp io.Reader) error {
			err := rpc.codec.DecodeBlob(resp, new(thash.Sum), buf)
			found = true
			return err
		},
	)
	return buf.Bytes(), found, err
}
func (rpc *rpcClient) InfoBlob(ctx context.Context, sum thash.Sum) (merkle.BlobInfo, bool, error) {
	var (
		info  merkle.BlobInfo
		found bool
	)
	return info, found, rpc.do(ctx, "HEAD", "/v1/blobs",
		func(w io.Writer) error {
			return rpc.codec.EncodeSum(w, sum)
		},
		func(resp io.Reader) error {
			err := rpc.codec.DecodeBlobInfo(resp, &info)
			found = true
			return err
		},
	)
}

type rpcServer struct {
	codec codec.Codec
	store merkle.Store
	log   *log.Log
}

func HTTPServer(codec codec.Codec, store merkle.Store) http.Handler {

	rpc := &rpcServer{codec: codec, store: store, log: log.KV("rpc", "server")}
	router := httprouter.New()
	router.PUT("/v1/nodes", rpc.PutNode)
	router.GET("/v1/nodes", rpc.GetNode)
	router.PUT("/v1/blobs", rpc.PutBlob)
	router.GET("/v1/blobs", rpc.GetBlob)
	router.HEAD("/v1/blobs", rpc.InfoBlob)

	return nethttp.Middleware(
		opentracing.GlobalTracer(),
		router,
		nethttp.OperationNameFunc(func(r *http.Request) string {
			switch {
			case strings.HasPrefix(r.URL.Path, "/v1/nodes"):
				return "rpcServer." + r.Method + "." + "Nodes"
			case strings.HasPrefix(r.URL.Path, "/v1/blobs"):
				return "rpcServer." + r.Method + "." + "Blobs"
			}
			return "HTTP" + r.Method
		}),
	)
}

func (rpc *rpcServer) PutNode(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	ctx := r.Context()

	var node merkle.Node
	err := rpc.codec.DecodeNode(r.Body, &node)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, "%v", err)
		return
	}

	err = rpc.store.PutNode(ctx, node)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, "%v", err)
		return
	}
}

func (rpc *rpcServer) GetNode(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	ctx := r.Context()

	var sum thash.Sum
	err := rpc.codec.DecodeSum(r.Body, &sum)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, "%v", err)
		return
	}

	node, found, err := rpc.store.GetNode(ctx, sum)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, "%v", err)
		return
	}
	if !found {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	if err := rpc.codec.EncodeNode(w, node); err != nil {
		rpc.log.Err(err).Info("can't send node to client")
		return
	}
}

func (rpc *rpcServer) PutBlob(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	ctx := r.Context()

	var (
		sum thash.Sum
		buf = bytes.NewBuffer(nil)
	)
	err := rpc.codec.DecodeBlob(r.Body, &sum, buf)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, "%v", err)
		return
	}

	err = rpc.store.PutBlob(ctx, sum, buf.Bytes())
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, "%v", err)
		return
	}
}

func (rpc *rpcServer) GetBlob(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	ctx := r.Context()

	var sum thash.Sum
	err := rpc.codec.DecodeSum(r.Body, &sum)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, "%v", err)
		return
	}

	blob, found, err := rpc.store.GetBlob(ctx, sum)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, "%v", err)
		return
	}
	if !found {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	if err := rpc.codec.EncodeBlob(w, sum, blob); err != nil {
		rpc.log.Err(err).Info("can't send blob to client")
		return
	}
}

func (rpc *rpcServer) InfoBlob(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	ctx := r.Context()

	var sum thash.Sum
	err := rpc.codec.DecodeSum(r.Body, &sum)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, "%v", err)
		return
	}

	info, found, err := rpc.store.InfoBlob(ctx, sum)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, "%v", err)
		return
	}
	if !found {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	if err := rpc.codec.EncodeBlobInfo(w, info); err != nil {
		rpc.log.Err(err).Info("can't send blob to client")
		return
	}
}
