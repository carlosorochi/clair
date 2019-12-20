package indexer

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"sync"

	"github.com/klauspost/compress/flate"
	"github.com/klauspost/compress/gzip"
	"github.com/klauspost/compress/snappy"
	"github.com/markusthoemmes/goautoneg"

	"github.com/quay/claircore"
	je "github.com/quay/claircore/pkg/jsonerr"
)

var _ http.Handler = &HTTP{}

const (
	IndexAPIPath       = "/api/v1/index"
	IndexReportAPIPath = "/api/v1/index_report/"
	StateAPIPath       = "/api/v1/state"
)

type HTTP struct {
	*http.ServeMux
	serv Service
}

var (
	snappyPool = sync.Pool{
		New: func() interface{} {
			return snappy.NewBufferedWriter(nil)
		},
	}
	gzipPool = sync.Pool{
		New: func() interface{} {
			w, _ := gzip.NewWriterLevel(nil, gzip.BestSpeed)
			return w
		},
	}
	flatePool = sync.Pool{
		New: func() interface{} {
			w, _ := flate.NewWriter(nil, flate.BestSpeed)
			return w
		},
	}
)

// Enc picks a suitable content encoding, sets the correct header, and returns
// an io.WriteCloser configured to write the chosen format to the
// http.ResponseWriter.
//
// The caller must call Close to ensure all data is flushed.
func enc(w http.ResponseWriter, r *http.Request) io.WriteCloser {
	as := goautoneg.ParseAccept(r.Header.Get("accept-encoding"))
Pick:
	for _, a := range as {
		switch a.Type {
		case "gzip":
			w.Header().Set("content-encoding", "gzip")
			wc := gzipPool.Get().(*gzip.Writer)
			wc.Reset(w)
			return &poolCloser{
				pool:        &gzipPool,
				WriteCloser: wc,
			}
		case "deflate":
			w.Header().Set("content-encoding", "deflate")
			wc := flatePool.Get().(*flate.Writer)
			wc.Reset(w)
			return &poolCloser{
				pool:        &flatePool,
				WriteCloser: wc,
			}
		case "identity":
			w.Header().Set("content-encoding", "identity")
			fallthrough
		case "*":
			break Pick
		case "snappy": // Nonstandard
			w.Header().Set("content-encoding", "snappy")
			wc := snappyPool.Get().(*snappy.Writer)
			wc.Reset(w)
			return &poolCloser{
				pool:        &snappyPool,
				WriteCloser: wc,
			}
		}
	}
	return nopcloser{w}
}

// Nopcloser allows an io.Writer to satisfy io.WriteCloser.
type nopcloser struct {
	io.Writer
}

func (nopcloser) Close() error { return nil }

// PoolCloser returns a WriteCloser to the pool after a successful Close call.
type poolCloser struct {
	pool *sync.Pool
	io.WriteCloser
}

func (p *poolCloser) Close() error {
	if err := p.WriteCloser.Close(); err != nil {
		return err
	}
	p.pool.Put(p.WriteCloser)
	return nil
}

func NewHTTPTransport(service Service) (*HTTP, error) {
	h := &HTTP{
		serv: service,
	}
	mux := http.NewServeMux()
	h.Register(mux)
	h.ServeMux = mux
	return h, nil
}

func (h *HTTP) IndexReportHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		resp := &je.Response{
			Code:    "method-not-allowed",
			Message: "endpoint only allows GET",
		}
		je.Error(w, resp, http.StatusMethodNotAllowed)
		return
	}

	manifestHash := strings.TrimPrefix(r.URL.Path, IndexReportAPIPath)
	if manifestHash == "" {
		resp := &je.Response{
			Code:    "bad-request",
			Message: "malformed path. provide a single manifest hash",
		}
		je.Error(w, resp, http.StatusBadRequest)
		return
	}

	report, ok, err := h.serv.IndexReport(context.Background(), manifestHash)
	if !ok {
		resp := &je.Response{
			Code:    "not-found",
			Message: fmt.Sprintf("index report for manifest %s not found", manifestHash),
		}
		je.Error(w, resp, http.StatusNotFound)
		return
	}
	if err != nil {
		resp := &je.Response{
			Code:    "internal-server-error",
			Message: fmt.Sprintf("%w", err),
		}
		je.Error(w, resp, http.StatusInternalServerError)
		return
	}

	out := enc(w, r)
	defer out.Close()
	err = json.NewEncoder(out).Encode(report)
	if err != nil {
		resp := &je.Response{
			Code:    "encoding-error",
			Message: fmt.Sprintf("failed to encode scan report: %v", err),
		}
		je.Error(w, resp, http.StatusInternalServerError)
		return
	}
}

func (h *HTTP) IndexHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		resp := &je.Response{
			Code:    "method-not-allowed",
			Message: "endpoint only allows POST",
		}
		je.Error(w, resp, http.StatusMethodNotAllowed)
		return
	}

	var m claircore.Manifest
	err := json.NewDecoder(r.Body).Decode(&m)
	if err != nil {
		resp := &je.Response{
			Code:    "bad-request",
			Message: fmt.Sprintf("failed to deserialize manifest: %v", err),
		}
		je.Error(w, resp, http.StatusBadRequest)
		return
	}

	// ToDo: manifest structure validation
	report, err := h.serv.Index(context.Background(), &m)
	if err != nil {
		resp := &je.Response{
			Code:    "index-error",
			Message: fmt.Sprintf("failed to start scan: %v", err),
		}
		je.Error(w, resp, http.StatusInternalServerError)
		return
	}

	out := enc(w, r)
	defer out.Close()
	w.WriteHeader(http.StatusCreated)
	err = json.NewEncoder(out).Encode(report)
	if err != nil {
		resp := &je.Response{
			Code:    "encoding-error",
			Message: fmt.Sprintf("failed to encode scan report: %v", err),
		}
		je.Error(w, resp, http.StatusInternalServerError)
		return
	}
}

func (h *HTTP) StateHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	s := h.serv.State()
	tag := `"` + s + `"`
	w.Header().Add("etag", tag)

	es, ok := r.Header["If-None-Match"]
	if ok {
		if sort.Strings(es); sort.SearchStrings(es, tag) != -1 {
			w.WriteHeader(http.StatusNotModified)
			return
		}
	}
	w.Header().Set("content-type", "text/plain")
	// No trailing newline, so a client can't get confused about whether it
	// counts or not.
	fmt.Fprint(w, s)
}

// Register will register the api on a given mux.
func (h *HTTP) Register(mux *http.ServeMux) {
	mux.HandleFunc(IndexAPIPath, h.IndexHandler)
	mux.HandleFunc(IndexReportAPIPath, h.IndexReportHandler)
	mux.HandleFunc(StateAPIPath, h.StateHandler)
}
