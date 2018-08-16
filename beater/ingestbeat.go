package beater

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/elastic/beats/libbeat/beat"
	"github.com/elastic/beats/libbeat/common"
	"github.com/elastic/beats/libbeat/logp"
	"github.com/gorilla/mux"

	"cdcvs.fnal.gov/landscape/ingestbeat/config"
)

type Ingestbeat struct {
	done   chan struct{}
	config config.Config
	client beat.Client
	logger *logp.Logger
}

// Creates beater
func New(b *beat.Beat, cfg *common.Config) (beat.Beater, error) {
	c := config.DefaultConfig
	if err := cfg.Unpack(&c); err != nil {
		return nil, fmt.Errorf("Error reading config file: %v", err)
	}

	bt := &Ingestbeat{
		done:   make(chan struct{}),
		config: c,
		logger: logp.NewLogger("ingestbeat"),
	}
	return bt, nil
}

func (bt *Ingestbeat) Run(b *beat.Beat) error {
	r := mux.NewRouter()
	http.Handle("/", bt.loggingHandler(r))
	r.Path("/").Methods("GET").HandlerFunc(bt.pingHandler)
	r.Path("/").Methods("HEAD").HandlerFunc(bt.headHandler)
	r.Path("/_bulk").Methods("GET", "POST").HandlerFunc(bt.bulkHandler)
	r.Path("/{index}").Methods("HEAD").HandlerFunc(bt.headHandler)
	r.Path("/{index}/_bulk").Methods("GET", "POST").HandlerFunc(bt.bulkHandler)
	r.Path("/{index}/{type}/_bulk").Methods("GET", "POST").HandlerFunc(bt.bulkHandler)
	r.Path("/{index}/{type}").Methods("HEAD").HandlerFunc(bt.headHandler)
	r.Path("/{index}/{type}").Methods("POST").HandlerFunc(bt.indexHandler)
	r.Path("/{index}/{type}/").Methods("POST").HandlerFunc(bt.indexHandler)
	r.Path("/{index}/{type}/{id}").Methods("PUT").HandlerFunc(bt.indexHandler)
	r.Path("/{index}/_mapping/{type}").Methods("HEAD").HandlerFunc(bt.headHandler)
	r.Path("/_xpack").Methods("GET").HandlerFunc(bt.xpackHandler)

	go http.ListenAndServe(bt.config.Address, nil)

	bt.logger.Infof("listening on %s", bt.config.Address)

	var err error
	bt.client, err = b.Publisher.Connect()
	if err != nil {
		return err
	}

	select {
	case <-bt.done:
		return nil
	}

}

func (bt *Ingestbeat) Stop() {
	bt.client.Close()
	close(bt.done)
}

// logResponseWriter wraps an http.ResponseWriter and writes the
// status code and response body to another io.Writer for logging.
type logResponseWriter struct {
	rw         http.ResponseWriter
	statusCode int
	logw       io.Writer
}

func newLogResponseWriter(rw http.ResponseWriter, logw io.Writer) http.ResponseWriter {
	w := logResponseWriter{
		rw:   rw,
		logw: logw,
	}
	return &w
}

func (w *logResponseWriter) Header() http.Header {
	return w.rw.Header()
}

func (w *logResponseWriter) Write(data []byte) (int, error) {
	if w.statusCode == 0 {
		w.statusCode = http.StatusOK
		fmt.Fprintf(w.logw, "200 OK\n")
	}
	mw := io.MultiWriter(w.rw, w.logw)
	return mw.Write(data)
}

func (w *logResponseWriter) WriteHeader(statusCode int) {
	w.statusCode = statusCode
	fmt.Fprintf(w.logw, "%d %s\n", statusCode, http.StatusText(statusCode))
	w.rw.WriteHeader(statusCode)
}

// loggingHandler wraps an http.Handler to log each request
func (bt *Ingestbeat) loggingHandler(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		path := r.URL.EscapedPath()
		// save request body
		//var rb bytes.Buffer
		//bodyReader := io.TeeReader(r.Body, &rb)
		//r.Body = ioutil.NopCloser(bodyReader)
		// save response
		var wb bytes.Buffer
		respWriter := newLogResponseWriter(w, &wb)
		h.ServeHTTP(respWriter, r)
		bt.logger.Infow("handled request",
			"origin", originAddr(r),
			"length", r.ContentLength,
			"agent", r.UserAgent(),
			"path", path,
			"method", r.Method,
			"time", time.Since(start).Nanoseconds(),
			//"body",     rb.String(),
			"response", wb.String(),
		)
	})
}

// originAddr returns the "real" remote address for forwarded requests
func originAddr(r *http.Request) string {
	if remote := r.Header.Get("X-Real-IP"); remote != "" {
		return remote
	} else if remote := r.Header.Get("X-Forwarded-For"); remote != "" {
		return remote
	}
	return r.RemoteAddr
}

type indexResponse struct {
	Index  string `json:"_index"`
	Type   string `json:"_type"`
	ID     string `json:"_id"`
	Result string `json:"result"`
	Status int    `json:"status,omitempty"`
}

func (bt *Ingestbeat) indexHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	event := beat.Event{
		Timestamp: time.Now(),
		Meta: common.MapStr{
			"_index": vars["index"],
			"_type":  vars["type"],
			"_id":    vars["id"],
		},
	}
	dec := json.NewDecoder(r.Body)
	if err := dec.Decode(&event.Fields); err != nil {
		bt.handleError(w, err.Error(), http.StatusBadRequest)
		return
	}
	bt.client.Publish(event)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)

	resp := indexResponse{
		Index:  vars["index"],
		Type:   vars["type"],
		ID:     vars["id"],
		Result: "created",
	}
	enc := json.NewEncoder(w)
	if err := enc.Encode(resp); err != nil {
		bt.logger.Errorf("error encoding response: %s", err.Error())
	}
}

type bulkItemResponse map[string]indexResponse
type bulkResponse struct {
	Took   int64              `json:"took"`
	Errors bool               `json:"errors"`
	Items  []bulkItemResponse `json:"items"`
}

func (bt *Ingestbeat) bulkHandler(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	vars := mux.Vars(r)
	resp := bulkResponse{
		Items: make([]bulkItemResponse, 0, 100),
	}
	scanner := bufio.NewScanner(r.Body)
	for scanner.Scan() {
		var act map[string]common.MapStr
		if err := json.Unmarshal(scanner.Bytes(), &act); err != nil {
			bt.handleError(w, err.Error(), http.StatusBadRequest)
			return
		}

		var action string
		var m common.MapStr
		for _, a := range []string{"index", "create", "delete", "update"} {
			var ok bool
			if m, ok = act[a]; ok {
				action = a
				break
			}
		}
		if i, ok := vars["index"]; ok && coerceString(m["_index"]) == "" {
			m["_index"] = i
		}
		if t, ok := vars["type"]; ok && coerceString(m["_type"]) == "" {
			m["_type"] = t
		}
		iresp := indexResponse{
			Index: coerceString(m["_index"]),
			Type:  coerceString(m["_type"]),
			ID:    coerceString(m["_id"]),
		}
		switch action {
		case "index", "create":
			m["origin"] = originAddr(r)
			event := beat.Event{
				Timestamp: time.Now(),
				Meta:      m,
			}
			if !scanner.Scan() {
				bt.handleError(w, "document missing", http.StatusBadRequest)
				return
			}
			if err := json.Unmarshal(scanner.Bytes(), &event.Fields); err != nil {
				resp.Errors = true
				iresp.Result = err.Error()
				iresp.Status = http.StatusBadRequest
			}
			bt.client.Publish(event)
			iresp.Result = "created"
			iresp.Status = http.StatusCreated
		case "delete":
			resp.Errors = true
			iresp.Result = "unauthorized"
			iresp.Status = http.StatusUnauthorized
		case "update":
			if !scanner.Scan() {
				bt.handleError(w, "document missing", http.StatusBadRequest)
				return
			}
			resp.Errors = true
			iresp.Result = "not_implemented"
			iresp.Status = http.StatusNotImplemented
		default:
			resp.Errors = true
			iresp.Result = "unknown_action"
			iresp.Status = http.StatusBadRequest
		}
		resp.Items = append(resp.Items, bulkItemResponse{action: iresp})
	}

	if err := scanner.Err(); err != nil {
		resp.Errors = true
		bt.handleError(w, err.Error(), http.StatusBadRequest)
		return
	}
	resp.Took = time.Since(start).Nanoseconds() / 1000000
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	enc := json.NewEncoder(w)
	if err := enc.Encode(resp); err != nil {
		bt.logger.Errorf("error encoding response: %s", err.Error())
	}
}

func coerceString(v interface{}) string {
	switch v.(type) {
	case string:
		return v.(string)
	case fmt.Stringer:
		return v.(fmt.Stringer).String()
	}
	return ""
}

func (bt *Ingestbeat) pingHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, `{
  "name" : "ingestbeat",
  "cluster_name" : "ingestbeat",
  "cluster_uuid" : "45SieFHisfD5SAS",
  "version" : {
    "number" : "5.6.9",
    "build_hash" : "877a590",
    "build_date" : "2018-04-12T16:25:14.838Z",
    "build_snapshot" : false,
    "lucene_version" : "6.6.1"
  },
  "tagline" : "You Know, for Search"
}`)
}

func (bt *Ingestbeat) headHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
}

func (bt *Ingestbeat) xpackHandler(w http.ResponseWriter, r *http.Request) {
	// yes, of course x-pack is enabled!
	// (this lets us receive monitoring from beats)
	fmt.Fprintf(w, `{
  "features": {
    "monitoring" : {
      "description" : "Monitoring for the Elastic Stack",
      "available" : true,
      "enabled" : true
    }
  },
  "tagline" : "You know, for X"
}`)
}

func (bt *Ingestbeat) handleError(w http.ResponseWriter, reason string, statusCode int) {
	errTpl := `{"error":{"type":"ingestbeat_error","reason":"%s"},"status":%d}`
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	fmt.Fprintf(w, errTpl, reason, statusCode)
}
