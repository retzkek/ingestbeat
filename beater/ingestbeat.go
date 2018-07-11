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
	r.PathPrefix("/_bulk").Methods("GET", "POST").HandlerFunc(bt.bulkHandler)
	r.Path("/").Methods("GET").HandlerFunc(bt.pingHandler)
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

func (bt *Ingestbeat) bulkHandler(w http.ResponseWriter, r *http.Request) {
	type action map[string]common.MapStr
	scanner := bufio.NewScanner(r.Body)
	nextIsAction := true
	var act action
	for scanner.Scan() {
		if nextIsAction {
			if err := json.Unmarshal(scanner.Bytes(), &act); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
		} else {
			if m, ok := act["index"]; ok {
				m["origin"] = originAddr(r)
				event := beat.Event{
					Timestamp: time.Now(),
					Meta:      m,
				}
				if err := json.Unmarshal(scanner.Bytes(), &event.Fields); err != nil {
					http.Error(w, err.Error(), http.StatusBadRequest)
					return
				}
				bt.client.Publish(event)
			} else if _, ok := act["create"]; ok {
				http.Error(w, "create not implemented", http.StatusNotImplemented)
				return

			} else if _, ok := act["delete"]; ok {
				http.Error(w, "delete not authorized", http.StatusUnauthorized)
				return
			} else if _, ok := act["update"]; ok {
				http.Error(w, "update not implemented", http.StatusNotImplemented)
				return
			} else {
				http.Error(w, fmt.Sprintf("unrecoginized action %v", act), http.StatusBadRequest)
				return
			}
		}
		nextIsAction = !nextIsAction

	}
	if err := scanner.Err(); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	fmt.Fprint(w, "OK")
}

type Document struct {
	Index    string                 `json:"_index"`
	Type     string                 `json:"_type"`
	ID       string                 `json:"_id"`
	Received string                 `json:"_received"`
	Origin   string                 `json:"_origin"`
	Data     map[string]interface{} `json:"data"`
}

func (d *Document) String() string {
	b, err := json.Marshal(d)
	if err != nil {
		return fmt.Sprintf("Index:%s, Type:%s, ID:%s", d.Index, d.Type, d.ID)
	}
	return string(b)
}

var pingResponse = `{
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
}`

func (bt *Ingestbeat) pingHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprint(w, pingResponse)
}
