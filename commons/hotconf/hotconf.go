package hotconf

import (
	"encoding/json"
	"expvar"
	"io/ioutil"
	"net/http"
	"strings"
)

const PathPrefix = "/hotconf/"

// HotConfHandler handle the change event of a key, and return the current value
type HotConfHandler func(key, val string) (interface{}, error)

// HotConf means the config option can be hot changed without restarting the process
type HotConf interface {
	// Handle should be guaranteed thread-safe by the user
	Handle(key string, currentValue interface{}, h HotConfHandler)
}

// ConfValue is the value set by hotconf
type ConfValue struct {
	Pre     interface{}
	Current interface{}
	Default interface{}
}

func (cv *ConfValue) String() string {
	data, _ := json.Marshal(cv)
	return string(data)
}

// An implementation of HotConf interface by HTTP
type httpHotConf struct {
	s        *http.ServeMux
	kv       map[string]*ConfValue
	handlers map[string]HotConfHandler
}

// URL: /hotconf/:key?querystring
// Get Value:
//     Get /hotconf/:key
// Set Value:
//     POST /hotconf/:key
//     with json encoded body
func (c *httpHotConf) liveChange(w http.ResponseWriter, req *http.Request) {
	path := req.RequestURI
	if !strings.HasPrefix(path, PathPrefix) {
		// It should be not possible to be here
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	key := path[len(PathPrefix):]

	if req.Method == http.MethodGet {
		if strings.TrimSpace(key) == "" {
			c.list(w)
			return
		}
		c.get(key, w)
	} else if req.Method == http.MethodPost {
		val, err := ioutil.ReadAll(req.Body)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		c.set(key, string(val), w)
	} else {
		w.WriteHeader(http.StatusMethodNotAllowed)
	}

}

func (c *httpHotConf) list(w http.ResponseWriter) {
	data, err := json.Marshal(c.kv)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write(data)
}
func (c *httpHotConf) get(key string, w http.ResponseWriter) {
	if val := c.kv[key]; val != nil {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(val.String()))
		return
	}
	w.WriteHeader(http.StatusNotFound)
}
func (c *httpHotConf) set(key, val string, w http.ResponseWriter) {
	if f := c.handlers[key]; f != nil {
		current, err := f(key, val)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(err.Error()))
			return
		}
		cv := c.kv[key]
		if cv == nil {
			// should not be here
			return
		}

		cv.Pre = cv.Current
		cv.Current = current
		return
	}
	w.WriteHeader(http.StatusNotFound)
}

func (c *httpHotConf) String() string {
	data, _ := json.Marshal(c.kv)
	return string(data)
}

func (c *httpHotConf) Handle(key string, currentValue interface{}, h HotConfHandler) {
	c.kv[key] = &ConfValue{Current: currentValue, Default: currentValue}
	c.handlers[key] = h
}

func New() HotConf {
	// Use the default multiplexer
	hc := &httpHotConf{s: http.DefaultServeMux,
		kv:       make(map[string]*ConfValue),
		handlers: make(map[string]HotConfHandler)}

	hc.s.HandleFunc("/hotconf/", hc.liveChange)

	expvar.Publish("hotconf", hc)
	return hc
}

var hc HotConf

//register to the default http server
func init() {
	hc = New()
}

func Handle(key string, currentValue interface{}, h HotConfHandler) {
	hc.Handle(key, currentValue, h)
}
