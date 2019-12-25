package status

import (
	_ "expvar"
	"net/http"
	_ "net/http/pprof"

	"github.com/prometheus/client_golang/prometheus"
)

const (
	namespace = "bifrost"
	subsystem = "pushd"
	leader    = "leader"
	gckeys    = "gckeys"
)

var (
	M              = &Metrics{}
	messageLabels  = []string{"StatLabel", "message"}
	publishLabels  = []string{"StatLabel", "message", "status"}
	callbackLabels = []string{"StatLabel", "callback"}
	leaderLabel    = []string{leader}
	gcKeysLabel    = []string{gckeys}
)

type Metrics struct {
	//message
	MessageCounterVec       *prometheus.CounterVec
	MessageSizeHistogramVec *prometheus.HistogramVec
	MessageHistogramVec     *prometheus.HistogramVec

	//callback cost
	CallbackHistogramVec *prometheus.HistogramVec

	//gc
	GCKeysCounterVec *prometheus.CounterVec
	IsLeaderGaugeVec *prometheus.GaugeVec
}

func init() {
	M.MessageCounterVec = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "message_total",
			Help:      "the total messages pushd routed",
		}, messageLabels,
	)
	prometheus.MustRegister(M.MessageCounterVec)

	M.MessageHistogramVec = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "message_duration_seconds",
			Help:      "the cost time of processed message",
		}, publishLabels,
	)
	prometheus.MustRegister(M.MessageHistogramVec)

	M.MessageSizeHistogramVec = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "message_size_bytes",
			Help:      "the size of processed message",
		}, messageLabels,
	)
	prometheus.MustRegister(M.MessageSizeHistogramVec)

	M.CallbackHistogramVec = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "callback_duration_seconds",
			Help:      "the time cost of callback function",
			Buckets:   prometheus.ExponentialBuckets(0.005, 2, 10),
		}, callbackLabels,
	)
	prometheus.MustRegister(M.CallbackHistogramVec)

	M.GCKeysCounterVec = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "gc_keys_total",
			Help:      "the number of gc keys added or deleted",
		}, gcKeysLabel)
	prometheus.MustRegister(M.GCKeysCounterVec)

	M.IsLeaderGaugeVec = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "is_leader",
			Help:      "mark bifrost is leader for gc",
		}, leaderLabel)
	prometheus.MustRegister(M.IsLeaderGaugeVec)
}

func HandleMetrics() {
	http.Handle("/metrics", prometheus.Handler())
}

func GetMertics() *Metrics {
	return M
}
