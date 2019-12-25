package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const (
	namespace = "bifrost"
	subsystem = "connd"
)

var (
	labels         = []string{"statLabel"}
	grpcLabels     = []string{"grpc"}
	packetLabels   = []string{"statLabel", "packetType"}
	defaultMetrics *Metrics
)

// Metrics monitoring management
type Metrics struct {
	// Connection gauge
	CurrentTCPConnectionCount  prometheus.Gauge
	CurrentMQTTConnectionCount *prometheus.GaugeVec

	// MQTT defaultMetrics
	NewConnectionCount    *prometheus.CounterVec
	ClosedConnectionCount *prometheus.CounterVec

	// packets histogram
	// UpPacketCount              *prometheus.CounterVec
	// DownPacketCount            *prometheus.CounterVec
	UpPacketSizeHistogramVec   *prometheus.HistogramVec
	DownPacketSizeHistogramVec *prometheus.HistogramVec

	CacheCount     prometheus.Counter
	CacheHintCount prometheus.Counter

	// GRPC
	GrpcHistogramVec         *prometheus.HistogramVec
	TopicScanDuration        prometheus.Histogram
	ConnectionOnlineDuration *prometheus.HistogramVec

	ClientPullDuration *prometheus.HistogramVec
	ClientPullCount    *prometheus.GaugeVec

	// panic due to an unknown error
	ClientPanicCount prometheus.Counter
}

func init() {
	defaultMetrics = &Metrics{}

	defaultMetrics.CurrentTCPConnectionCount = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "tcp_connect_count",
			Help:      "the total count of current tcp connection",
		},
	)
	prometheus.MustRegister(defaultMetrics.CurrentTCPConnectionCount)

	defaultMetrics.CurrentMQTTConnectionCount = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "mqtt_connect_count",
			Help:      "the total count of current mqtt connection",
		}, labels,
	)
	prometheus.MustRegister(defaultMetrics.CurrentMQTTConnectionCount)

	defaultMetrics.ClosedConnectionCount = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "close_connect_count",
			Help:      "the total count of close connection",
		}, labels,
	)
	prometheus.MustRegister(defaultMetrics.ClosedConnectionCount)

	defaultMetrics.NewConnectionCount = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "new_connect_count",
			Help:      "the total count of new connection",
		}, labels,
	)
	prometheus.MustRegister(defaultMetrics.NewConnectionCount)

	/*
			defaultMetrics.UpPacketCount = prometheus.NewCounterVec(
				prometheus.CounterOpts{
					Namespace: namespace,
					Subsystem: subsystem,
					Name:      "up_packet_count",
					Help:      "the total count of upload packet count",
				}, packetLabels,
			)
			prometheus.MustRegister(defaultMetrics.UpPacketCount)

		defaultMetrics.DownPacketCount = prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: namespace,
				Subsystem: subsystem,
				Name:      "down_packet_count",
				Help:      "the total count of download packet count",
			}, packetLabels,
		)
		prometheus.MustRegister(defaultMetrics.DownPacketCount)
	*/

	defaultMetrics.UpPacketSizeHistogramVec = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "up_message_size_bytes",
			Help:      "the size of packet message",
		}, packetLabels,
	)
	prometheus.MustRegister(defaultMetrics.UpPacketSizeHistogramVec)

	defaultMetrics.DownPacketSizeHistogramVec = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "down_message_size_bytes",
			Help:      "the size of packet message",
		}, packetLabels,
	)
	prometheus.MustRegister(defaultMetrics.DownPacketSizeHistogramVec)

	defaultMetrics.CacheCount = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "cache_count",
			Help:      "the total count of cache",
		},
	)
	prometheus.MustRegister(defaultMetrics.CacheCount)

	defaultMetrics.CacheHintCount = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "cache_hint_count",
			Help:      "the total count of cache hint",
		},
	)
	prometheus.MustRegister(defaultMetrics.CacheHintCount)

	defaultMetrics.ConnectionOnlineDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "connection_duration_seconds", //NOTE changed name to connection_duration_seconds
			Help:      "the client connection's online duration",
			Buckets:   prometheus.ExponentialBuckets(0.005, 2, 10),
		}, labels,
	)
	prometheus.MustRegister(defaultMetrics.ConnectionOnlineDuration)

	defaultMetrics.ClientPullDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "client_pull_duration_seconds",
			Help:      "the client pull message duration",
			Buckets:   prometheus.ExponentialBuckets(0.005, 2, 10),
		}, labels,
	)
	prometheus.MustRegister(defaultMetrics.ClientPullDuration)

	defaultMetrics.TopicScanDuration = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "topic_scan_duration_seconds",
			Help:      "the scan topic online duration",
			Buckets:   prometheus.ExponentialBuckets(0.005, 2, 10),
		},
	)
	prometheus.MustRegister(defaultMetrics.TopicScanDuration)

	defaultMetrics.GrpcHistogramVec = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "grpc_duration_seconds",
			Help:      "the time cost of grpc function",
			Buckets:   prometheus.ExponentialBuckets(0.005, 2, 10),
		}, grpcLabels,
	)
	prometheus.MustRegister(defaultMetrics.GrpcHistogramVec)

	defaultMetrics.ClientPullCount = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "client_pull_count",
			Help:      "the total count of pull messages",
		}, labels,
	)
	prometheus.MustRegister(defaultMetrics.ClientPullCount)

	defaultMetrics.ClientPanicCount = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "client_panic_count",
			Help:      "the total count of connection panic",
		},
	)
	prometheus.MustRegister(defaultMetrics.ClientPanicCount)
}

// GetMetrics get default a metrics object
func GetMetrics() *Metrics {
	return defaultMetrics
}

// HandleMetrics register handler
func Handler() {
	http.Handle("/metrics", promhttp.Handler())
}
