package metrics

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
)

const (
	defaultLabel = "test"
)

func TestMetrics(t *testing.T) {

	go func() {
		assert.NoError(t, http.ListenAndServe(":8888", nil))
	}()

	defaultMetrics.ClosedConnectionCount.WithLabelValues(defaultLabel).Inc()
	defaultMetrics.CurrentMQTTConnectionCount.WithLabelValues(defaultLabel).Inc()
	defaultMetrics.NewConnectionCount.WithLabelValues(defaultLabel).Inc()
	defaultMetrics.CurrentTCPConnectionCount.Inc()
	// defaultMetrics.DownPacketCount.WithLabelValues(defaultLabel, "connect").Inc()
	// defaultMetrics.UpPacketCount.WithLabelValues(defaultLabel, "connect").Inc()
	defaultMetrics.DownPacketSizeHistogramVec.WithLabelValues(defaultLabel, "connect").Observe(0)
	defaultMetrics.UpPacketSizeHistogramVec.WithLabelValues(defaultLabel, "connect").Observe(9)

}
