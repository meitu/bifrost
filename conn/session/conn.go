package session

import (
	"sync"
	"time"

	"github.com/arthurkiller/mqtt-go/packets"
	"github.com/meitu/bifrost/commons/log"
	"github.com/meitu/bifrost/conn/context"
	"github.com/meitu/bifrost/conn/status/metrics"
	"go.uber.org/zap"
)

// Conn is used for write/read operations on MQTT packages
type Conn struct {
	ctx     *context.SessionContext
	tlock   sync.Mutex
	timeout *time.Timer
}

// NewConn create a new object
func NewConn(ctx *context.SessionContext) *Conn {
	return &Conn{
		ctx:     ctx,
		timeout: time.NewTimer(ctx.Conf.DefaultKeepalive),
	}
}

// readNoDeadlinePacket read a packets
// No read, write timeout is required.
// Session itself manages read-write timeouts
func (c *Conn) readPacket() (packets.ControlPacket, int, error) {
	c.reset()
	return packets.ReadPacketLimitSize(c.ctx.Conn, c.ctx.Conf.PacketSizeLimit)
}

//  No write timeout is set. blocking occurs if client read buf is full and service write buf is full
func (c *Conn) writePacket(pkg packets.ControlPacket) error {
	if chk := log.Check(zap.DebugLevel, "write package"); chk != nil {
		chk.Write(c.ctx.LogFields(
			zap.String("type", packets.PacketNames[pkg.Type()]),
			zap.Stringer("pkg", pkg),
		)...)
	}
	c.reset()
	//Maybe statable is null
	ll, err := pkg.Write(c.ctx.Conn)
	// metrics.GetMetrics().DownPacketCount.WithLabelValues(c.ctx.StatLabel, packets.PacketNames[pkg.Type()]).Inc()
	if err != nil {
		return err
	}
	metrics.GetMetrics().DownPacketSizeHistogramVec.WithLabelValues(c.ctx.StatLabel, packets.PacketNames[pkg.Type()]).Observe(float64(ll))
	return nil
}

// Timer the client reads and writes timeout
func (c *Conn) Timer(handler func() error) {
	for {
		select {
		case <-c.timeout.C:
			// Read and write timeout to close the client
			log.Error("session timeout", c.ctx.LogFields()...)
			handler()
			return
		case <-c.ctx.Done():
			// context close
			log.Info("session pull close", c.ctx.LogFields()...)
			return
		}
	}
}

// time timer.Reset is not thread safe
func (c *Conn) reset() {
	c.tlock.Lock()
	if !c.timeout.Stop() {
		// Multiple gorontine wait for signal release
		select {
		case <-c.timeout.C:
		default:
		}
	}
	c.timeout.Reset(c.ctx.Keepalive)
	c.tlock.Unlock()
}

func (c *Conn) Close() {
	c.timeout.Stop()
}
