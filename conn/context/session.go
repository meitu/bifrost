package context

import (
	"bytes"
	"sync"
	"sync/atomic"
	"time"

	"github.com/arthurkiller/mqtt-go/packets"
	"github.com/meitu/bifrost/conn/poll"

	pb "github.com/meitu/bifrost/grpc/push"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// PullInfo records the interval in which the client pulls messages [)
// Cursor currently fetches the message location
// Latest the message queue assigns the location of the next message
// state 0 init
// state 1 notify
type PullInfo struct {
	Cursor []byte
	Latest []byte
	QoS    byte
	State  int32
}

type SessionState int32

const (
	ConnectState SessionState = iota
	RunningState
	ClosedState
	DestroyState
)

type WorkState int32

const (
	Idle WorkState = iota
	Busy
)

type SubState int32

const (
	Subscribing SubState = iota
	Subscribed
)

// SessionContext combines the client and server context
type SessionContext struct {
	*MqttSrvContext

	packetID     int64
	ClientID     string
	Service      string
	StatLabel    string
	CleanSession bool
	Kick         bool
	state        SessionState

	writeBufferPackets []packets.ControlPacket
	writePacketMutex   sync.RWMutex
	writeInfo          map[string]SubState
	writeInfoMutex     sync.RWMutex
	writeState         WorkState

	PullMutex sync.RWMutex // mutex should guide fields bellow
	PullInfo  map[string]*PullInfo
	pullState WorkState
	Conn      *poll.Conn
	Keepalive time.Duration
}

// NewSessionContext return a new SessionContext
func NewSessionContext(mctx *MqttSrvContext, conn *poll.Conn) (*SessionContext, CancelFunc) {
	ctx, cancel := mctx.WithCancel()
	return &SessionContext{
		MqttSrvContext:     ctx,
		Conn:               conn,
		Keepalive:          ctx.Conf.DefaultKeepalive,
		PullInfo:           make(map[string]*PullInfo),
		writeBufferPackets: make([]packets.ControlPacket, 0, ctx.Conf.WriteChanSize),
		writeInfo:          make(map[string]SubState),
		state:              ConnectState,
		writeState:         Idle,
	}, cancel
}

func (ctx *SessionContext) LoadState() SessionState {
	return (SessionState)(atomic.LoadInt32((*int32)(&ctx.state)))
}

func (ctx *SessionContext) CompareState(state SessionState) bool {
	return ctx.LoadState() == state
}

func (ctx *SessionContext) StoreState(state SessionState) {
	atomic.StoreInt32(((*int32)(&ctx.state)), (int32)(state))
}

func (ctx *SessionContext) LoadWriteState() WorkState {
	return (WorkState)(atomic.LoadInt32((*int32)(&ctx.writeState)))
}

func (ctx *SessionContext) CompareWriteState(state WorkState) bool {
	return ctx.LoadWriteState() == state
}

func (ctx *SessionContext) CompareAndSwapWriteState(olds, news WorkState) bool {
	return atomic.CompareAndSwapInt32((*int32)(&ctx.writeState), (int32)(olds), (int32)(news))
}

func (ctx *SessionContext) StoreWriteState(state WorkState) {
	atomic.StoreInt32(((*int32)(&ctx.writeState)), (int32)(state))
}

// WithContext return the base context
func (ctx *SessionContext) WithContext() *Context {
	return ctx.Context
}

// LogFields return a slice fields
// Specify the order of printing ,print ctx then fs
func (ctx *SessionContext) LogFields(fs ...zapcore.Field) []zap.Field {
	fields := make([]zapcore.Field, 0, len(fs)+6)
	fields = append(fields, zap.Stringer("addr", ctx.Conn.RemoteAddr()))
	fields = append(fields, zap.Int("fd", ctx.Conn.FD()))
	if ctx.ClientID != "" {
		fields = append(fields, zap.String("clientid", ctx.ClientID))
	}
	if ctx.Service != "" {
		fields = append(fields, zap.String("service", ctx.Service))
	}
	fields = append(fields, zap.Bool("cleansession", ctx.CleanSession))
	fields = append(fields, zap.Duration("keepalive", ctx.Keepalive))
	return append(fields, fs...)
}

// WritePublishPackets send data to the write chan
// return true in the position of 0 otherwise false
func (ctx *SessionContext) WritePublishPackets(pkgs []*packets.PublishPacket, noWriteHandler func() bool) {
	releaseWrite := true
	ctx.writeInfoMutex.RLock()
	ctx.writePacketMutex.Lock()
	if noWriteHandler == nil || len(ctx.writeBufferPackets) != 0 || noWriteHandler() {
		releaseWrite = false
	}

	for _, pkg := range pkgs {
		if state, ok := ctx.writeInfo[pkg.TopicName]; ok && state == Subscribed {
			ctx.writeBufferPackets = append(ctx.writeBufferPackets, pkg)
		}
	}

	if releaseWrite {
		//The process of writing from nothing requires the release of write events
		ctx.Poll.ModReadWrite(poll.NewEvent(ctx.Conn.FD()))
	}
	ctx.writePacketMutex.Unlock()
	ctx.writeInfoMutex.RUnlock()
}

func (ctx *SessionContext) GetWriteInfo(topic string) bool {
	ctx.writeInfoMutex.RLock()
	if _, ok := ctx.writeInfo[topic]; !ok {
		ctx.writeInfo[topic] = Subscribing
		ctx.writeInfoMutex.RUnlock()
		return false
	}
	ctx.writeInfoMutex.RUnlock()
	return true
}

// WritePacket send a package to client
func (ctx *SessionContext) WritePacket(pkg packets.ControlPacket, noWriteHandler func() bool) {
	releaseWrite := true

	ctx.writePacketMutex.Lock()
	if noWriteHandler == nil || len(ctx.writeBufferPackets) != 0 || noWriteHandler() {
		releaseWrite = false
	}

	ctx.writeBufferPackets = append(ctx.writeBufferPackets, pkg)
	if releaseWrite {
		ctx.Poll.ModReadWrite(poll.NewEvent(ctx.Conn.FD()))
	}
	ctx.writePacketMutex.Unlock()
}

// GetAndClearWriteBuffer return write buffer and clear write buffer
func (ctx *SessionContext) GetAndClearWriteBuffer() []packets.ControlPacket {
	ctx.writePacketMutex.Lock()
	if len(ctx.writeBufferPackets) == 0 {
		ctx.writePacketMutex.Unlock()
		return nil
	}
	// Spatial reuse
	wpkg := make([]packets.ControlPacket, len(ctx.writeBufferPackets))
	copy(wpkg, ctx.writeBufferPackets)
	ctx.writeBufferPackets = ctx.writeBufferPackets[:0]
	ctx.writePacketMutex.Unlock()
	return wpkg
}

// IsWrite When the write buffer is not empty return true otherwise return false
func (ctx *SessionContext) IsWrite(noWriteHandler func(), writeHandler func()) bool {
	ctx.writePacketMutex.RLock()
	if len(ctx.writeBufferPackets) != 0 {
		if writeHandler != nil {
			writeHandler()
		}
		ctx.writePacketMutex.RUnlock()
		return true
	}
	if noWriteHandler != nil {
		noWriteHandler()
	}
	ctx.writePacketMutex.RUnlock()
	return false
}

// GetPacketID get a packet id
func (ctx *SessionContext) GetPacketID() uint16 {
	var pid uint16
	for pid == 0 {
		pid = uint16(atomic.AddInt64(&ctx.packetID, 1) & 0xFFFF)
	}
	return pid
}

// SetPacketID set the starting point for the assignment package ids
func (ctx *SessionContext) SetPacketID(i int64) {
	atomic.StoreInt64(&ctx.packetID, i)
}

// CloneWriteInfo return a slice topics that is cloned from the writeinfo
func (ctx *SessionContext) CloneWriteInfo() []string {
	ctx.writeInfoMutex.RLock()
	topics := make([]string, 0, len(ctx.writeInfo))
	for topic := range ctx.writeInfo {
		topics = append(topics, topic)
	}
	ctx.writeInfoMutex.RUnlock()
	return topics
}

// UnsubUpdateInfo clean up the subscription list information by using the topics parameter
func (ctx *SessionContext) UnsubUpdateInfo(topics []string) {
	ctx.PullMutex.Lock()
	for _, k := range topics {
		delete(ctx.PullInfo, k)
	}
	ctx.PullMutex.Unlock()

	ctx.writeInfoMutex.Lock()
	for _, k := range topics {
		delete(ctx.writeInfo, k)
	}
	ctx.writeInfoMutex.Unlock()
}

// SubUpdateInfo update the subscribe list infomation
// qoss == 0x80 invalid subscription
// TODO
// If topic is subscribed to update all information
func (ctx *SessionContext) SubUpdateInfo(topics []string, indexs [][]byte, qoss []int32) {
	ctx.PullMutex.Lock()
	for i := range indexs {
		if qoss[i] == 0x80 {
			continue
		}
		record, ok := ctx.PullInfo[topics[i]]
		if ok {
			record.QoS = byte(qoss[i])
			record.Latest = indexs[i]
			continue
		}

		ctx.PullInfo[topics[i]] = &PullInfo{
			Cursor: indexs[i],
			Latest: indexs[i],
			QoS:    byte(qoss[i]),
		}
	}
	ctx.PullMutex.Unlock()

	ctx.writeInfoMutex.Lock()
	for _, t := range topics {
		ctx.writeInfo[t] = Subscribed
	}
	ctx.writeInfoMutex.Unlock()
}

// RecoveryUpdateInfo TODO bug
// the subscription can only be made when the painting restoration is complete
func (ctx *SessionContext) RecoveryUpdateInfo(r *pb.Record) bool {
	busy := false
	ctx.PullMutex.Lock()
	ctx.PullInfo[r.Topic] = &PullInfo{
		QoS:    byte(r.Qos),
		Cursor: r.CurrentIndex,
		Latest: r.LastestIndex,
	}
	if bytes.Compare(r.CurrentIndex, r.LastestIndex) == -1 {
		ctx.StorePullState(Busy)
		busy = true
	}
	ctx.PullMutex.Unlock()

	ctx.writeInfoMutex.Lock()
	ctx.writeInfo[r.Topic] = Subscribed
	ctx.writeInfoMutex.Unlock()
	return busy
}

// UpdatePullInfo update pulling list when last is greater than latest
// return true when update sucessfully otherwise return false
func (ctx *SessionContext) UpdatePullInfo(topic string, last []byte) bool {
	ctx.PullMutex.Lock()
	v, ok := ctx.PullInfo[topic]
	if !ok || bytes.Compare(last, v.Latest) != 1 {
		ctx.PullMutex.Unlock()
		return false
	}
	v.Latest = last
	v.State = 1
	busy := ctx.CompareAndSwapPullState(Idle, Busy)
	ctx.PullMutex.Unlock()
	return busy
}

// GetNeedPullInfo get a list that cursor is greater than the latest in the pullinfo map
func (ctx *SessionContext) GetNeedPullInfo() map[string]PullInfo {
	ctx.PullMutex.RLock()
	records := make(map[string]PullInfo, len(ctx.PullInfo))
	for topic, record := range ctx.PullInfo {
		if bytes.Compare(record.Cursor, record.Latest) == -1 {
			records[topic] = *record
		}
	}
	if len(records) == 0 {
		ctx.StorePullState(Idle)
		ctx.PullMutex.RUnlock()
		return nil
	}
	ctx.PullMutex.RUnlock()
	return records
}

// UpdatePullInfoCursor update cursor in the pullinfo map
func (ctx *SessionContext) UpdatePullInfoCursor(cursors map[string][]byte) {
	ctx.PullMutex.Lock()
	for topic, cursor := range cursors {
		if info, ok := ctx.PullInfo[topic]; ok {
			info.Cursor = cursor
		}
	}
	ctx.PullMutex.Unlock()
}

func (ctx *SessionContext) UpdateResumePullInfo(cursors map[string][]byte, completes map[string]bool) {
	// complete update cursors and latest
	ctx.PullMutex.Lock()
	for topic, cursor := range cursors {
		if info, ok := ctx.PullInfo[topic]; ok {
			info.Cursor = cursor
			// still in the initialization phase, reset latest position
			if info.State == 0 && completes[topic] {
				info.Latest = cursor
			}
		}
	}
	ctx.PullMutex.Unlock()
}

func (s *SessionContext) StorePullState(state WorkState) {
	atomic.StoreInt32((*int32)(&s.pullState), (int32)(state))
}

func (s *SessionContext) ComparePullState(expect WorkState) bool {
	if !s.CompareState(RunningState) {
		return false
	}
	if (WorkState)(atomic.LoadInt32((*int32)(&s.pullState))) != expect {
		return false
	}
	return true
}

func (s *SessionContext) CompareAndSwapPullState(olds, news WorkState) bool {
	return atomic.CompareAndSwapInt32((*int32)(&s.pullState), (int32)(olds), (int32)(news))
}
