package session

import (
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/arthurkiller/mqtt-go/packets"
	"github.com/meitu/bifrost/commons/log"
	"github.com/meitu/bifrost/conn/context"
	"github.com/meitu/bifrost/conn/poll"
	"github.com/meitu/bifrost/conn/status/metrics"
	pb "github.com/meitu/bifrost/grpc/push"
	"github.com/twinj/uuid"
	"go.uber.org/zap"
)

var (
	// ErrClosed The client side is normally closed
	ErrClosed = errors.New("recv a the disconnected packet")
)

// Session link painting information
type Session struct {
	conn      *Conn
	processor *processor

	ctx       *context.SessionContext
	cancel    context.CancelFunc
	once      sync.Once
	birthtime time.Time

	processing context.WorkState
	reading    context.WorkState

	readPacketMutex   sync.Mutex
	readBufferPackets []packets.ControlPacket
}

// NewSession create a new session object
func NewSession(ctx *context.MqttSrvContext, c *poll.Conn) *Session {
	cctx, cancel := context.NewSessionContext(ctx, c)

	s := &Session{
		ctx:        cctx,
		cancel:     cancel,
		birthtime:  time.Now(),
		once:       sync.Once{},
		conn:       NewConn(cctx),
		reading:    context.Idle,
		processing: context.Idle,
	}

	// init procesessor
	s.processor = NewProcessor(cctx, s.Notify)

	metrics.GetMetrics().CurrentTCPConnectionCount.Inc()
	if env := log.Check(zap.DebugLevel, "recv a connection"); env != nil {
		env.Write(s.ctx.LogFields(zap.String("birthtime", s.birthtime.Format("2006-01-02 15:04:05")))...)
	}

	// start read and write timeout
	go s.conn.Timer(s.Close)
	return s
}

// KickClose used to kick off the connection operation
// maybe a bug
// The second shutdown failed because of the once operation, and the first shutdown was incomplete
// If the store USES transactional operations, there is no bug
func (s *Session) KickClose(cid int64) bool {
	if cid == int64(s.ctx.Conn.FD()) {
		s.ctx.Kick = true
		s.Close()
		return true
	}
	return false
}

// Close close session and clean information
func (s *Session) Close() error {
	s.once.Do(func() {
		// clear fd in poll
		state := s.ctx.LoadState()
		if state != context.ConnectState {
			s.ctx.RemoveSession(s.ctx.Conn.FD())
			// epoll automatically removes the fd that is turned off
			if err := s.ctx.Poll.Remove(s.ctx.Conn.FD(), nil); err != nil {
				log.Error("remove fd failed", s.ctx.LogFields(zap.Error(err))...)
			}
		}

		s.ctx.Conn.Close()
		s.cancel()
		s.ctx.StoreState(context.ClosedState)

		// clear register info
		s.waitExit()
		s.processor.handleDirty(state)
		s.ctx.Clients.Deregister(s.ctx.Service + s.ctx.ClientID)
		s.conn.Close()
		metrics.GetMetrics().CurrentTCPConnectionCount.Dec()
		if state != context.ConnectState {
			metrics.GetMetrics().CurrentMQTTConnectionCount.WithLabelValues(s.ctx.StatLabel).Dec()
			metrics.GetMetrics().ClosedConnectionCount.WithLabelValues(s.ctx.StatLabel).Inc()
			metrics.GetMetrics().ConnectionOnlineDuration.WithLabelValues(s.ctx.StatLabel).Observe(time.Since(s.birthtime).Seconds())
		}
		s.ctx.StoreState(context.DestroyState)

		// record client close
		if chk := log.Check(zap.InfoLevel, "close client"); chk != nil {
			chk.Write(s.ctx.LogFields()...)
		}
	})

	for !s.ctx.CompareState(context.DestroyState) {
		time.Sleep(time.Millisecond * 10)
	}

	return nil
}

func (s *Session) waitExit() {
	for (context.WorkState)(atomic.LoadInt32((*int32)(&s.processing))) != context.Idle {
		time.Sleep(time.Millisecond * 10)
	}

	for (context.WorkState)(atomic.LoadInt32((*int32)(&s.reading))) != context.Idle {
		time.Sleep(time.Millisecond * 10)
	}

	for s.ctx.ComparePullState(context.Busy) {
		time.Sleep(time.Millisecond * 10)
	}

	for s.ctx.CompareWriteState(context.Busy) {
		time.Sleep(time.Millisecond * 10)
	}
}

func (s *Session) handlePacket(p packets.ControlPacket) error {
	//Connect
	switch s.State() {
	case context.ConnectState:
		pkg, ok := p.(*packets.ConnectPacket)
		if !ok {
			return errors.New("the first packet is not the connect packet")
		}
		ack, pulling, err := s.processor.handleConnect(pkg)

		if ack != nil {
			if err := s.conn.writePacket(ack); err != nil {
				log.Error("write connack failed", s.ctx.LogFields(zap.Error(err))...)
				return err
			}
		}

		if err != nil {
			log.Error("handle connect failed", s.ctx.LogFields(zap.Error(err))...)
			return err
		}

		s.ctx.Clients.Register(s.ctx.Service+s.ctx.ClientID, s)
		s.ctx.StoreState(context.RunningState)
		s.conn.reset()
		metrics.GetMetrics().CurrentMQTTConnectionCount.WithLabelValues(s.ctx.StatLabel).Inc()
		metrics.GetMetrics().NewConnectionCount.WithLabelValues(s.ctx.StatLabel).Inc()

		// start pull goroutine
		if pulling {
			go s.pull(ack.TraceID)
		}
	case context.RunningState:
		//Disconnect
		if p.Type() == packets.Disconnect {
			if err := s.processor.handleDisconnect(p.(*packets.DisconnectPacket)); err != nil {
				log.Error("handle disconnect failed", s.ctx.LogFields(zap.Error(err))...)
				return err
			}
			s.ctx.StoreState(context.ClosedState)
			return ErrClosed
		}

		// Other packets
		if err := s.processor.handlePacket(p); err != nil {
			return err
		}
	case context.ClosedState:
		return errors.New("client has been closed")
	case context.DestroyState:
		return errors.New("client has been destroyed")
	default:
		return errors.New("unknown state")
	}
	return nil
}

// Notify receive a notification that if the latest location is greater than or equal to the lastest of the record,
// the post triggers a pull message operation
func (s *Session) Notify(topic string, last []byte) {
	//topic may be unsubscribed
	if !s.CompareState(context.RunningState) {
		return
	}
	if s.ctx.UpdatePullInfo(topic, last) {
		go s.pull("")
	}
}

// Publish pull the message and publish it
// To ensure order of messages, you need to send messages without ack before sending other messages
func (s *Session) pull(traceID string) {
	if s.CompareState(context.ClosedState) {
		s.ctx.StorePullState(context.Idle)
		return
	}
	// unack message
	resume := false

	if traceID != "" {
		if err := s.processor.handleRangeUnack(traceID); err != nil {
			log.Error("session range unack falied", s.ctx.LogFields()...)
			s.ctx.StorePullState(context.Idle)
			s.Close()
			return
		}
		resume = true
	}

	var records map[string]context.PullInfo
	// other message
	for {
		// pull
		if records = s.ctx.GetNeedPullInfo(); records == nil {
			return
		}

		traceID := uuid.NewV4().String()
		cursors := make(map[string][]byte, len(records))
		complete := make(map[string]bool)

		// publish message
		for topic, record := range records {
			start := time.Now()
			resp, err := s.ctx.Pubsub.Pull(s.ctx.WithContext(), &pb.PullReq{
				TraceID: traceID,
				Topic:   topic,
				Offset:  record.Cursor,
				Limit:   s.ctx.Conf.PullLimit,
				Service: s.ctx.Service,
			})
			metrics.GetMetrics().ClientPullDuration.WithLabelValues(s.ctx.StatLabel).Observe(time.Since(start).Seconds())

			if s.ctx.Err() != nil {
				log.Info("session pull close", s.ctx.LogFields(zap.Error(err))...)
				s.ctx.StorePullState(context.Idle)
				return
			}

			if err != nil {
				log.Error("session pull failed", s.ctx.LogFields(
					zap.String("traceid", traceID),
					zap.String("topic", topic),
					zap.Error(err),
				)...)
				continue
			}

			if err = s.sendMsg(topic, resp.Messages, traceID); err != nil {
				log.Error("session send message failed", s.ctx.LogFields(
					zap.String("traceid", traceID),
					zap.String("topic", topic),
					zap.Error(err),
				)...)
				continue
			}
			cursors[topic] = resp.Offset

			// resume record complete flag
			if resume {
				complete[topic] = resp.Complete
			}
		}
		// Painting recovery process will cause cursor fallback problems,
		// message pull complete by the complete flag judgment
		if resume {
			s.ctx.UpdateResumePullInfo(cursors, complete)
		} else {
			s.ctx.UpdatePullInfoCursor(cursors)
		}
	}
}

func (s *Session) sendMsg(topic string, msgs []*pb.Message, traceID string) error {
	// write message
	if env := log.Check(zap.InfoLevel, "pull message in session"); env != nil {
		env.Write(s.ctx.LogFields(
			zap.String("traceid", traceID),
			zap.String("topic", topic),
			zap.Int("count", len(msgs)),
		)...)
	}

	metrics.GetMetrics().ClientPullCount.WithLabelValues(s.ctx.StatLabel).Add(float64(len(msgs)))

	unackdescs := make([]*pb.UnackDesc, 0, len(msgs))
	var pkgs []*packets.PublishPacket
	for _, v := range msgs {
		pkg := packets.NewPublishPacket()
		pkg.SetTraceID(traceID)
		pkg.FixedHeader.QoS = byte(v.Qos)
		pkg.TopicName = topic
		pkg.Payload = v.Payload
		if v.Qos > 0 {
			pkg.MessageID = s.ctx.GetPacketID()
			unackdescs = append(unackdescs, &pb.UnackDesc{
				Topic:     v.Topic,
				Index:     v.Index,
				MessageID: int64(pkg.MessageID),
				BizID:     v.BizID,
				// TraceID:   traceID,
			})
		}
		pkgs = append(pkgs, pkg)
	}

	if len(unackdescs) > 0 {
		if _, err := s.processor.ack.PutUnack(s.ctx, &pb.PutUnackReq{
			TraceID:      traceID,
			ClientID:     s.ctx.ClientID,
			Messages:     unackdescs,
			Service:      s.ctx.Service,
			CleanSession: s.ctx.CleanSession,
		}); err != nil {
			return err
		}
	}

	handler := func() bool {
		return s.ctx.CompareWriteState(context.Busy)
	}
	s.ctx.WritePublishPackets(pkgs, handler)
	return nil
}

// Read an MQTT package and call back the handle function to handle it
// Close the connection if something goes wrong
func (s *Session) Read() error {
	pkg, ll, err := s.conn.readPacket()
	if err != nil {
		if err == io.EOF {
			log.Debug("read packet EOF", s.ctx.LogFields(zap.Error(err))...)
			return err
		}
		log.Error("read packet failed", s.ctx.LogFields(zap.Error(err))...)
		return err
	}
	ptype := pkg.Type()

	// if the type of pkg is ping ,handle returning pong directly
	if ptype == packets.Pingreq || ptype == packets.Connect {
		metrics.GetMetrics().UpPacketSizeHistogramVec.WithLabelValues(s.ctx.StatLabel, packets.PacketNames[ptype]).Observe(float64(ll))
		return s.handlePacket(pkg)
	}

	metrics.GetMetrics().UpPacketSizeHistogramVec.WithLabelValues(s.ctx.StatLabel, packets.PacketNames[ptype]).Observe(float64(ll))

	if ptype == packets.Disconnect {
		// wait startHandle finished
		return s.handlePacket(pkg)
	}

	// pkg maybe put back in the packet of pool
	return s.PutReadBuffer(pkg)
}

// Write an MQTT packet to the client
// biz must ensure the correct order of writechan
func (s *Session) Write() error {

	// Copy the buffer of write packages
	wpkg := s.ctx.GetAndClearWriteBuffer()
	// write mqtt packets
	for _, pkg := range wpkg {
		if err := s.conn.writePacket(pkg); err != nil {
			log.Error("write packet failed", s.ctx.LogFields(
				zap.String("type", packets.PacketNames[pkg.Type()]),
				zap.Stringer("pkg", pkg),
				zap.Error(err))...)
			pkg.Close()
			return err
		}
		pkg.Close()
	}
	return nil
}

// IsWrite When the write buffer is not empty return true otherwise return false
func (s *Session) IsWrite(noWriteHandler func(), writeHandler func()) bool {
	return s.ctx.IsWrite(noWriteHandler, writeHandler)
}

// PutReadBuffer put pkg the packet of buffer
// if there is no the handle of function . create one
func (s *Session) PutReadBuffer(pkg packets.ControlPacket) error {
	for {
		s.readPacketMutex.Lock()
		// TODO 128 configuration management
		if len(s.readBufferPackets) >= 128 {
			s.readPacketMutex.Unlock()
			time.Sleep(time.Second)
			continue
		}

		s.readBufferPackets = append(s.readBufferPackets, pkg)
		// if there's no handler. create one
		if s.CompareAndSwapProcessState(context.Idle, context.Busy) {
			pkgs := make([]packets.ControlPacket, len(s.readBufferPackets))
			copy(pkgs, s.readBufferPackets)
			s.readBufferPackets = s.readBufferPackets[:0]
			go s.startHandle(pkgs)
		}
		s.readPacketMutex.Unlock()
		return nil
	}
}

func (s *Session) startHandle(pkgs []packets.ControlPacket) {
	if s.CompareState(context.ClosedState) {
		s.StoreProcessState(context.Idle)
		return
	}

	for {
		for _, pkg := range pkgs {
			if err := s.handlePacket(pkg); err != nil {
				log.Error("handle packet failed", s.ctx.LogFields(
					zap.String("type", packets.PacketNames[pkg.Type()]),
					zap.Stringer("pkg", pkg),
					zap.Error(err))...)
				pkg.Close()
				s.StoreProcessState(context.Idle)
				s.Close()
				return
			}
			pkg.Close()
		}

		// get the unprocessed package from the read buffer
		s.readPacketMutex.Lock()
		if len(s.readBufferPackets) == 0 {
			s.StoreProcessState(context.Idle)
			s.readPacketMutex.Unlock()
			return
		}
		pkgs = make([]packets.ControlPacket, len(s.readBufferPackets))
		copy(pkgs, s.readBufferPackets)
		s.readBufferPackets = s.readBufferPackets[:0]
		s.readPacketMutex.Unlock()
	}
}

func (s *Session) State() context.SessionState {
	return s.ctx.LoadState()
}

func (s *Session) CompareState(expect context.SessionState) bool {
	return s.ctx.CompareState(expect)
}

func (s *Session) CompareWriteState(expect context.WorkState) bool {
	if !s.CompareState(context.RunningState) {
		return false
	}
	return s.ctx.CompareWriteState(expect)
}

func (s *Session) StoreWriteState(state context.WorkState) {
	s.ctx.StoreWriteState(state)
}

func (s *Session) CompareAndSwapWriteState(olds, news context.WorkState) bool {
	if !s.CompareState(context.RunningState) {
		return false
	}
	return s.ctx.CompareAndSwapWriteState(olds, news)
}

func (s *Session) CompareReadState(expect context.WorkState) bool {
	if !s.CompareState(context.RunningState) {
		return false
	}
	if (context.WorkState)(atomic.LoadInt32((*int32)(&s.reading))) != expect {
		return false
	}
	return true
}

func (s *Session) StoreReadState(state context.WorkState) {
	atomic.StoreInt32((*int32)(&s.reading), (int32)(state))
}

func (s *Session) CompareAndSwapReadState(olds, news context.WorkState) bool {
	if !s.CompareState(context.RunningState) {
		return false
	}
	return atomic.CompareAndSwapInt32((*int32)(&s.reading), int32(olds), int32(news))
}

func (s *Session) CompareProcessState(expect context.WorkState) bool {
	if !s.CompareState(context.RunningState) {
		return false
	}
	if (context.WorkState)(atomic.LoadInt32((*int32)(&s.processing))) != expect {
		return false
	}
	return true
}

func (s *Session) StoreProcessState(state context.WorkState) {
	atomic.StoreInt32((*int32)(&s.processing), (int32)(state))
}

func (s *Session) CompareAndSwapProcessState(olds, news context.WorkState) bool {
	if !s.CompareState(context.RunningState) {
		return false
	}
	return atomic.CompareAndSwapInt32((*int32)(&s.processing), int32(olds), int32(news))
}
