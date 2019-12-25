package session

import (
	"errors"
	"time"

	"github.com/arthurkiller/mqtt-go/packets"
	"github.com/meitu/bifrost/commons/auth"
	"github.com/meitu/bifrost/commons/log"
	"github.com/meitu/bifrost/conn/context"
	ph "github.com/meitu/bifrost/grpc/push"
	"github.com/twinj/uuid"
	"go.uber.org/zap"
)

// NotifyHandler register recv pushcli notice
type NotifyHandler func(topic string, last []byte)

var (
	// ErrTopicsCount the count of topic is zero
	ErrTopicsCount = errors.New("the count of topic is zero")
	// ErrTopicsQoSsCount the count of topic and qoss
	ErrTopicsQoSsCount = errors.New("the count of topics and the count of qoss is not equal")
	// ErrQoSInvalid qos is invalid
	ErrQoSInvalid = errors.New("qos is invalid")
	// ErrPacketInvalid packet is invalid"
	ErrPacketInvalid = errors.New("packet is invalid")
)

func toBytes(src []int32) []byte {
	dst := make([]byte, len(src))
	for v := range src {
		dst[v] = byte(src[v])
	}
	return dst
}

// processor the business logic handles the interaction with push
type processor struct {
	ctx             *context.SessionContext
	registerHandler NotifyHandler
	ack             *ack

	cookie      []byte
	willFlag    bool
	willMessage *ph.Message
}

// NewProcessor create a new object
func NewProcessor(ctx *context.SessionContext, notifyHandler NotifyHandler) *processor {
	return &processor{
		ctx:             ctx,
		registerHandler: notifyHandler,
		ack:             NewACK(),
	}
}

// Distribution processing of MQTT packages
// Only once the connect and disconnect packet is allowed to be sent and handled separately by the upper layer
func (s *processor) handlePacket(pkg packets.ControlPacket) error {
	var err error
	switch pkg.Type() {
	case packets.Subscribe:
		err = s.handleSubscribe(pkg.(*packets.SubscribePacket))
	case packets.Unsubscribe:
		err = s.handleUnsubscribe(pkg.(*packets.UnsubscribePacket))
	case packets.Publish:
		err = s.handlePublish(pkg.(*packets.PublishPacket))
	case packets.Puback:
		err = s.handlePuback(pkg.(*packets.PubackPacket))
	case packets.Pubrec:
		log.Info("receive pubrec packet") //TODO
	case packets.Pubrel:
		log.Info("receive pubrel packet") //TODO
	case packets.Pubcomp:
		log.Info("receive pubcomp packet") //TODO
	case packets.Pingreq:
		s.ctx.WritePacket(packets.NewPingrespPacket(), nil)
	case packets.Disconnect:
		fallthrough
	case packets.Connect:
		fallthrough
	case packets.Unsuback:
		fallthrough
	case packets.Pingresp:
		fallthrough
	case packets.Suback:
		fallthrough
	case packets.Connack:
		fallthrough
	default:
		err = ErrPacketInvalid
	}
	return err
}

func (s *processor) handleConnect(pkg *packets.ConnectPacket) (*packets.ConnackPacket, bool, error) {
	var err error
	pulling := false
	ack := packets.NewConnackPacket()
	code := pkg.Validate()

	switch code {
	case packets.ErrRefusedBadProtocolVersion:
		fallthrough
	case packets.ErrProtocolViolation:
		ack.ReturnCode = 0x1
	case packets.ErrRefusedIDRejected:
		ack.ReturnCode = 0x2
	case packets.ErrRefusedServerUnavailable:
		ack.ReturnCode = 0x3
	case packets.ErrRefusedBadUsernameOrPassword:
		ack.ReturnCode = 0x4
	case packets.ErrRefusedNotAuthorised:
		ack.ReturnCode = 0x5
	}
	if code != packets.Accepted {
		return ack, pulling, packets.ConnErrors[code]
	}

	s.ctx.ClientID = pkg.ClientIdentifier
	if len(pkg.ClientIdentifier) == 0 {
		s.ctx.ClientID = uuid.NewV4().String()
		log.Info("generate clientid",
			s.ctx.LogFields()...)
	}

	if pkg.Keepalive > 0 && time.Duration(pkg.Keepalive*3/2)*time.Second < s.ctx.Conf.MaxKeepalive {
		s.ctx.Keepalive = time.Duration(pkg.Keepalive*3/2) * time.Second
	}

	if err := s.parseService(pkg.Username); err != nil {
		ack.ReturnCode = 0x5
		log.Error("parse service failed", s.ctx.LogFields(zap.Error(err))...)
		return ack, pulling, err
	}

	traceID := uuid.NewV4().String()

	//handle connect
	resp, err := s.ctx.PushCli.Connect(s.ctx, &ph.ConnectReq{
		Username:      pkg.Username,
		Password:      pkg.Password,
		CleanSession:  pkg.CleanSession,
		ClientID:      s.ctx.ClientID,
		GrpcAddress:   s.ctx.GrpcAddr,
		TraceID:       traceID,
		ClientAddress: s.ctx.Conn.RemoteAddr().String(),
		ConnectionID:  int64(s.ctx.Conn.FD()),
		Service:       s.ctx.Service,
	})
	if err != nil {
		ack.ReturnCode = 0x3
		log.Error("grpc call connect failed", s.ctx.LogFields(zap.Error(err))...)
		return ack, pulling, err
	}

	s.cookie = resp.Cookie
	s.ctx.StatLabel = resp.StatLabel
	s.ctx.CleanSession = !resp.SessionPresent
	s.ctx.Service = resp.Service
	ack.SetTraceID(traceID)
	s.ctx.SetPacketID(resp.MessageID)

	//check connreturn code
	if ack.ReturnCode = byte(resp.MqttCode); ack.ReturnCode != 0x0 {
		log.Info("biz refuses to connect", s.ctx.LogFields()...)
		return ack, pulling, packets.ConnErrors[ack.ReturnCode]
	}

	if ack.SessionPresent = resp.SessionPresent; ack.SessionPresent {
		if pulling, err = s.recoverySession(resp.Records, traceID); err != nil {
			ack.ReturnCode = 0x3
			return ack, pulling, err
		}
	}

	if pkg.WillFlag {
		s.willMessages(pkg, traceID)
	}

	if env := log.Check(zap.InfoLevel, "recv connect package"); env != nil {
		env.Write(s.ctx.LogFields(
			zap.Bool("will", s.willFlag),
			zap.String("traceid", traceID),
		)...)
	}
	return ack, pulling, nil
}

func (s *processor) handleDisconnect(p *packets.DisconnectPacket) error {
	traceID := uuid.NewV4().String()
	req := &ph.DisconnectReq{
		ClientID:     s.ctx.ClientID,
		ConnectionID: int64(s.ctx.Conn.FD()),
		Cookie:       s.cookie,
		CleanSession: s.ctx.CleanSession,
		TraceID:      traceID,
		GrpcAddress:  s.ctx.GrpcAddr,
		Service:      s.ctx.Service,
		StatLabel:    s.ctx.StatLabel,
		Lost:         false,
	}

	if _, err := s.ctx.PushCli.Disconnect(s.ctx, req); err != nil {
		return err
	}

	if env := log.Check(zap.InfoLevel, "recv disconnect package"); env != nil {
		env.Write(s.ctx.LogFields(zap.String("traceid", traceID))...)
	}
	return nil
}

func (s *processor) handleSubscribe(p *packets.SubscribePacket) error {
	if len(p.Topics) == 0 {
		log.Error("recv subscribe failed",
			s.ctx.LogFields(
				zap.Strings("topics", p.Topics),
				zap.Error(ErrTopicsCount),
			)...)
		return ErrTopicsCount
	}

	if len(p.Topics) != len(p.QoSs) {
		log.Error("recv subscribe failed",
			s.ctx.LogFields(
				zap.Strings("topics", p.Topics),
				zap.Error(ErrTopicsQoSsCount),
			)...)
		return ErrTopicsQoSsCount
	}

	qoss := make([]int32, len(p.QoSs))
	for v := range p.QoSs {
		qoss[v] = int32(p.QoSs[v])
		if qoss[v] > 2 || qoss[v] < 0 {
			return ErrQoSInvalid
		}
	}

	traceID := uuid.NewV4().String()

	if err := s.addroute(p.Topics, traceID); err != nil {
		return err
	}

	resp, err := s.ctx.PushCli.Subscribe(s.ctx, &ph.SubscribeReq{
		ClientID:     s.ctx.ClientID,
		Cookie:       s.cookie,
		CleanSession: s.ctx.CleanSession,
		TraceID:      traceID,
		Topics:       p.Topics,
		Qoss:         qoss,
		Service:      s.ctx.Service,
		StatLabel:    s.ctx.StatLabel,
	})

	if err != nil {
		if err := s.removeroute(p.Topics, traceID); err != nil {
			log.Error("remove route failed", s.ctx.LogFields(zap.Error(err))...)
		}
		return err
	}

	// write sub ack
	sack := packets.NewSubackPacket()
	sack.SetTraceID(traceID)
	sack.MessageID = p.MessageID
	sack.ReturnCodes = toBytes(resp.Qoss)
	handler := func() bool {
		return s.ctx.CompareWriteState(context.Busy)
	}
	// update subscription
	s.cookie = resp.Cookie
	s.ctx.SubUpdateInfo(p.Topics, resp.Index, resp.Qoss)
	s.ctx.WritePacket(sack, handler)

	// write retain
	if len(resp.RetainMessage) > 0 {
		pkgs := make([]*packets.PublishPacket, 0, len(resp.RetainMessage))
		for _, v := range resp.RetainMessage {
			// TODO bug
			pub := packets.NewPublishPacket()
			pub.SetTraceID(traceID)
			pub.TopicName = v.Topic
			pub.QoS = byte(v.Qos)
			pub.MessageID = s.ctx.GetPacketID()
			pub.Payload = v.Payload
			pkgs = append(pkgs, pub)
		}
		s.ctx.WritePublishPackets(pkgs, handler)
	}

	// call post subscribe
	postresp, err := s.ctx.PushCli.PostSubscribe(s.ctx, &ph.PostSubscribeReq{
		ClientID:  s.ctx.ClientID,
		Cookie:    s.cookie,
		StatLabel: s.ctx.StatLabel,
		Service:   s.ctx.Service,
		TraceID:   traceID,
		Topics:    p.Topics,
		Qoss:      resp.Qoss,
	})
	if err != nil {
		return err
	}

	s.cookie = postresp.Cookie

	if env := log.Check(zap.InfoLevel, "recv subscribe package"); env != nil {
		env.Write(s.ctx.LogFields(
			zap.String("traceid", traceID),
			zap.Strings("topics", p.Topics),
			zap.String("qos", string(p.QoSs)),
		)...)
	}
	return nil
}

func (s *processor) handleUnsubscribe(p *packets.UnsubscribePacket) error {
	if len(p.Topics) == 0 {
		log.Info("recv unsubscribe failed", s.ctx.LogFields(zap.Error(ErrTopicsCount))...)
		return ErrTopicsCount
	}

	traceID := uuid.NewV4().String()

	resp, err := s.ctx.PushCli.Unsubscribe(s.ctx, &ph.UnsubscribeReq{
		ClientID:     s.ctx.ClientID,
		Cookie:       s.cookie,
		CleanSession: s.ctx.CleanSession,
		Service:      s.ctx.Service,
		StatLabel:    s.ctx.StatLabel,
		TraceID:      traceID,
		Topics:       p.Topics,
		Lost:         false,
	})
	if err != nil {
		return err
	}

	if err := s.removeroute(p.Topics, traceID); err != nil {
		log.Error("remove route failed", s.ctx.LogFields(zap.Error(err))...)
		return err
	}
	s.ctx.UnsubUpdateInfo(p.Topics)

	s.cookie = resp.Cookie
	usack := packets.NewUnsubackPacket()
	usack.SetTraceID(traceID)
	usack.MessageID = p.MessageID
	handler := func() bool {
		return s.ctx.CompareWriteState(context.Busy)
	}
	s.ctx.WritePacket(usack, handler)

	if env := log.Check(zap.InfoLevel, "recv unsubscribe package"); env != nil {
		env.Write(s.ctx.LogFields(
			zap.String("traceid", traceID),
			zap.Strings("topics", p.Topics),
		)...)
	}
	return nil
}

func (s *processor) handlePublish(p *packets.PublishPacket) error {
	traceID := uuid.NewV4().String()
	resp, err := s.ctx.PushCli.Publish(s.ctx, &ph.PublishReq{
		ClientID:  s.ctx.ClientID,
		Cookie:    s.cookie,
		Service:   s.ctx.Service,
		StatLabel: s.ctx.StatLabel,
		Message: &ph.Message{
			Topic:     p.TopicName,
			MessageID: int64(p.MessageID),
			Retain:    p.Retain,
			Qos:       int32(p.QoS),
			Payload:   p.Payload,
			TraceID:   traceID,
			//BizID:   s.BizID, //FIXME
		},
	})
	if err != nil {
		return err //TODO FIXME do noting on error
	}

	s.cookie = resp.Cookie
	if int(p.QoS) == 1 { //Only support qos 1 yet
		puback := packets.NewPubackPacket()
		puback.MessageID = p.MessageID
		puback.SetTraceID(traceID)
		handler := func() bool {
			return s.ctx.CompareWriteState(context.Busy)
		}
		s.ctx.WritePacket(puback, handler)
	}

	if env := log.Check(zap.InfoLevel, "recv publish package"); env != nil {
		env.Write(s.ctx.LogFields(
			zap.String("topic", p.TopicName),
			zap.String("traceid", traceID),
			zap.Int("qos", int(p.QoS)),
			zap.Int("messageid", int(p.MessageID)),
			zap.Bool("retain", p.Retain),
		)...)
	}
	return nil
}

func (s *processor) handlePuback(p *packets.PubackPacket) error {
	traceID := uuid.NewV4().String()
	resp, err := s.ack.DelUnack(s.ctx, &ph.DelUnackReq{
		ClientID:     s.ctx.ClientID,
		Cookie:       s.cookie,
		Service:      s.ctx.Service,
		CleanSession: s.ctx.CleanSession,
		StatLabel:    s.ctx.StatLabel,
		MessageID:    int64(p.MessageID),
		TraceID:      traceID,
	})
	if err != nil {
		return err
	}
	s.cookie = resp.Cookie
	if env := log.Check(zap.InfoLevel, "recv handle puback"); env != nil {
		env.Write(s.ctx.LogFields(
			zap.String("bizid", string(resp.BizID)),
			zap.String("traceid", traceID),
			zap.Int("qos", int(p.QoS)),
			zap.Int("messageid", int(p.MessageID)),
		)...)
	}
	return nil
}

func (s *processor) parseService(username string) error {
	if username == "" || len(s.ctx.Conf.Auth) == 0 {
		return nil
	}
	token, err := auth.ParseAppKey(username)
	if err != nil || len(token) == 0 {
		log.Error("parse appkey failed", s.ctx.LogFields(zap.Error(err))...)
		return nil
	}
	service, err := auth.Verify([]byte(token), []byte(s.ctx.Conf.Auth))
	if err != nil {
		return err
	}
	s.ctx.Service = string(service)
	return nil
}

func (s *processor) pubWillMessage(msg *ph.Message) {
	_, err := s.ctx.PushCli.Publish(context.Background(), &ph.PublishReq{
		ClientID:  s.ctx.ClientID,
		Cookie:    s.cookie,
		Message:   msg,
		Service:   s.ctx.Service,
		StatLabel: s.ctx.StatLabel,
	})
	if err != nil {
		log.Error("publish will message failed",
			s.ctx.LogFields(
				zap.String("traceid", msg.TraceID),
				zap.String("topic", msg.Topic),
				zap.Error(err),
			)...)
	}
}

func (s *processor) handleDirty(state context.SessionState) {
	topics := s.ctx.CloneWriteInfo()
	traceID := uuid.NewV4().String()
	// unsub
	if len(topics) != 0 {
		_, err := s.ctx.PushCli.Unsubscribe(context.Background(), &ph.UnsubscribeReq{
			ClientID:     s.ctx.ClientID,
			Cookie:       s.cookie,
			CleanSession: s.ctx.CleanSession,
			Service:      s.ctx.Service,
			StatLabel:    s.ctx.StatLabel,
			TraceID:      traceID,
			Topics:       topics,
			Lost:         true,
		})
		if err != nil {
			log.Error("clean unsubscribe failed",
				s.ctx.LogFields(
					zap.String("traceid", traceID),
					zap.Strings("topics", topics),
					zap.Error(err),
				)...)
		}
		// delete route
		err = s.removeroute(topics, traceID)
		if err != nil {
			log.Error("remove route failed", s.ctx.LogFields(zap.Error(err))...)
		}
		// delete info
		s.ctx.UnsubUpdateInfo(topics)
	}

	// publish will
	if s.willFlag {
		s.pubWillMessage(s.willMessage)
	}

	if state == context.RunningState {
		req := &ph.DisconnectReq{
			ClientID:     s.ctx.ClientID,
			Cookie:       s.cookie,
			CleanSession: s.ctx.CleanSession,
			TraceID:      traceID,
			GrpcAddress:  s.ctx.GrpcAddr,
			Service:      s.ctx.Service,
			StatLabel:    s.ctx.StatLabel,
			ConnectionID: int64(s.ctx.Conn.FD()),
			Kick:         s.ctx.Kick,
			Lost:         true,
		}
		if _, err := s.ctx.PushCli.Disconnect(context.Background(), req); err != nil {
			log.Error("clean disconnect failed",
				s.ctx.LogFields(
					zap.String("traceid", traceID),
					zap.Strings("topics", topics),
					zap.Error(err),
				)...)
		}
	}
}

func (p *processor) handleRangeUnack(traceID string) error {
	// resume session with message Pull UnackMessage
	offset := []byte{}
	var complete bool

	for !complete {
		resp, err := p.ctx.PushCli.RangeUnack(p.ctx, &ph.RangeUnackReq{
			ClientID: p.ctx.ClientID,
			TraceID:  traceID,
			Limit:    p.ctx.Conf.RangeUnackLimit,
			Offset:   offset,
			Service:  p.ctx.Service,
		})

		if err != nil {
			log.Error("unack failed", p.ctx.LogFields(
				zap.String("traceid", traceID),
				zap.Error(err),
			)...)
			return err
		}
		//update offset
		offset = resp.Offset
		complete = resp.Complete
		// send all unack message
		pkgs := make([]*packets.PublishPacket, 0, len(resp.Messages))
		for _, v := range resp.Messages {
			pkg := packets.NewPublishPacket()
			pkg.SetTraceID(v.TraceID)
			pkg.TopicName = v.Topic
			pkg.MessageID = uint16(v.MessageID)
			pkg.Payload = v.Payload
			pkg.Dup = true
			pkg.QoS = 1
			pkgs = append(pkgs, pkg)
		}
		handler := func() bool {
			return p.ctx.CompareWriteState(context.Busy)
		}
		p.ctx.WritePublishPackets(pkgs, handler)
	}
	return nil
}

func (s *processor) recoverySession(records []*ph.Record, traceID string) (bool, error) {
	// resume subscribe info
	var busy bool
	for _, r := range records {
		if err := s.addroute([]string{r.Topic}, traceID); err != nil {
			log.Error("addroute in recovery handler",
				s.ctx.LogFields(
					zap.String("traceid", traceID),
					zap.String("topic", r.Topic),
				)...)
			return false, err
		}
		if !busy {
			busy = s.ctx.RecoveryUpdateInfo(r)
		}
	}
	return busy, nil
}

func (s *processor) willMessages(pkg *packets.ConnectPacket, traceid string) {
	s.willFlag = pkg.WillFlag
	s.willMessage = &ph.Message{
		TraceID: traceid,
		Qos:     int32(pkg.WillQoS),
		Retain:  pkg.WillRetain,
		Topic:   pkg.WillTopic,
		Payload: make([]byte, len(pkg.WillMessage)),
	}
	copy(s.willMessage.Payload[:], pkg.WillMessage)
}

func (s *processor) removeroute(topics []string, traceID string) error {
	for _, topic := range topics {
		if !s.ctx.GetWriteInfo(topic) {
			continue
		}
		if err := s.ctx.Pubsub.RemoveRoute(&ph.RemoveRouteReq{
			TraceID:     traceID,
			Service:     s.ctx.Service,
			Topic:       topic,
			GrpcAddress: s.ctx.GrpcAddr,
			Version:     uint64(time.Now().UnixNano()),
		}, &s.registerHandler); err != nil {
			return err
		}
	}
	return nil
}

func (s *processor) addroute(topics []string, traceID string) error {
	for _, topic := range topics {
		if s.ctx.GetWriteInfo(topic) {
			continue
		}
		if err := s.ctx.Pubsub.AddRoute(&ph.AddRouteReq{
			TraceID:     traceID,
			Service:     s.ctx.Service,
			Topic:       topic,
			GrpcAddress: s.ctx.GrpcAddr,
			Version:     uint64(time.Now().UnixNano()),
		}, &s.registerHandler); err != nil {
			return err
		}
	}
	return nil
}
