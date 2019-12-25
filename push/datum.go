package push

import (
	"errors"
	"fmt"
	"time"

	"github.com/meitu/bifrost/commons/log"
	"github.com/meitu/bifrost/commons/trace"
	"github.com/meitu/bifrost/push/conf"
	"github.com/meitu/bifrost/push/conncli"
	"github.com/meitu/bifrost/push/store"
	"go.uber.org/zap"
	"golang.org/x/net/context"

	pb "github.com/meitu/bifrost/grpc/push"
	pbMessage "github.com/meitu/bifrost/push/message"
)

var (
	ErrNoSubscribers = errors.New("topis has not been subscribed")
)

type Datum struct {
	store.Storage
	connd  conncli.Connd
	config *conf.Store
}

func NewDatum(config *conf.Store, connd conncli.Connd) (*Datum, error) {
	// store
	var (
		s   store.Storage
		err error
	)
	if config.Name == "redis" {
		s, err = store.NewRdsStore(&config.Redis)
		if err != nil {
			log.Error("store new failed", zap.Error(err))
			return nil, fmt.Errorf("store new failed err:%s", err)
		}
	} else {
		s, err = store.NewTiStore(&config.Tikv)
		if err != nil {
			log.Error("store new failed", zap.Error(err))
			return nil, fmt.Errorf("store new failed err:%s", err)
		}
	}

	return &Datum{
		connd:   connd,
		config:  config,
		Storage: s,
	}, nil
}

func (s *Datum) connect(ctx context.Context, req *pb.ConnectReq) (int64, []*pb.Record, bool, error) {
	trace.WithTraceID(ctx, req.TraceID)
	start := time.Now()
	err := s.KickIfNeeded(ctx, req.Service, req.ClientID, req.GrpcAddress, req.ConnectionID, s.connd.Disconnect)
	cost := time.Since(start)
	if err != nil {
		log.Error("kick connection failed", errCostField(req.ClientID, req.TraceID, req.Service, cost, err)...)
		return 0, nil, false, err
	}
	tikvSlow("kick connection slow", req.ClientID, req.TraceID, cost, s.config.Slowlog)

	if !s.config.SessionRecovery {
		if !req.CleanSession {
			log.Warn("connect invalid request", zap.String("clientid", req.ClientID), zap.String("traceid", req.TraceID))
			return 0, nil, false, errors.New("invalid request")
		}
		//return early to avoid accessing redis(Drop and Discard)
		return 0, nil, false, nil
	}

	if req.CleanSession {
		// drop clientid session messages,set SessionPresent false
		if env := log.Check(zap.DebugLevel, "try drop session"); env != nil {
			env.Write(zap.String("clientid", req.ClientID), zap.String("traceid", req.TraceID))
		}

		start = time.Now()
		err = s.DropSession(ctx, req.Service, req.ClientID)
		cost = time.Since(start)
		if err != nil {
			log.Error("commit drop session failed", errCostField(req.ClientID, req.TraceID, req.Service, cost, err)...)
			return 0, nil, false, err
		}
		tikvSlow("drop session slow", req.ClientID, req.TraceID, cost, s.config.Slowlog)
		return 0, nil, false, nil
	}

	if env := log.Check(zap.DebugLevel, "try resume seession"); env != nil {
		env.Write(zap.String("clientid", req.ClientID), zap.String("traceid", req.TraceID))
	}

	start = time.Now()
	mid, rs, err := s.ResumeSession(ctx, req.Service, req.ClientID)
	cost = time.Since(start)
	if err != nil {
		log.Error("commit session failed", errCostField(req.ClientID, req.TraceID, req.Service, cost, err)...)
		return 0, nil, false, err
	}
	tikvSlow("resume slow", req.ClientID, req.TraceID, cost, s.config.Slowlog)

	records := make([]*pb.Record, len(rs))
	for i, r := range rs {
		records[i] = &pb.Record{
			Topic:        r.Topic,
			CurrentIndex: r.CurrentIndex,
			LastestIndex: r.LastestIndex,
			Qos:          int32(r.Qos),
		}
	}
	return mid, records, true, nil
}

func (s *Datum) rangeUnack(ctx context.Context, req *pb.RangeUnackReq) ([]byte, []*pb.UnackDesc, bool, error) {
	var err error
	start := time.Now()
	offset, msgs, complete, err := s.RangeUnackMessages(ctx, req.Service, req.ClientID, req.Offset, req.Limit)
	cost := time.Since(start)
	if err != nil {
		log.Error("query rangeunack failed", errCostField(req.ClientID, req.TraceID, req.Service, cost, err)...)
		return nil, nil, false, err
	}
	tikvSlow("rangeunack slow", req.ClientID, req.TraceID, cost, s.config.Slowlog)
	return offset, storeUnacksChangeGrpcUnacks(msgs), complete, nil
}

func (s *Datum) putUnack(ctx context.Context, req *pb.PutUnackReq) error {
	start := time.Now()
	err := s.PutUnackMessage(ctx, req.Service, req.ClientID, req.CleanSession, grpcUnacksChangeStoreUnacks(req.Messages))
	cost := time.Since(start)
	if err != nil {
		log.Error("putunack failed", errCostField(req.ClientID, req.TraceID, req.Service, cost, err)...)
		return err
	}
	tikvSlow("putunack slow", req.ClientID, req.TraceID, cost, s.config.Slowlog)
	return nil
}

func (s *Datum) disconnect(ctx context.Context, req *pb.DisconnectReq) error {
	start := time.Now()
	err := s.Offline(ctx, req.Service, req.ClientID, req.GrpcAddress, req.ConnectionID, req.CleanSession, req.Kick)
	cost := time.Since(start)
	if err != nil {
		log.Error("offline failed", errCostField(req.ClientID, req.TraceID, req.Service, cost, err)...)
		return err
	}
	tikvSlow("offline slow", req.ClientID, req.TraceID, cost, s.config.Slowlog)
	return nil
}

func (s *Datum) subscribe(ctx context.Context, req *pb.SubscribeReq) ([][]byte, []*pb.Message, error) {
	qoss := make([]byte, 0, len(req.Topics))
	topics := make([]string, 0, len(req.Topics))
	for i, topic := range req.Topics {
		if req.Qoss[i] == 0x80 {
			continue
		}
		qoss = append(qoss, byte(req.Qoss[i]))
		topics = append(topics, topic)
	}

	start := time.Now()
	index, retainMsgs, err := s.Subscribe(ctx, req.Service, req.ClientID, topics, qoss, req.CleanSession)
	cost := time.Since(start)
	if err != nil {
		log.Error("subscribe failed", errCostField(req.ClientID, req.TraceID, req.Service, cost, err)...)
		return nil, nil, err
	}
	tikvSlow("subscribe slow", req.ClientID, req.TraceID, cost, s.config.Slowlog)

	return index, storeMsgsChangeGrpcMsgs(retainMsgs, true), nil
}

func (s *Datum) unsubscribe(ctx context.Context, req *pb.UnsubscribeReq) error {
	start := time.Now()
	err := s.Unsubscribe(ctx, req.Service, req.ClientID, req.Topics, req.CleanSession)
	cost := time.Since(start)
	if err != nil {
		log.Error("unsubscribe failed", errCostField(req.ClientID, req.TraceID, req.Service, cost, err)...)
		return err
	}
	tikvSlow("unsubscribe slow", req.ClientID, req.TraceID, cost, s.config.Slowlog)
	return nil
}

func (s *Datum) mqttpublish(ctx context.Context, req *pb.PublishReq) ([]string, error) {
	msg := &pbMessage.Message{
		Topic:   req.Message.Topic,
		Payload: req.Message.Payload,
		Qos:     req.Message.Qos,
		TraceID: req.Message.TraceID,
		// Index:     req.Message.Index,
		BizID:      req.Message.BizID,
		MessageID:  req.Message.MessageID,
		CreateTime: time.Now().UnixNano(),
	}
	var err error
	start := time.Now()
	connds, subers, index, err := s.Publish(ctx, req.Service, msg, false, s.config.SessionRecovery)
	cost := time.Since(start)
	if err != nil {
		log.Error("publish failed", errCostField(req.ClientID, req.Message.TraceID, req.Service, cost, err)...)
		return nil, err
	}
	tikvSlow("publish slow", msg.Topic, msg.TraceID, cost, s.config.Slowlog)

	if len(connds) == 0 {
		return subers, nil
	}

	if err := s.connd.Notify(ctx, req.Service, connds, msg.Topic, index, false); err != nil {
		log.Error("publishByRoute notify failed",
			zap.String("topic", msg.Topic),
			zap.String("traceid", msg.TraceID),
			zap.Error(err))
		return subers, err
	}
	return subers, nil
}

func (s *Datum) delack(ctx context.Context, req *pb.DelUnackReq) ([]byte, error) {
	var bizID []byte
	var err error
	start := time.Now()
	bizID, err = s.DelUnackMessage(ctx, req.Service, req.ClientID, req.MessageID)
	cost := time.Since(start)
	if err != nil {
		log.Error("delack failed", errCostField(req.ClientID, req.TraceID, req.Service, cost, err)...)
		return nil, err
	}
	tikvSlow("delunack slow", req.ClientID, req.TraceID, cost, s.config.Slowlog)
	return bizID, nil
}

func (s *Datum) pull(ctx context.Context, req *pb.PullReq) ([]byte, []*pb.Message, bool, error) {
	start := time.Now()
	var err error
	off, messages, complete, err := s.Pull(ctx, req.Service, req.TraceID, req.Topic, req.Offset, req.Limit)
	cost := time.Since(start)
	if err != nil {
		log.Error("pull failed", errCostField(req.Topic, req.TraceID, req.Service, cost, err)...)
		return nil, nil, false, err
	}
	tikvSlow("callback pull slow", req.Topic, req.TraceID, cost, s.config.Slowlog)
	return off, storeMsgsChangeGrpcMsgs(messages, false), complete, nil
}

func (s *Datum) addroute(ctx context.Context, req *pb.AddRouteReq) error {
	start := time.Now()
	err := s.AddRoute(ctx, req.Service, req.Topic, req.GrpcAddress, req.Version)
	cost := time.Since(start)
	if err != nil {
		log.Error("addroute failed", errCostField(req.Topic, req.TraceID, req.Service, cost, err)...)
		return err
	}
	tikvSlow("addroute slow", req.Topic, req.TraceID, cost, s.config.Slowlog)
	return nil
}

func (s *Datum) removeroute(ctx context.Context, req *pb.RemoveRouteReq) error {
	start := time.Now()
	err := s.RemoveRoute(ctx, req.Service, req.Topic, req.GrpcAddress, req.Version)
	cost := time.Since(start)
	if err != nil {
		log.Error("remove failed", errCostField(req.Topic, req.TraceID, req.Service, cost, err)...)
		return err
	}
	tikvSlow("removeroute slow", req.GrpcAddress, req.TraceID, cost, s.config.Slowlog)
	return nil
}

func (s *Datum) publish(ctx context.Context, service string, msg *pbMessage.Message, retain bool, noneDowngrade bool) ([]string, []byte, error) {
	var err error
	start := time.Now()
	connds, subers, index, err := s.Publish(ctx, service, msg, retain, s.config.SessionRecovery)
	cost := time.Since(start)
	if err != nil {
		log.Error("biz publish failed", errCostField(msg.Topic, msg.TraceID, service, cost, err)...)
		return nil, nil, err
	}
	tikvSlow("publish slow", msg.Topic, msg.TraceID, cost, s.config.Slowlog)

	if len(connds) == 0 && len(subers) == 0 {
		return nil, nil, ErrNoSubscribers
	}

	if len(connds) != 0 {
		if err := s.connd.Notify(ctx, service, connds, msg.Topic, index, noneDowngrade); err != nil {
			log.Error("publishByRoute notify failed",
				zap.String("topic", msg.Topic),
				zap.String("traceid", msg.TraceID),
				zap.Error(err))
			return subers, nil, err
		}
	}

	return subers, index, nil
}
