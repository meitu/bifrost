package push

import (
	"errors"
	"time"

	"github.com/coreos/etcd/clientv3"
	lb "github.com/meitu/bifrost/commons/grpc-lb"
	"github.com/meitu/bifrost/commons/log"
	pb "github.com/meitu/bifrost/grpc/push"
	"github.com/meitu/bifrost/push/callback"
	"github.com/meitu/bifrost/push/conf"
	"github.com/meitu/bifrost/push/status"
	"go.uber.org/zap"
	"golang.org/x/net/context"
)

var (
	ErrTopicInvalid = errors.New("TopicInvalid")
)

type Push struct {
	call     callback.Callback
	store    *Datum
	config   *conf.Push
	register *lb.Node
}

func NewPush(config *conf.Push, call callback.Callback, store *Datum) *Push {
	ph := &Push{
		store:  store,
		call:   call,
		config: config,
	}
	return ph
}

func (ph *Push) Register(cc clientv3.Config, addr string) error {
	log.Info("register pushd service",
		zap.String("service", ph.config.Service),
		zap.String("group", ph.config.Group))

	var err error
	ph.register, err = lb.NewNode(cc, ph.config.Service, addr)
	if err != nil {
		return err
	}
	return ph.register.Register()
}

func (ph *Push) Deregister() error {
	log.Info("stop push servvice",
		zap.String("service", ph.config.Service),
		zap.String("group", ph.config.Group))

	return ph.register.Deregister()
}

// Connect Handle the client link request to notify the business client to login and handle the single concurrent connection problem.
func (ph *Push) Connect(ctx context.Context, req *pb.ConnectReq) (*pb.ConnectResp, error) {
	if env := log.Check(zap.DebugLevel, "recv connect"); env != nil {
		env.Write(zap.String("req", req.String()))
	}
	resp := &pb.ConnectResp{}
	// 客户端配置以 客户端为准
	req.Service = servname(req.Service, ph.config.Servname)

	start := time.Now()
	r, err := ph.call.OnConnect(ctx, req.Service, connReqChangeResp(req))
	cost := time.Since(start)
	if err != nil {
		log.Error("callback connect failed", errCostField(req.ClientID, req.TraceID, req.Service, cost, err)...)
		return nil, err
	}
	grpcSlow("callback connect slow", req.ClientID, req.TraceID, cost, ph.config.GrpcSlowlog)

	//TODO StatLabel and Service  have the same meaning
	resp.StatLabel = statlabel(req.Service, req.TraceID, r.StatLabel)
	resp.Service = req.Service
	// MQTT code
	resp.MqttCode = int32(r.ConnectCode)
	if r.ConnectCode != 0 {
		//The Client identifier is correct UTF-8 but not allowed by the Server
		resp.MqttCode = 4
	}
	resp.Cookie = r.Cookie
	status.GetMertics().CallbackHistogramVec.WithLabelValues(resp.StatLabel, "OnConnect").Observe(cost.Seconds())

	if r.ConnectCode != 0 {
		log.Info("callback connect failed",
			zap.String("clientid", req.ClientID),
			zap.String("traceid", req.TraceID),
			zap.String("service", req.Service),
			zap.String("connd-addr", req.GrpcAddress),
			zap.String("client-addr", req.ClientAddress),
			zap.Bool("cleansession", req.CleanSession),
			zap.Duration("cost", cost),
			zap.Int("code", int(r.ConnectCode)))
		return resp, nil
	}

	resp.MessageID, resp.Records, resp.SessionPresent, err = ph.store.connect(ctx, req)
	if err != nil {
		log.Error("callback connect failed",
			zap.String("clientid", req.ClientID),
			zap.String("traceid", req.TraceID),
			zap.String("service", req.Service),
			zap.String("connd-addr", req.GrpcAddress),
			zap.String("client-addr", req.ClientAddress),
			zap.Bool("cleansession", req.CleanSession),
			zap.Error(err))
		return nil, err
	}
	return resp, nil
}

// RangeUnack gets a message that does not reply to an ack
func (ph *Push) RangeUnack(ctx context.Context, req *pb.RangeUnackReq) (*pb.RangeUnackResp, error) {
	if env := log.Check(zap.DebugLevel, "recv rangeunack"); env != nil {
		env.Write(zap.String("req", req.String()))
	}
	resp := &pb.RangeUnackResp{}
	var err error

	resp.Offset, resp.Messages, resp.Complete, err = ph.store.rangeUnack(ctx, req)
	if err != nil {
		log.Error("store rangeunack failed",
			zap.String("clientid", req.ClientID),
			zap.String("traceid", req.TraceID),
			zap.String("service", req.Service),
			zap.Int64("limit", req.Limit),
			zap.Reflect("offset", req.Offset),
			zap.Error(err))
		return nil, err
	}
	return resp, nil
}

// PutUnack record unack messages
func (ph *Push) PutUnack(ctx context.Context, req *pb.PutUnackReq) (*pb.PutUnackResp, error) {
	if env := log.Check(zap.DebugLevel, "recv putunack"); env != nil {
		env.Write(zap.String("req", req.String()))
	}
	resp := &pb.PutUnackResp{}
	if req.CleanSession {
		return resp, nil
	}

	err := ph.store.putUnack(ctx, req)
	if err != nil {
		log.Error("store putunack failed",
			zap.String("clientid", req.ClientID),
			zap.String("traceid", req.TraceID),
			zap.String("service", req.Service),
			zap.Error(err))
		return nil, err
	}
	return resp, nil
}

// Disconnect connection disconnect
func (ph *Push) Disconnect(ctx context.Context, req *pb.DisconnectReq) (*pb.DisconnectResp, error) {
	if env := log.Check(zap.DebugLevel, "recv disconnect"); env != nil {
		env.Write(zap.String("req", req.String()))
	}
	resp := &pb.DisconnectResp{}
	start := time.Now()
	_, err := ph.call.OnDisconnect(ctx, req.Service, disReqChangeResp(req))
	cost := time.Since(start)
	if err != nil {
		log.Error("callback disconnect failed", errCostField(req.ClientID, req.TraceID, req.Service, cost, err)...)
		return resp, err
	}
	grpcSlow("callback disconnect slow", req.ClientID, req.TraceID, cost, ph.config.GrpcSlowlog)
	status.GetMertics().CallbackHistogramVec.WithLabelValues(req.StatLabel, "OnDisconnect").Observe(cost.Seconds())

	err = ph.store.disconnect(ctx, req)
	if err != nil {
		log.Error("store diconnect failed",
			zap.String("clientid", req.ClientID),
			zap.String("traceid", req.TraceID),
			zap.String("service", req.Service),
			zap.Error(err))
		return nil, err
	}
	return resp, nil
}

// Subscribe client subscribe topics and qos
func (ph *Push) Subscribe(ctx context.Context, req *pb.SubscribeReq) (*pb.SubscribeResp, error) {
	if env := log.Check(zap.DebugLevel, "recv subscribe"); env != nil {
		env.Write(zap.String("req", req.String()))
	}

	if len(req.Topics) != len(req.Qoss) {
		log.Error("invalid topics or qoss",
			zap.String("clientID", req.ClientID),
			zap.String("traceID", req.TraceID))
		return nil, ErrTopicInvalid
	}

	if len(req.Topics) == 0 {
		log.Error("no topic specified",
			zap.String("clientID", req.ClientID),
			zap.String("traceID", req.TraceID))
		return nil, ErrTopicInvalid
	}

	resp := &pb.SubscribeResp{}
	start := time.Now()
	r, err := ph.call.OnSubscribe(ctx, req.Service, subReqChangeResp(req))
	cost := time.Since(start)
	if err != nil {
		log.Error("callback subscribe failed", errCostField(req.ClientID, req.TraceID, req.Service, cost, err)...)
		return nil, err
	}
	grpcSlow("callback disconnect slow", req.ClientID, req.TraceID, cost, ph.config.GrpcSlowlog)
	status.GetMertics().CallbackHistogramVec.WithLabelValues(req.StatLabel, "OnSubsribce").Observe(cost.Seconds())

	resp.Qoss = make([]int32, len(req.Topics))
	resp.Cookie = r.Cookie
	for i, topic := range req.Topics {
		if r.Successes != nil && !r.Successes[i] {
			resp.Qoss[i] = 0x80
			req.Qoss[i] = 0x80
			log.Info("OnSubscribe reject the subscription",
				zap.String("clientid", req.ClientID),
				zap.String("traceid", req.TraceID),
				zap.String("topic", topic),
				zap.Int("qos", int(resp.Qoss[i])))
			continue
		}
		resp.Qoss[i] = req.Qoss[i]
		continue
	}

	resp.Index, resp.RetainMessage, err = ph.store.subscribe(ctx, req)
	if err != nil {
		log.Error("store subscribe failed",
			zap.String("clientid", req.ClientID),
			zap.String("traceid", req.TraceID),
			zap.String("service", req.Service),
			zap.Strings("topics", req.Topics),
			zap.Error(err))
		return nil, err
	}
	return resp, nil
}

// PostSubscribe client has subscribed
func (ph *Push) PostSubscribe(ctx context.Context, req *pb.PostSubscribeReq) (*pb.PostSubscribeResp, error) {
	if env := log.Check(zap.DebugLevel, "recv postSubscribe"); env != nil {
		env.Write(zap.String("req", req.String()))
	}

	resp := &pb.PostSubscribeResp{}
	start := time.Now()
	r, err := ph.call.PostSubscribe(ctx, req.Service, postReqChangeResp(req))
	cost := time.Since(start)
	if err != nil {
		log.Error("callback postsubscribe failed", errCostField(req.ClientID, req.TraceID, req.Service, cost, err)...)
		return resp, err
	}
	grpcSlow("callback unsubscribe slow", req.ClientID, req.TraceID, cost, ph.config.GrpcSlowlog)
	status.GetMertics().CallbackHistogramVec.WithLabelValues(req.StatLabel, "OnPostSubscribe").Observe(cost.Seconds())
	resp.Cookie = r.Cookie
	return resp, nil
}

// Unsubscribe  client unsubscribe topics
func (ph *Push) Unsubscribe(ctx context.Context, req *pb.UnsubscribeReq) (*pb.UnsubscribeResp, error) {
	if env := log.Check(zap.DebugLevel, "recv unsubscribe"); env != nil {
		env.Write(zap.String("req", req.String()))
	}

	if len(req.Topics) == 0 {
		log.Error("no topic supplied",
			zap.String("clientID", req.ClientID),
			zap.String("traceID", req.TraceID))
		return nil, ErrTopicInvalid
	}

	resp := &pb.UnsubscribeResp{}
	start := time.Now()
	r, err := ph.call.OnUnsubscribe(ctx, req.Service, unsubReqChangeResp(req))
	cost := time.Since(start)
	if err != nil {
		log.Error("callback unsubscribe failed", errCostField(req.ClientID, req.TraceID, req.Service, cost, err)...)
		return nil, err
	}
	grpcSlow("callback unsubscribe slow", req.ClientID, req.TraceID, cost, ph.config.GrpcSlowlog)
	status.GetMertics().CallbackHistogramVec.WithLabelValues(req.StatLabel, "OnUnsubscribe").Observe(cost.Seconds())

	resp.Cookie = r.Cookie

	if err := ph.store.unsubscribe(ctx, req); err != nil {
		log.Error("store unsubscribe failed",
			zap.String("clientid", req.ClientID),
			zap.String("traceid", req.TraceID),
			zap.String("service", req.Service),
			zap.Strings("topics", req.Topics),
			zap.Error(err))
		return nil, err
	}
	return resp, nil
}

// MQTTPublish client publishs a message to topic
func (ph *Push) MQTTPublish(ctx context.Context, req *pb.PublishReq) (*pb.PublishResp, error) {
	if env := log.Check(zap.DebugLevel, "recv mqttpublish"); env != nil {
		env.Write(zap.String("req", req.String()))
	}
	status.M.MessageCounterVec.WithLabelValues(req.StatLabel, "up").Inc()
	status.M.MessageSizeHistogramVec.WithLabelValues(req.StatLabel, "up").Observe(float64(len(req.Message.Payload)))

	if req.Message.Topic == "" {
		log.Error("publish topic name is empty",
			zap.String("clientID", req.ClientID),
			zap.String("traceID", req.Message.TraceID))
		return nil, ErrTopicInvalid
	}

	resp := &pb.PublishResp{}
	start := time.Now()
	r, err := ph.call.OnPublish(ctx, req.Service, pubReqChangeResp(req))
	cost := time.Since(start)
	if err != nil {
		log.Error("callback mqttpublish failed", errCostField(req.ClientID, req.Message.TraceID, req.Service, cost, err)...)
		return resp, err
	}
	grpcSlow("callback mqttpublish slow", req.Message.TraceID, req.ClientID, cost, ph.config.GrpcSlowlog)
	status.GetMertics().CallbackHistogramVec.WithLabelValues(req.StatLabel, "OnPublish").Observe(cost.Seconds())

	resp.Cookie = r.Cookie
	if r.Skip {
		log.Info("callback mqttpublish skip", errCostField(req.ClientID, req.Message.TraceID, req.Service, cost, err)...)
		return resp, nil
	}
	req.Message.BizID = r.BizID
	req.Message.Payload = r.Message

	start = time.Now()
	subers, err := ph.store.mqttpublish(ctx, req)
	if err != nil {
		log.Error("store mqttpublish failed",
			zap.String("clientid", req.ClientID),
			zap.String("traceid", req.Message.TraceID),
			zap.String("service", req.Service),
			zap.Error(err))
		status.M.MessageHistogramVec.WithLabelValues(req.StatLabel, "up", "failed").Observe(time.Now().Sub(start).Seconds())
		return nil, err
	}

	if len(subers) != 0 {
		status.M.MessageHistogramVec.WithLabelValues(req.StatLabel, "up", "nosubscriber").Observe(time.Now().Sub(start).Seconds())
		r, err := ph.call.OnOffline(ctx, req.Service, OnOfflineRequest(req.ClientID, req.Message.Topic, subers, req.Message.Payload, req.Cookie))
		if err != nil {
			log.Error("callback mqttpublish failed", errCostField(req.ClientID, req.Message.TraceID, req.Service, cost, err)...)
			return resp, err
		}
		resp.Cookie = r.Cookie
	}
	status.M.MessageHistogramVec.WithLabelValues(req.StatLabel, "up", "sucessed").Observe(time.Now().Sub(start).Seconds())

	return resp, nil
}

// Pubrec TODO qos 2
func (ph *Push) Pubrec(ctx context.Context, req *pb.PubrecReq) (*pb.PubrecResp, error) {
	if env := log.Check(zap.DebugLevel, "recv pubrec"); env != nil {
		env.Write(zap.String("req", req.String()))
	}
	resp := &pb.PubrecResp{}
	return resp, nil
}

// Pubrel TODO qos 2
func (ph *Push) Pubrel(ctx context.Context, req *pb.PubrelReq) (*pb.PubrelResp, error) {
	if env := log.Check(zap.DebugLevel, "recv pubrel"); env != nil {
		env.Write(zap.String("req", req.String()))
	}
	resp := &pb.PubrelResp{}
	return resp, nil
}

// Pubcomp TODO qos 2
func (ph *Push) Pubcomp(ctx context.Context, req *pb.PubcompReq) (*pb.PubcompResp, error) {
	if env := log.Check(zap.DebugLevel, "recv pubcomp"); env != nil {
		env.Write(zap.String("req", req.String()))
	}
	resp := &pb.PubcompResp{}
	return resp, nil
}

// DelUnack server recv the ack of message that client send
func (ph *Push) DelUnack(ctx context.Context, req *pb.DelUnackReq) (*pb.DelUnackResp, error) {
	if env := log.Check(zap.DebugLevel, "recv delunack"); env != nil {
		env.Write(zap.String("req", req.String()))
	}

	biz := req.BizID
	if !req.CleanSession {
		var err error
		biz, err = ph.store.delack(ctx, req)
		if err != nil {
			return nil, err
		}
	}

	resp := &pb.DelUnackResp{}
	start := time.Now()
	r, err := ph.call.OnACK(ctx, req.Service, ackReqChangeResp(req, biz))
	cost := time.Since(start)
	grpcSlow("callback puack slow", req.ClientID, req.TraceID, cost, ph.config.GrpcSlowlog)
	status.GetMertics().CallbackHistogramVec.WithLabelValues(req.StatLabel, "OnAck").Observe(cost.Seconds())
	if err != nil {
		log.Error("store delunack failed",
			zap.String("clientid", req.ClientID),
			zap.String("traceid", req.TraceID),
			zap.String("service", req.Service),
			zap.Bool("cleansession", req.CleanSession),
			zap.Error(err))
		log.Error("callback postreceive ack failed", errCostField(req.ClientID, req.TraceID, req.Service, cost, err)...)
		return nil, err
	}

	resp.Cookie = r.Cookie
	resp.BizID = biz
	return resp, nil
}

// Pull conn pull messages
func (ph *Push) Pull(ctx context.Context, req *pb.PullReq) (*pb.PullResp, error) {
	if env := log.Check(zap.DebugLevel, "recv pull"); env != nil {
		env.Write(zap.String("req", req.String()))
	}
	if len(req.Topic) == 0 {
		return nil, ErrTopicInvalid
	}

	resp := &pb.PullResp{}
	var err error
	resp.Offset, resp.Messages, resp.Complete, err = ph.store.pull(ctx, req)
	if err != nil {
		log.Error("store pull failed",
			zap.String("topic", req.Topic),
			zap.String("traceid", req.TraceID),
			zap.String("service", req.Service),
			zap.Int64("limit", req.Limit),
			zap.Reflect("offset", req.Offset),
			zap.Error(err))
		return nil, err
	}
	return resp, nil
}

// Addroute add router to topic
func (ph *Push) AddRoute(ctx context.Context, req *pb.AddRouteReq) (*pb.AddRouteResp, error) {
	if env := log.Check(zap.DebugLevel, "recv add route"); env != nil {
		env.Write(zap.String("req", req.String()))
	}

	resp := &pb.AddRouteResp{}
	err := ph.store.addroute(ctx, req)
	if err != nil {
		log.Error("store addroute failed",
			zap.String("topic", req.Topic),
			zap.String("traceid", req.TraceID),
			zap.String("service", req.Service),
			zap.Uint64("version", req.Version),
			zap.Error(err))
		return nil, err
	}
	return resp, nil
}

func (ph *Push) RemoveRoute(ctx context.Context, req *pb.RemoveRouteReq) (*pb.RemoveRouteResp, error) {
	if env := log.Check(zap.DebugLevel, "recv removeroute"); env != nil {
		env.Write(zap.String("req", req.String()))
	}

	resp := &pb.RemoveRouteResp{}
	if err := ph.store.removeroute(ctx, req); err != nil {
		log.Error("store pull failed",
			zap.String("topic", req.Topic),
			zap.String("traceid", req.TraceID),
			zap.String("service", req.Service),
			zap.Uint64("version", req.Version),
			zap.Error(err))
		return nil, err
	}
	return resp, nil
}
