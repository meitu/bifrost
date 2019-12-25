package push

import (
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/meitu/bifrost/commons/log"
	"github.com/meitu/bifrost/commons/trace"
	"github.com/meitu/bifrost/grpc/callback"
	"github.com/twinj/uuid"
	"go.uber.org/zap"

	"github.com/meitu/bifrost/commons/auth"
	lb "github.com/meitu/bifrost/commons/grpc-lb"
	pb "github.com/meitu/bifrost/grpc/publish"
	"github.com/meitu/bifrost/push/conf"
	pbMessage "github.com/meitu/bifrost/push/message"
	"github.com/meitu/bifrost/push/status"
	"golang.org/x/net/context"
)

type OfflineFunc func(ctx context.Context, service string, request *callback.OnOfflineRequest) (*callback.OnOfflineReply, error)

type Publish struct {
	store       *Datum
	config      *conf.Publish
	offlineFunc OfflineFunc
	register    *lb.Node
}

func NewPublish(config *conf.Publish, store *Datum, offlineFunc OfflineFunc) *Publish {
	ph := &Publish{
		store:       store,
		config:      config,
		offlineFunc: offlineFunc,
	}
	return ph
}

func (ps *Publish) Register(cc clientv3.Config, addr string) error {
	var err error
	ps.register, err = lb.NewNode(cc, ps.config.Service, addr)
	if err != nil {
		return err
	}
	return ps.register.Register()
}

func (ps *Publish) Deregister() error {
	log.Info("stop publish service",
		zap.String("service", ps.config.Service),
		zap.String("group", ps.config.Group))
	return ps.register.Deregister()
}

// Publish biz send messages
func (ps *Publish) Publish(ctx context.Context, req *pb.PublishRequest) (*pb.PublishReply, error) {

	resp := &pb.PublishReply{}
	traceid, err := trace.GetTraceID(ctx)
	if err != nil {
		traceid = uuid.NewV4().String()
		log.Debug("no traceid splied", zap.Error(err), zap.String("MessgeID", string(req.MessageID)),
			zap.String("new-traceid", traceid))
	}

	if env := log.Check(zap.InfoLevel, "recv biz publish"); env != nil {
		env.Write(zap.String("traceid", traceid),
			zap.String("req", req.String()))
	}
	service, err := Service(req.AppKey, ps.config.Auth, ps.config.Servname)
	if err != nil {
		resp.ReturnCode = 2
		return resp, err
	}

	resp.Results = make([]pb.ErrCode, len(req.Targets))
	resp.Cursor = make([][]byte, len(req.Targets))
	label := statlabel(ps.config.Servname, "", req.StatLabel)
	status.M.MessageSizeHistogramVec.WithLabelValues(label, "down").Observe(float64(len(req.Payload)))
	status.M.MessageCounterVec.WithLabelValues(label, "down").Inc()

	var subers []string
	for index, target := range req.Targets {
		ctime := time.Now()
		msg := &pbMessage.Message{
			Topic:      target.Topic,
			Qos:        target.Qos,
			Payload:    req.Payload,
			TraceID:    traceid,
			BizID:      req.MessageID,
			CreateTime: ctime.UnixNano(),
		}

		if req.TTL > 0 {
			msg.ExpireAt = ctime.Add(time.Duration(req.TTL) * time.Second).UnixNano()
		}

		subers, resp.Cursor[index], err = ps.store.publish(ctx, service, msg, target.IsRetain, target.NoneDowngrade)
		if err != nil {
			resp.Results[index] = pb.ErrCode_ErrInternalError
			if err == ErrNoSubscribers {
				resp.Results[index] = pb.ErrCode_ErrNoSubScribers
				log.Warn("publish biz failed",
					zap.String("traceID", traceid),
					zap.String("topic", target.Topic),
					zap.String("service", service),
					zap.Int("qos", int(target.Qos)),
					zap.Error(err))
				status.M.MessageHistogramVec.WithLabelValues(label, "down", "nosubscriber").Observe(time.Now().Sub(ctime).Seconds())
				continue
			}
			log.Error("publish biz failed",
				zap.String("traceID", traceid),
				zap.String("topic", target.Topic),
				zap.Int("qos", int(target.Qos)),
				zap.String("service", service),
				zap.Error(err))
			status.M.MessageHistogramVec.WithLabelValues(req.StatLabel, "up", "failed").Observe(time.Now().Sub(ctime).Seconds())
		}

		resp.Results[index] = pb.ErrCode_ErrOK
		status.M.MessageHistogramVec.WithLabelValues(label, "down", "sucessed").Observe(time.Now().Sub(ctime).Seconds())

		if len(subers) != 0 {
			// ignore return err
			ps.offlineFunc(ctx, service, OnOfflineRequest("biz", msg.Topic, subers, req.Payload, req.Cookie))
		}
	}

	resp.Cookie = req.Cookie

	return resp, nil
}

// DeleteQueue is not used
func (ps *Publish) DeleteQueue(ctx context.Context, req *pb.DeleteQueueRequest) (*pb.DeleteQueueReply, error) {
	/*
		if err := ps.broker.Delete(topic, ps.connd.Addrs()); err != nil {
			log.Error("Delete topic queue failed", zap.String("topic", topic), zap.Error(err))
			return nil, err
		}
	*/
	return &pb.DeleteQueueReply{}, nil
}

//Service parse service name .if service can not exist, return default service name
func Service(appkey, key, service string) (string, error) {
	if len(key) == 0 {
		return service, nil
	}
	namespace, err := auth.Verify([]byte(appkey), []byte(key))
	if err != nil {
		return "", err
	}
	if len(namespace) != 0 {
		return string(namespace), nil
	}
	return service, nil
}
