package push

import (
	"time"

	"github.com/meitu/bifrost/commons/log"
	"github.com/meitu/bifrost/grpc/callback"
	pb "github.com/meitu/bifrost/grpc/push"
	pbMessage "github.com/meitu/bifrost/push/message"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func connReqChangeResp(req *pb.ConnectReq) *callback.OnConnectRequest {
	return &callback.OnConnectRequest{
		Username:     req.Username,
		Password:     req.Password,
		ClientID:     req.ClientID,
		CleanSession: req.CleanSession,
		Address:      req.ClientAddress,
	}
}

func subReqChangeResp(req *pb.SubscribeReq) *callback.OnSubscribeRequest {
	resp := &callback.OnSubscribeRequest{
		ClientID: req.ClientID,
		Cookie:   req.Cookie,
	}
	for i := range req.Topics {
		resp.SubTopics = append(resp.SubTopics, &callback.SubscribeTopics{
			Topic: req.Topics[i],
			Qos:   int32(req.Qoss[i]),
		})
	}
	return resp
}

func postReqChangeResp(req *pb.PostSubscribeReq) *callback.PostSubscribeRequest {
	resp := &callback.PostSubscribeRequest{
		Cookie:   req.Cookie,
		ClientID: req.ClientID,
	}
	for i := range req.Topics {
		resp.SubTopics = append(resp.SubTopics, &callback.SubscribeTopics{
			Topic: req.Topics[i],
			Qos:   int32(req.Qoss[i]),
		})
	}
	return resp
}

func pubReqChangeResp(req *pb.PublishReq) *callback.OnPublishRequest {
	return &callback.OnPublishRequest{
		Topic:     req.Message.Topic,
		Qos:       int32(req.Message.Qos),
		Message:   []byte(req.Message.Payload),
		Cookie:    req.Cookie,
		ClientID:  req.ClientID,
		StatLabel: req.StatLabel,
	}
}

func unsubReqChangeResp(req *pb.UnsubscribeReq) *callback.OnUnsubscribeRequest {
	return &callback.OnUnsubscribeRequest{
		Topics:         req.Topics,
		Cookie:         req.Cookie,
		ClientID:       req.ClientID,
		LostConnection: req.Lost,
	}
}

func disReqChangeResp(req *pb.DisconnectReq) *callback.OnDisconnectRequest {
	return &callback.OnDisconnectRequest{
		ClientID:       req.ClientID,
		Cookie:         req.Cookie,
		LostConnection: req.Lost,
	}
}

func ackReqChangeResp(req *pb.DelUnackReq, biz []byte) *callback.OnACKRequest {
	return &callback.OnACKRequest{
		ClientID: req.ClientID,
		Cookie:   req.Cookie,
		BizID:    biz,
		// Topic : req.Topic
	}
}

func OnOfflineRequest(cid, topic string, subers []string, message, cookie []byte) *callback.OnOfflineRequest {
	return &callback.OnOfflineRequest{
		ClientID: cid,
		Topic:    topic,
		Subers:   subers,
		Cookie:   cookie,
		Message:  message,
	}
}

func statlabel(servName, traceid, label string) string {
	if label == "" {
		log.Debug("publish statlabel is null",
			zap.String("default-label", servName),
			zap.String("tid", traceid))
		return servName
	}
	return label
}

func servname(name string, servname string) string {
	if name != "" {
		return name
	}
	return servname
}

func grpcSlow(msg, cid, tid string, cost time.Duration, slow time.Duration) {
	if cost > slow {
		log.Warn(msg, zap.String("clientid", cid),
			zap.String("traceid", tid),
			zap.Duration("cost", cost),
			zap.Duration("threshold", slow))
	}
}

func tikvSlow(msg, id, tid string, cost time.Duration, slow time.Duration) {
	if cost > slow {
		log.Warn(msg, zap.String("key", id),
			zap.String("traceid", tid),
			zap.Duration("cost", cost),
			zap.Duration("threshold", slow))
	}
}

func errCostField(cid, tid, service string, cost time.Duration, err error) []zapcore.Field {
	fields := make([]zapcore.Field, 5)
	fields[0] = zap.String("clientid", cid)
	fields[1] = zap.String("traceid", tid)
	fields[2] = zap.String("service", service)
	fields[3] = zap.Duration("cost", cost)
	fields[4] = zap.Error(err)
	return fields
}

func storeMsgsChangeGrpcMsgs(msgs []*pbMessage.Message, retain bool) []*pb.Message {
	pbMsgs := make([]*pb.Message, 0, len(msgs))
	for _, msg := range msgs {
		if msg == nil {
			continue
		}
		pbMsgs = append(pbMsgs, storeMsgChangeGrpcMsg(msg, retain))
	}
	return pbMsgs
}

func storeMsgChangeGrpcMsg(msg *pbMessage.Message, retain bool) *pb.Message {
	return &pb.Message{
		Payload: msg.Payload,
		Qos:     int32(msg.Qos),
		Retain:  retain,
		Topic:   msg.Topic,
		TraceID: msg.TraceID,
		Index:   msg.Index,
		BizID:   msg.BizID,
	}
}

func storeUnacksChangeGrpcUnacks(msgs []*pbMessage.Message) []*pb.UnackDesc {
	pbMsgs := make([]*pb.UnackDesc, 0, len(msgs))
	for _, msg := range msgs {
		if msg == nil {
			continue
		}
		pbMsgs = append(pbMsgs, &pb.UnackDesc{
			Payload:   msg.Payload,
			Topic:     msg.Topic,
			TraceID:   msg.TraceID,
			MessageID: msg.MessageID,
			BizID:     msg.BizID,
			// not used
			Index: msg.Index,
		},
		)
	}
	return pbMsgs
}

func grpcUnacksChangeStoreUnacks(msgs []*pb.UnackDesc) []*pbMessage.Message {
	messages := make([]*pbMessage.Message, len(msgs))
	for index, msg := range msgs {
		messages[index] = &pbMessage.Message{
			MessageID: msg.MessageID,
			Index:     msg.Index,
			BizID:     msg.BizID,
			// BizID is not used
			Topic:   msg.Topic,
			Payload: msg.Payload,
			TraceID: msg.TraceID,
		}
	}
	return messages
}
