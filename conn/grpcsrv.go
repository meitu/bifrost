package conn

import (
	"context"
	"net"

	"github.com/meitu/bifrost/commons/log"
	gctx "github.com/meitu/bifrost/conn/context"
	"github.com/meitu/bifrost/conn/session"
	conn "github.com/meitu/bifrost/grpc/conn"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

type GrpcServer struct {
	srv *grpc.Server
	ctx *gctx.GrpcSrvContext
}

func NewGrpcServer(ctx *gctx.GrpcSrvContext) *GrpcServer {
	s := &GrpcServer{
		srv: grpc.NewServer(),
		ctx: ctx,
	}
	conn.RegisterConnServiceServer(s.srv, s)
	return s
}

func (s *GrpcServer) Serve(lis net.Listener) error {
	return s.srv.Serve(lis)
}

func (s *GrpcServer) Stop() error {
	s.srv.Stop()
	return nil
}

func (s *GrpcServer) GracefulStop() error {
	s.srv.GracefulStop()
	return nil
}

func (srv *GrpcServer) Disconnect(ctx context.Context, req *conn.DisconnectReq) (*conn.DisconnectResp, error) {
	if env := log.Check(zap.InfoLevel, "recv grpc disconnect"); env != nil {
		env.Write(zap.String("clientid", req.ClientID),
			zap.String("service", req.Service),
			zap.String("traceID", req.TraceID),
			zap.Int64("connectionID", req.ConnectionID))
	}
	// TODO we will not handle error at all
	resp := &conn.DisconnectResp{}
	select {
	case <-ctx.Done():
		log.Error("notify cancel request", zap.String("traceID", req.TraceID),
			zap.String("clientID", req.ClientID), zap.Int64("connectionID", req.ConnectionID), zap.Error(ctx.Err()))
	default:
		// disconnect old connection
		if c := srv.ctx.Clients.Get(req.Service + req.ClientID); c != nil {
			if c.(*session.Session).KickClose(req.ConnectionID) {
				log.Info("server close connection", zap.String("clientid", req.ClientID),
					zap.Int64("connectionid", req.ConnectionID), zap.String("traceid", req.TraceID))
				return resp, nil
			}
		}
		log.Error("disconnect connection failed", zap.String("clientid", req.ClientID), zap.Int64("cid", req.ConnectionID))
	}
	return resp, nil
}

func (srv *GrpcServer) Notify(ctx context.Context, req *conn.NotifyReq) (*conn.NotifyResp, error) {
	if env := log.Check(zap.InfoLevel, "recv grpc notify"); env != nil {
		env.Write(zap.String("traceID", req.TraceID),
			zap.String("req", req.String()))
	}

	resp := &conn.NotifyResp{}
	select {
	case <-ctx.Done():
		log.Error("notify cancel request", zap.String("traceID", req.TraceID),
			zap.String("topic", req.Topic), zap.Error(ctx.Err()))
	default:
		// type NotifyHandler func(c Conn, topic string, last []byte)
		handler := func(c interface{}, topic string, last []byte) {
			(*c.(*session.NotifyHandler))(topic, last)
		}
		srv.ctx.Pubsub.Notify(req, handler)
	}
	return resp, nil
}
