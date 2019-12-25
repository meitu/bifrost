package push

import (
	"net"

	"github.com/coreos/etcd/clientv3"
	"github.com/meitu/bifrost/commons/log"
	pub "github.com/meitu/bifrost/grpc/publish"
	pb "github.com/meitu/bifrost/grpc/push"
	"github.com/meitu/bifrost/push/callback"
	"github.com/meitu/bifrost/push/conf"
	"github.com/meitu/bifrost/push/conncli"
	"go.uber.org/zap"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

type Server struct {
	config  *conf.Server
	push    *Push
	publish *Publish

	server *grpc.Server
	ctx    context.Context
	cancel context.CancelFunc
}

func NewServer(config *conf.Server) (*Server, error) {
	ctx, cancel := context.WithCancel(context.Background())
	// conncli
	conn, err := conncli.NewConnd(&config.Connd)
	if err != nil {
		log.Error("connd new failed", zap.Error(err))
		return nil, err
	}
	// callback
	call, err := callback.NewCallback(&config.Callback, ctx)
	if err != nil {
		log.Error("callback new failed", zap.Error(err))
		return nil, err
	}

	store, err := NewDatum(&config.Store, conn)
	if err != nil {
		log.Error("store new failed", zap.Error(err))
		return nil, err
	}

	srv := &Server{
		config:  config,
		push:    NewPush(&config.Push, call, store),
		publish: NewPublish(&config.Publish, store, call.OnOffline),
		ctx:     ctx,
		cancel:  cancel,
	}

	if srv.server, err = srv.register(); err != nil {
		log.Error("regiser server failed", zap.Error(err))
		return srv, nil
	}

	return srv, err
}

func (srv *Server) register() (*grpc.Server, error) {
	ss := grpc.NewServer()
	pb.RegisterPushServiceServer(ss, srv.push)
	pub.RegisterPublishServiceServer(ss, srv.publish)
	return ss, nil
}

func (srv *Server) Serve(lis net.Listener) error {
	cc := clientv3.Config{
		Endpoints: srv.config.Etcd.Cluster,
		Username:  srv.config.Etcd.Username,
		Password:  srv.config.Etcd.Password,
	}

	if err := srv.push.Register(cc, lis.Addr().String()); err != nil {
		log.Fatal("register push server failed", zap.Error(err))
	}

	if err := srv.publish.Register(cc, lis.Addr().String()); err != nil {
		log.Fatal("register publish server failed", zap.Error(err))
	}

	if err := srv.server.Serve(lis); err != nil {
		log.Error("publish service failed", zap.Error(err))
		return err
	}
	return nil
}

func (srv *Server) Stop() error {
	srv.publish.Deregister()
	srv.push.Deregister()
	srv.server.Stop()
	srv.cancel()
	return nil
}

func (srv *Server) GracefulStop() error {
	log.Info("graceful stop push servvice",
		zap.String("region", srv.config.Region))
	srv.server.GracefulStop()
	srv.cancel()
	return nil
}
