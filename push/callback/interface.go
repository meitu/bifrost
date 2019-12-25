package callback

import (
	pb "github.com/meitu/bifrost/grpc/callback"
	"golang.org/x/net/context"
)

type Callback interface {
	OnConnect(ctx context.Context, service string, request *pb.OnConnectRequest) (*pb.OnConnectReply, error)
	OnSubscribe(ctx context.Context, service string, request *pb.OnSubscribeRequest) (*pb.OnSubscribeReply, error)
	PostSubscribe(ctx context.Context, service string, request *pb.PostSubscribeRequest) (*pb.PostSubscribeReply, error)
	OnPublish(ctx context.Context, service string, request *pb.OnPublishRequest) (*pb.OnPublishReply, error)
	OnUnsubscribe(ctx context.Context, service string, request *pb.OnUnsubscribeRequest) (*pb.OnUnsubscribeReply, error)
	OnDisconnect(ctx context.Context, service string, request *pb.OnDisconnectRequest) (*pb.OnDisconnectReply, error)
	OnACK(ctx context.Context, service string, request *pb.OnACKRequest) (*pb.OnACKReply, error)
	OnOffline(ctx context.Context, service string, request *pb.OnOfflineRequest) (*pb.OnOfflineReply, error)
	Close() error
	String() string
}
