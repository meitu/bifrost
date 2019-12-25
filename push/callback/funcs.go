package callback

import (
	"context"

	pb "github.com/meitu/bifrost/grpc/callback"
)

// OnConnect call backend with username password clientID cleansessiOn and address.
// return connection_code cookie statlabel and error
func (c *callback) OnConnect(ctx context.Context, service string, req *pb.OnConnectRequest) (*pb.OnConnectReply, error) {
	if s, ok := c.record.OnConnect[service]; ok {
		reply, err := s.OnConnect(ctx, req)
		if err != nil {
			return nil, err
		}
		return reply, nil
	}
	return &pb.OnConnectReply{}, nil
}

// OnSubscribe called On subscribtion return success and Cookie in return
func (c *callback) OnSubscribe(ctx context.Context, service string, req *pb.OnSubscribeRequest) (*pb.OnSubscribeReply, error) {
	if s, ok := c.record.OnSubscribe[service]; ok {
		reply, err := s.OnSubscribe(ctx, req)
		if err != nil {
			return nil, err
		}
		if len(reply.Cookie) == 0 {
			reply.Cookie = req.Cookie
		}
		return reply, nil
	}
	return &pb.OnSubscribeReply{Cookie: req.Cookie}, nil
}

// PostSubscribe called after subscribe succeed, return cookie and error
func (c *callback) PostSubscribe(ctx context.Context, service string, req *pb.PostSubscribeRequest) (*pb.PostSubscribeReply, error) {
	if s, ok := c.record.PostSubscribe[service]; ok {
		reply, err := s.PostSubscribe(ctx, req)
		if err != nil {
			return nil, err
		}
		if len(reply.Cookie) == 0 {
			reply.Cookie = req.Cookie
		}
		return reply, nil
	}
	return &pb.PostSubscribeReply{Cookie: req.Cookie}, nil
}

// OnPublish called On client call pbPublish, callback may return errSkip to break publish flow
// return cookid and error
func (c *callback) OnPublish(ctx context.Context, service string, req *pb.OnPublishRequest) (*pb.OnPublishReply, error) {
	if s, ok := c.record.OnPublish[service]; ok {
		reply, err := s.OnPublish(ctx, req)
		if err != nil {
			return nil, err
		}
		if len(reply.Cookie) == 0 {
			reply.Cookie = req.Cookie
		}
		if len(reply.Message) == 0 {
			reply.Message = req.Message
		}
		return reply, nil
	}
	return &pb.OnPublishReply{Skip: false, Cookie: req.Cookie, Message: req.Message}, nil
}

// OnUnsubscribe called On Unsubscribtion, return cookie and error
func (c *callback) OnUnsubscribe(ctx context.Context, service string, req *pb.OnUnsubscribeRequest) (*pb.OnUnsubscribeReply, error) {
	if s, ok := c.record.OnUnsubscribe[service]; ok {
		reply, err := s.OnUnsubscribe(ctx, req)
		if err != nil {
			return nil, err
		}
		if len(reply.Cookie) == 0 {
			reply.Cookie = req.Cookie
		}
		return reply, nil
	}
	return &pb.OnUnsubscribeReply{Cookie: req.Cookie}, nil
}

// OnDisconnect called On Disconnect. Only error return
func (c *callback) OnDisconnect(ctx context.Context, service string, req *pb.OnDisconnectRequest) (*pb.OnDisconnectReply, error) {
	if s, ok := c.record.OnDisconnect[service]; ok {
		_, err := s.OnDisconnect(ctx, req)
		if err != nil {
			return nil, err
		}
	}
	return &pb.OnDisconnectReply{}, nil
}

// OnACK called OnACK . return OnACKReply and error
func (c *callback) OnACK(ctx context.Context, service string, req *pb.OnACKRequest) (*pb.OnACKReply, error) {
	if s, ok := c.record.OnACK[service]; ok {
		reply, err := s.OnACK(ctx, req)
		if err != nil {
			return nil, err
		}
		if len(reply.Cookie) == 0 {
			reply.Cookie = req.Cookie
		}
		return reply, nil
	}
	return &pb.OnACKReply{Cookie: req.Cookie}, nil
}

// OnOffline called OnOffline. return OnOfflineReply and error
func (c *callback) OnOffline(ctx context.Context, service string, req *pb.OnOfflineRequest) (*pb.OnOfflineReply, error) {
	if s, ok := c.record.OnOffline[service]; ok {
		reply, err := s.OnOffline(ctx, req)
		if err != nil {
			return nil, err
		}
		if len(reply.Cookie) == 0 {
			reply.Cookie = req.Cookie
		}
		return reply, nil
	}
	return &pb.OnOfflineReply{Cookie: req.Cookie}, nil
}
