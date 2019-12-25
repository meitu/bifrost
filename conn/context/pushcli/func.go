package pushcli

import (
	"context"
	"time"

	"github.com/meitu/bifrost/conn/status/metrics"
	pb "github.com/meitu/bifrost/grpc/push"
)

// Connect complete the client connection operation
func (p *PushCli) Connect(ctx context.Context, req *pb.ConnectReq) (*pb.ConnectResp, error) {
	if !p.Ready() {
		return nil, ErrNoEndpoints
	}
	opts := p.GrpcOptions()
	start := time.Now()
	resp, err := p.PushServiceClient.Connect(ctx, req, opts...)
	cost := time.Since(start)
	p.slowClientLog("grpc connect slow log", req.ClientID, req.TraceID, req.Service, cost)
	metrics.GetMetrics().GrpcHistogramVec.WithLabelValues("Connect").Observe(cost.Seconds())
	return resp, err
}

// Disconnect complete the client disconnection operation
func (p *PushCli) Disconnect(ctx context.Context, req *pb.DisconnectReq) (*pb.DisconnectResp, error) {
	if !p.Ready() {
		return nil, ErrNoEndpoints
	}
	opts := p.GrpcOptions()
	start := time.Now()
	resp, err := p.PushServiceClient.Disconnect(ctx, req, opts...)
	cost := time.Since(start)
	p.slowClientLog("grpc disconnect slow log", req.ClientID, req.TraceID, req.Service, cost)
	metrics.GetMetrics().GrpcHistogramVec.WithLabelValues("Disconnect").Observe(cost.Seconds())
	return resp, err
}

// Subscribe complete the client subscribe operation
func (p *PushCli) Subscribe(ctx context.Context, req *pb.SubscribeReq) (*pb.SubscribeResp, error) {
	if !p.Ready() {
		return nil, ErrNoEndpoints
	}
	opts := p.GrpcOptions()
	start := time.Now()
	resp, err := p.PushServiceClient.Subscribe(ctx, req, opts...)
	cost := time.Since(start)
	if err != nil {
		return resp, err
	}
	p.slowClientLog("grpc subscribe slow log", req.ClientID, req.TraceID, req.Service, cost)
	metrics.GetMetrics().GrpcHistogramVec.WithLabelValues("Subscribe").Observe(cost.Seconds())
	return resp, nil

}

// Unsubscribe complete the client unsubscribe operation
func (p *PushCli) Unsubscribe(ctx context.Context, req *pb.UnsubscribeReq) (*pb.UnsubscribeResp, error) {
	if !p.Ready() {
		return nil, ErrNoEndpoints
	}
	opts := p.GrpcOptions()
	start := time.Now()
	resp, err := p.PushServiceClient.Unsubscribe(ctx, req, opts...)
	cost := time.Since(start)
	p.slowClientLog("grpc unsubscribe slow log", req.ClientID, req.TraceID, req.Service, cost)
	metrics.GetMetrics().GrpcHistogramVec.WithLabelValues("UnSubscribe").Observe(cost.Seconds())
	return resp, err
}

// Publish complete the client publish operation
func (p *PushCli) Publish(ctx context.Context, req *pb.PublishReq) (*pb.PublishResp, error) {
	if !p.Ready() {
		return nil, ErrNoEndpoints
	}
	opts := p.GrpcOptions()
	start := time.Now()
	resp, err := p.PushServiceClient.MQTTPublish(ctx, req, opts...)
	cost := time.Since(start)
	p.slowClientLog("grpc mqttpublish slow log", req.ClientID, req.Message.TraceID, req.Service, cost)
	metrics.GetMetrics().GrpcHistogramVec.WithLabelValues("Publish").Observe(cost.Seconds())
	return resp, err
}

// Pubrec complete the client pubrec operation
func (p *PushCli) Pubrec(ctx context.Context, req *pb.PubrecReq) (*pb.PubrecResp, error) {
	if !p.Ready() {
		return nil, ErrNoEndpoints
	}
	opts := p.GrpcOptions()
	start := time.Now()
	resp, err := p.PushServiceClient.Pubrec(ctx, req, opts...)
	cost := time.Since(start)
	p.slowClientLog("grp pubrec slow log", req.ClientID, req.TraceID, req.Service, cost)
	metrics.GetMetrics().GrpcHistogramVec.WithLabelValues("Pubrec").Observe(cost.Seconds())
	return resp, err
}

// Pubrel complete the client pubrel operation
func (p *PushCli) Pubrel(ctx context.Context, req *pb.PubrelReq) (*pb.PubrelResp, error) {
	if !p.Ready() {
		return nil, ErrNoEndpoints
	}
	opts := p.GrpcOptions()
	start := time.Now()
	resp, err := p.PushServiceClient.Pubrel(ctx, req, opts...)
	cost := time.Since(start)
	p.slowClientLog("grpc pubrel slow log", req.ClientID, req.TraceID, req.Service, cost)
	metrics.GetMetrics().GrpcHistogramVec.WithLabelValues("Pubrel").Observe(cost.Seconds())
	return resp, err
}

// Pubcomp complete the client pubcomp operation
func (p *PushCli) Pubcomp(ctx context.Context, req *pb.PubcompReq) (*pb.PubcompResp, error) {
	if !p.Ready() {
		return nil, ErrNoEndpoints
	}
	opts := p.GrpcOptions()
	start := time.Now()
	resp, err := p.PushServiceClient.Pubcomp(ctx, req, opts...)
	cost := time.Since(start)
	p.slowClientLog("grpc pubcomp slow log", req.ClientID, req.TraceID, req.Service, cost)
	metrics.GetMetrics().GrpcHistogramVec.WithLabelValues("Pubcomp").Observe(cost.Seconds())
	return resp, err
}

// RangeUnack complete the client rangeunack operation
func (p *PushCli) RangeUnack(ctx context.Context, req *pb.RangeUnackReq) (*pb.RangeUnackResp, error) {
	if !p.Ready() {
		return nil, ErrNoEndpoints
	}
	opts := p.GrpcOptions()
	start := time.Now()
	resp, err := p.PushServiceClient.RangeUnack(ctx, req, opts...)
	cost := time.Since(start)
	p.slowClientLog("grpc rangeunack slow log", req.ClientID, req.TraceID, req.Service, cost)
	metrics.GetMetrics().GrpcHistogramVec.WithLabelValues("RangeUnack").Observe(cost.Seconds())
	return resp, err
}

// PutUnack complete the client putunack operation
func (p *PushCli) PutUnack(ctx context.Context, req *pb.PutUnackReq) (*pb.PutUnackResp, error) {
	if !p.Ready() {
		return nil, ErrNoEndpoints
	}
	opts := p.GrpcOptions()
	start := time.Now()
	resp, err := p.PushServiceClient.PutUnack(ctx, req, opts...)
	cost := time.Since(start)
	p.slowClientLog("grpc putunack slow log", req.ClientID, req.TraceID, req.Service, cost)
	metrics.GetMetrics().GrpcHistogramVec.WithLabelValues("PutUnack").Observe(cost.Seconds())
	return resp, err
}

// DelUnack complete the client delunack operation
func (p *PushCli) DelUnack(ctx context.Context, req *pb.DelUnackReq) (*pb.DelUnackResp, error) {
	if !p.Ready() {
		return nil, ErrNoEndpoints
	}
	opts := p.GrpcOptions()
	start := time.Now()
	resp, err := p.PushServiceClient.DelUnack(ctx, req, opts...)
	cost := time.Since(start)
	p.slowClientLog("grpc deluncak slow log", req.ClientID, req.TraceID, req.Service, cost)
	metrics.GetMetrics().GrpcHistogramVec.WithLabelValues("DelUnack").Observe(cost.Seconds())
	return resp, err
}

// PostSubscribe complete the client postsubscribe operation
func (p *PushCli) PostSubscribe(ctx context.Context, req *pb.PostSubscribeReq) (*pb.PostSubscribeResp, error) {
	if !p.Ready() {
		return nil, ErrNoEndpoints
	}
	opts := p.GrpcOptions()
	start := time.Now()
	resp, err := p.PushServiceClient.PostSubscribe(ctx, req, opts...)
	cost := time.Since(start)
	p.slowClientLog("grpc postsubscribe slow log", req.ClientID, req.TraceID, req.Service, cost)
	metrics.GetMetrics().GrpcHistogramVec.WithLabelValues("PostSubscribe").Observe(cost.Seconds())
	return resp, err
}

// AddRoute complete the client addroute operation
func (p *PushCli) AddRoute(ctx context.Context, req *pb.AddRouteReq) (*pb.AddRouteResp, error) {
	if !p.Ready() {
		return nil, ErrNoEndpoints
	}
	opts := p.GrpcOptions()
	start := time.Now()
	resp, err := p.PushServiceClient.AddRoute(ctx, req, opts...)
	cost := time.Since(start)
	p.slowTopicLog("grpc addroute slow log", req.Topic, req.TraceID, req.Service, cost)
	metrics.GetMetrics().GrpcHistogramVec.WithLabelValues("AddRoute").Observe(cost.Seconds())
	return resp, err
}

// RemoveRoute complete the client removeroute operation
func (p *PushCli) RemoveRoute(ctx context.Context, req *pb.RemoveRouteReq) (*pb.RemoveRouteResp, error) {
	if !p.Ready() {
		return nil, ErrNoEndpoints
	}
	opts := p.GrpcOptions()
	start := time.Now()
	resp, err := p.PushServiceClient.RemoveRoute(ctx, req, opts...)
	cost := time.Since(start)
	p.slowTopicLog("grpc removeroute slow log", req.Topic, req.TraceID, req.Service, cost)
	metrics.GetMetrics().GrpcHistogramVec.WithLabelValues("RemoveRoute").Observe(cost.Seconds())
	return resp, err
}

// Pull complete the client pull operation
func (p *PushCli) Pull(ctx context.Context, req *pb.PullReq) (*pb.PullResp, error) {
	if !p.Ready() {
		return nil, ErrNoEndpoints
	}
	opts := p.GrpcOptions()
	start := time.Now()
	resp, err := p.PushServiceClient.Pull(ctx, req, opts...)
	cost := time.Since(start)
	p.slowTopicLog("grpc pull slow log", req.Topic, req.TraceID, req.Service, cost)
	metrics.GetMetrics().GrpcHistogramVec.WithLabelValues("Pull").Observe(cost.Seconds())
	return resp, err
}
