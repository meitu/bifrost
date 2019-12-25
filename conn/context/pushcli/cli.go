package pushcli

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/coreos/etcd/clientv3"
	lb "github.com/meitu/bifrost/commons/grpc-lb"
	"github.com/meitu/bifrost/commons/log"
	"github.com/meitu/bifrost/conn/conf"

	pb "github.com/meitu/bifrost/grpc/push"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/resolver"
)

var (
	// ErrAleradyClosed pushcli close
	ErrAleradyClosed = errors.New("pushcli already close")

	// ErrNoEndpoints there is no the endpoint of push
	ErrNoEndpoints = errors.New("no the endpoints of push")
)

const (
	pushStarting int = iota
	pushIsReday
	pushClosed
)

// PushCli a grpc connection
// It completed the interaction with the push service
// When there is no the push service ,the status is pushStarting
// If pushcli is close ,status is pushClosed
type PushCli struct {
	pb.PushServiceClient
	mutex    sync.RWMutex
	status   int
	config   *conf.PushClient
	conn     *grpc.ClientConn
	resolver *lb.Resolver
}

// NewClient new a pushcli object by configuration and the access logger is used of the recording of log
// Support for two ways to connect to push services
// 1. Specify fixed cluster address
// 2. Through discovery services
// Grpc connection mode is non-blocking mode , the connection will modify state successfully
func NewClient(conf *conf.PushClient) (*PushCli, error) {
	push := &PushCli{
		config: conf,
		status: pushStarting,
	}

	go push.initClient()
	return push, nil
}

// initClien use mutex for security
// There are two goroutines
// 1. Try to connect to the push service
// 2. Close the client
func (p *PushCli) initClient() {
	cc := clientv3.Config{
		Endpoints: p.config.Etcd.Cluster,
		Username:  p.config.Etcd.Username,
		Password:  p.config.Etcd.Password,
	}
	p.resolver = lb.NewResolver(cc, p.config.Service)
	resolver.Register(p.resolver)

	var opts []grpc.DialOption
	opts = append(opts, grpc.WithBalancerName("round_robin"))
	opts = append(opts, grpc.WithInsecure())
	opts = append(opts, grpc.WithBlock())
	opts = append(opts, grpc.WithTimeout(p.config.DefaultTimeout))
	opts = append(opts, grpc.WithBackoffMaxDelay(p.config.BackoffTime))
	conn, err := grpc.Dial(p.resolver.URL(), opts...)
	if err != nil {
		log.Error("grpc dail failed",
			zap.String("service", p.config.Service),
			zap.Error(err))
	}
	p.conn = conn
	p.status = pushIsReday
	p.PushServiceClient = pb.NewPushServiceClient(p.conn)

	log.Info("init push client sucess",
		zap.String("service", p.config.Service),
		zap.String("group", p.config.Group),
		zap.String("region", p.config.Region),
		zap.String("appkey", p.config.AppKey))
}

func (p *PushCli) slowClientLog(msg, cid, tid, service string, cost time.Duration) {
	if cost > p.config.RequestSlowThreshold {
		log.Warn(msg,
			zap.String("clientid", cid),
			zap.String("traceid", tid),
			zap.String("service", service),
			zap.Duration("threshold", p.config.RequestSlowThreshold),
			zap.Duration("cost", cost),
		)
	}
}

func (p *PushCli) slowTopicLog(msg, topic, tid, service string, cost time.Duration) {
	if cost > p.config.RequestSlowThreshold {
		log.Warn(msg,
			zap.String("topic", topic),
			zap.String("traceid", tid),
			zap.String("service", service),
			zap.Duration("threshold", p.config.RequestSlowThreshold),
			zap.Duration("cost", cost),
		)
	}
}

func (p *PushCli) GrpcOptions() []grpc.CallOption {
	return []grpc.CallOption{grpc.WaitForReady(false)}
}

// String() return json the addresses of the push service cluster
func (p *PushCli) String() string {
	if !p.Ready() {
		return ErrNoEndpoints.Error()
	}
	addr := p.resolver.ServerAddrs()
	rawcluster, err := json.Marshal(addr)
	if err != nil {
		log.Error("error in marshal push cluster", zap.Error(err))
		return fmt.Sprintf("\"%s\"", addr)
	}
	return string(rawcluster)

}

// Ready if the push service node exists return true otherwise return false
func (p *PushCli) Ready() bool {
	p.mutex.RLock()
	defer p.mutex.RUnlock()
	return p.status == pushIsReday
}

// Close the connection to release resources
func (p *PushCli) Close() error {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	if p.status == pushClosed {
		return ErrAleradyClosed
	}
	p.status = pushClosed
	p.resolver.Close()
	return p.conn.Close()
}
