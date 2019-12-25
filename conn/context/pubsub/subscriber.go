package pubsub

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/meitu/bifrost/commons/log"
	"github.com/meitu/bifrost/conn/conf"
	"github.com/meitu/bifrost/conn/context/pushcli"
	"github.com/meitu/bifrost/conn/status/metrics"

	conn "github.com/meitu/bifrost/grpc/conn"
	ph "github.com/meitu/bifrost/grpc/push"
	"go.uber.org/zap"
)

// Subscribers ensure concurrency security on add route and remove route
// pull data asynchronously and store cache
// manage conn set
type Subscribers struct {
	bs *BigSet

	cache *Cache

	cf              *conf.Topic
	routeMutex      sync.Mutex
	subscribesCount int32

	ctx     context.Context
	cancel  context.CancelFunc
	pushcli *pushcli.PushCli
}

// NewSubscribers create new a topic object
func NewSubscribers(ctx context.Context, cf *conf.Topic, pushcli *pushcli.PushCli) *Subscribers {
	cctx, cancel := context.WithCancel(ctx)
	t := &Subscribers{
		cf:      cf,
		bs:      NewBigSet(cf.DiscardThreshold),
		ctx:     cctx,
		cancel:  cancel,
		pushcli: pushcli,
	}

	return t
}

func (t *Subscribers) Incr() int32 {
	return atomic.AddInt32(&t.subscribesCount, 1)
}

func (t *Subscribers) Decr() int32 {
	return atomic.AddInt32(&t.subscribesCount, -1)
}

func (t *Subscribers) Register(c interface{}) {
	t.bs.Put(c)
}

func (t *Subscribers) Deregister(c interface{}) {
	t.bs.Delete(c)
}

func (t *Subscribers) Count() int32 {
	return atomic.LoadInt32(&t.subscribesCount)
}

func (t *Subscribers) Scan(handler func(c interface{}) error) {
	t.bs.Scan(handler)
}

// AddRoute send addroute requst if the frist time topic is created
// guaranteed topic refers to adding a route once
func (t *Subscribers) AddRoute(req *ph.AddRouteReq) error {
	t.routeMutex.Lock()
	defer t.routeMutex.Unlock()
	_, err := t.pushcli.AddRoute(t.ctx, req)
	if err != nil {
		log.Error("addroute failed",
			zap.String("topic", req.Topic),
			zap.String("service", req.Service),
			zap.String("traceid", req.TraceID),
			zap.String("address", req.GrpcAddress),
			zap.Uint64("version", req.Version),
			zap.Error(err))
		return err
	}
	return nil
}

// DelRoute send removeroute requst if the addroute is done
func (t *Subscribers) DelRoute(req *ph.RemoveRouteReq) error {
	t.routeMutex.Lock()
	defer t.routeMutex.Unlock()
	_, err := t.pushcli.RemoveRoute(t.ctx, req)
	if err != nil {
		log.Error("remove route failed",
			zap.String("topic", req.Topic),
			zap.String("service", req.Service),
			zap.String("traceid", req.TraceID),
			zap.String("address", req.GrpcAddress),
			zap.Uint64("version", req.Version),
			zap.Error(err))
		return err
	}
	return nil
}

type NotifyHandler func(c interface{}, topic string, last []byte)

//Notify all conns and pull message
func (t *Subscribers) Notify(nr *conn.NotifyReq, notifyHandler NotifyHandler) {
	// notify all connection
	timer := time.NewTimer(t.cf.DowngradeLimit)
	if !timer.Stop() {
		<-timer.C
	}
	length := t.bs.ValidSize()

	if length > t.cf.DowngradeThreshold && !nr.NoneDowngrade {
		timer.Reset(t.cf.DowngradeLimit)
	}

	var count int
	handler := func(c interface{}) error {
		select {
		case <-t.ctx.Done():
			log.Info("topic is cancel",
				zap.String("topic", nr.Topic),
				zap.String("service", nr.Service),
				zap.String("traceid", nr.TraceID),
			)
			return errors.New("topic is canceled")
		case <-timer.C:
			log.Info("notify the client of a timeout",
				zap.String("topic", nr.Topic),
				zap.String("service", nr.Service),
				zap.String("traceid", nr.TraceID),
				zap.Duration("limit", t.cf.DowngradeLimit),
				zap.Int("threshold", t.cf.DowngradeThreshold),
			)
			return errors.New("notify the client of a timeout")
		default:
			count++
			notifyHandler(c, nr.Topic, nr.Index)
			return nil
		}
	}

	start := time.Now()
	t.bs.Scan(handler)
	cost := time.Since(start)
	metrics.GetMetrics().TopicScanDuration.Observe(cost.Seconds())

	if cost > 100*time.Millisecond {
		log.Warn("slow log in notify handler",
			zap.String("topic", nr.Topic),
			zap.String("service", nr.Service),
			zap.String("traceid", nr.TraceID),
			zap.Int("notify-count", count),
			zap.Int("total-count", t.bs.TotalSize()),
			zap.Int("vailid-count", t.bs.ValidSize()),
			zap.Duration("cost", cost))
		return
	}

	if env := log.Check(zap.DebugLevel, "notify all client in topic"); env != nil {
		env.Write(zap.String("topic", nr.Topic),
			zap.String("service", nr.Service),
			zap.String("traceid", nr.TraceID),
			zap.Int("notify-count", count),
			zap.Int("total-count", t.bs.TotalSize()),
			zap.Int("vailid-count", t.bs.ValidSize()),
			zap.Duration("cost", cost))
	}
}

//TODO Destory
func (t *Subscribers) Destory() {
	t.cancel()
	t.bs.Destory()
}

// Pull messages from cache or pushd
func (t *Subscribers) Pull(ctx context.Context, req *ph.PullReq) (*ph.PullResp, error) {
	if t.cf.CacheEnabled && t.bs.ValidSize() > 1 {
		return t.GetCache(ctx, req)
	}
	return t.pull(ctx, req)
}

func (t *Subscribers) pull(ctx context.Context, req *ph.PullReq) (*ph.PullResp, error) {
	resp, err := t.pushcli.Pull(ctx, req)
	if err != nil {
		log.Error("pull failed",
			zap.String("topic", req.Topic),
			zap.String("service", req.Service),
			zap.String("traceid", req.TraceID),
			zap.Error(err))
		return nil, err
	}
	return resp, nil
}

// GetCache
func (t *Subscribers) GetCache(ctx context.Context, req *ph.PullReq) (*ph.PullResp, error) {
	if atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&t.cache))) == nil {
		atomic.CompareAndSwapPointer((*unsafe.Pointer)(unsafe.Pointer(&t.cache)), nil, (unsafe.Pointer)(newCache(t.cf.CacheBufferSize)))
	}

	if t.cache.expire(req.Offset) {
		return t.pull(ctx, req)
	}
	metrics.GetMetrics().CacheCount.Inc()
	t.cache.mu.Lock()
	defer t.cache.mu.Unlock()
	if resp := t.cache.getResp(req.Offset); resp != nil {
		metrics.GetMetrics().CacheHintCount.Inc()
		return resp, nil
	}
	resp, err := t.pull(ctx, req)
	if err != nil {
		return resp, err
	}
	if l := len(resp.Messages); l > 0 {
		resp.Offset = append(resp.Messages[l-1].Index, byte(0))
	}
	t.cache.putMessages(req.Offset, resp.Messages)
	return resp, nil
}
