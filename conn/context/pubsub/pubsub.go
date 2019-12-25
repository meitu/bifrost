package pubsub

import (
	"context"
	"errors"
	"sync"

	"github.com/meitu/bifrost/commons/log"
	"github.com/meitu/bifrost/conn/conf"
	"github.com/meitu/bifrost/conn/context/pushcli"
	conn "github.com/meitu/bifrost/grpc/conn"
	ph "github.com/meitu/bifrost/grpc/push"

	"go.uber.org/zap"
)

// Pubsub use save the topic object for service+topic
type Pubsub struct {
	mu       sync.RWMutex
	topicMap map[string]*Subscribers

	config  *conf.Pubsub
	ctx     context.Context
	pushcli *pushcli.PushCli
}

// NewPubsub create new a pubsub object
func NewPubsub(config *conf.Pubsub, ctx context.Context, pushcli *pushcli.PushCli) *Pubsub {
	pubsub := &Pubsub{
		topicMap: make(map[string]*Subscribers, 1),
		config:   config,
		ctx:      ctx,
		pushcli:  pushcli,
	}
	return pubsub
}

// Put find a topic object from topicMap, create new a topic object if not exist then send addroute request
func (pubsub *Pubsub) AddRoute(req *ph.AddRouteReq, c interface{}) error {
	pubsub.mu.Lock()
	t, exist := pubsub.topicMap[req.Service+req.Topic]
	if !exist {
		t = NewSubscribers(pubsub.ctx, &pubsub.config.Topic, pubsub.pushcli)
		pubsub.topicMap[req.Service+req.Topic] = t
	}
	t.Incr()
	pubsub.mu.Unlock()

	if !exist {
		if err := t.AddRoute(req); err != nil {
			return err
		}
	}

	t.Register(c)
	return nil
}

// Delete delete a topic object from topicMap if the num of topic size is zero then send remove request
func (pubsub *Pubsub) RemoveRoute(req *ph.RemoveRouteReq, c interface{}) error {
	pubsub.mu.Lock()
	t, exist := pubsub.topicMap[req.Service+req.Topic]
	if !exist {
		log.Warn("delete not exist topic",
			zap.String("topic", req.Topic),
			zap.String("service", req.Service))
		pubsub.mu.Unlock()
		return nil
	}

	currentCount := t.Decr()
	if currentCount == 0 {
		delete(pubsub.topicMap, req.Service+req.Topic)
	}
	pubsub.mu.Unlock()

	if currentCount == 0 {
		if err := t.DelRoute(req); err != nil {
			return err
		}
	}

	t.Deregister(c)
	return nil
}

// Size return the topic object is size
func (pubsub *Pubsub) Size(service, topic string) int {
	t, exist := pubsub.get(service, topic)
	if !exist {
		return 0
	}
	return int(t.Count())
}

// Scan call topic the function of scan
func (pubsub *Pubsub) Scan(service, topic string, f func(interface{}) error) {
	t, exist := pubsub.get(service, topic)
	if !exist {
		log.Warn("scan not exist topic",
			zap.String("topic", topic),
			zap.String("service", service),
		)
		return
	}
	t.Scan(f)
}

// Notify call topic the function of notify
func (pubsub *Pubsub) Notify(req *conn.NotifyReq, handler NotifyHandler) {
	t, exist := pubsub.get(req.Service, req.Topic)
	if !exist {
		log.Warn("notify not exist topic",
			zap.String("topic", req.Topic),
			zap.String("service", req.Service),
		)
		return
	}
	t.Notify(req, handler)
}

// Pull call topic the function of pull
func (pubsub *Pubsub) Pull(ctx context.Context, req *ph.PullReq) (*ph.PullResp, error) {
	t, exist := pubsub.get(req.Service, req.Topic)
	if !exist {
		log.Warn("notify not exist topic",
			zap.String("topic", req.Topic),
			zap.String("service", req.Service))
		return nil, errors.New("topic is not exist")
	}
	return t.Pull(ctx, req)
}

func (pubsub *Pubsub) get(service, topic string) (*Subscribers, bool) {
	pubsub.mu.Lock()
	t, exist := pubsub.topicMap[service+topic]
	pubsub.mu.Unlock()
	return t, exist
}
