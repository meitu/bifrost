package store

import (
	pbMessage "github.com/meitu/bifrost/push/message"
	"github.com/meitu/bifrost/push/store/pubsub"
	"github.com/meitu/bifrost/push/store/route"
	"github.com/meitu/bifrost/push/store/session"
	"golang.org/x/net/context"
)

type KickHook func(ctx context.Context, clientID, service, conndAddress string, connectionID int64) error

type Record struct {
	Topic        string
	Qos          byte
	CurrentIndex []byte
	LastestIndex []byte
}

type Storage interface {
	ResumeSession(ctx context.Context, namespace, clientID string) (int64, []*Record, error)
	DropSession(ctx context.Context, namespace, clientID string) error

	KickIfNeeded(ctx context.Context, namespace, clientID, conndAddress string, connectionID int64, hook KickHook) error
	Offline(ctx context.Context, namespace, clientID, conndAddress string, connectionID int64, cleanSession, kick bool) error

	Subscribe(ctx context.Context, namespace, clientID string, topics []string, qoss []byte, cleanSession bool) ([][]byte, []*pbMessage.Message, error)
	Unsubscribe(ctx context.Context, namespace, clientID string, topic []string, cleanSession bool) error

	Pull(ctx context.Context, namespace, traceID, topic string, offset []byte, limit int64) ([]byte, []*pbMessage.Message, bool, error)

	RangeUnackMessages(ctx context.Context, namespace, clientID string, offset []byte, limit int64) ([]byte, []*pbMessage.Message, bool, error)
	PutUnackMessage(ctx context.Context, namespace, clientID string, cleanSession bool, unacks []*pbMessage.Message) error
	DelUnackMessage(ctx context.Context, namespace, clientID string, messageID int64) (bizID []byte, err error)

	AddRoute(ctx context.Context, namespace string, topic string, connd string, version uint64) error
	RemoveRoute(ctx context.Context, namespace string, topic string, connd string, version uint64) error
	DropTopicIfNoRoute(ctx context.Context, namespace, topic string) error

	Publish(ctx context.Context, namespace string, msg *pbMessage.Message, isRetain, sessionRecovery bool) (connds, subers []string, index []byte, err error)
	Close() error
}

type Transaction interface {
	TopicOps() pubsub.PubsubOps
	SessionOps() session.SessionOps
	RouteOps() route.RouteOps
	Rollback() error
	Commit(ctx context.Context) error
}

type Client interface {
	Start() (Transaction, error)
	Close() error
}
