package session

import (
	"errors"
	"time"

	pbMessage "github.com/meitu/bifrost/push/message"
	util "github.com/meitu/bifrost/push/store/redis_util"
	"github.com/pingcap/tidb/kv"
)

var (
	ErrMinimalIntervalLimit = errors.New("lower than minimal kick interval, request rejected")
	ErrInvalidRecord        = errors.New("invalid record")
	ErrNotMatch             = errors.New("not match")
	ErrEmpty                = errors.New("empty")

	MinimalKickInterval time.Duration = 500
)

// ClientInfo client basic information
type ClientInfo struct {
	TimeStamp    int64  `json:"time_stamp"`
	Address      string `json:"address"`
	ConnectionID int64  `json:"connection_id"`
}

type SessionOps interface {
	// session的数据量比较小，暂时考虑不引入ObjID机制，结构实现了一部分，如果需要后面再加上

	//Create(name string) (int64, error)
	//ID(name string) (int64, error)
	Subscribe(ns, client string, topic string, qos byte) error // 订阅Topic
	Unsubscribe(ns, client string, topic string) error         // 取消订阅
	Subscriptions(ns, client string) (map[string]byte, error)  // 会话恢复

	// 游标信息 topic->cursor
	SetCursor(ns, client, topic string, cursor []byte) error // 更新cursor，为了保证幂等，不提供move方式（类似INCRBY的方式）
	Cursor(ns, client, topic string) ([]byte, error)         // 查询cursor
	DeleteCursor(ns, client, topic string) error

	ConndAddress(ns, client string) (*ClientInfo, error)
	SetConndAddress(ns, client, address string, conneciontID int64) error
	UpdateConndAddress(ns, client, address string, conneciontID int64) (*ClientInfo, error)
	DeleteConndAddress(ns, client, address string, conneciontID int64) error

	// 未ACK队列接口
	DropUnack(ns, client string) error
	Unack(ns, client string, messages []*pbMessage.Message) error
	Ack(ns, client string, mid int64) ([]byte, error)
	Unacks(ns, client string, pos []byte, count int64) (first []byte, messages []*pbMessage.Message, end bool, err error)

	// 遗言消息 willMessage
	//SetWillMessage(client string, topic string, qos byte, payload []byte) error
	//WillMessage(client string) (string, byte, []byte, error)

	// 消息ID messageID
	SetMessageID(ns, client string, messageID int64) error
	MessageID(ns, client string) (int64, error)
	DeleteMessageID(ns, client string) error

	// 删除Session信息
	Drop(ns, client string) error
}

type Option func(r *sessionOps)

type sessionOps struct {
	txn kv.Transaction
	rdc util.Redic
}

func SetTikvStore(txn kv.Transaction) Option {
	return func(r *sessionOps) {
		r.txn = txn
	}
}

func SetRedisStore(rdc util.Redic) Option {
	return func(r *sessionOps) {
		r.rdc = rdc
	}
}

func NewSessionOps(opts ...Option) SessionOps {
	s := &sessionOps{}
	for _, op := range opts {
		op(s)
	}
	if s.txn != nil {
		return NewTiSession(s.txn)
	}
	return NewRdsSession(s.rdc)
}
