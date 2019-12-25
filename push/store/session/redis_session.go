package session

import (
	"encoding/json"
	"sort"
	"time"

	"github.com/garyburd/redigo/redis"
	"github.com/gogo/protobuf/proto"
	"github.com/meitu/bifrost/commons/log"
	pbMessage "github.com/meitu/bifrost/push/message"
	util "github.com/meitu/bifrost/push/store/redis_util"
	"go.uber.org/zap"
)

type RdsSession struct {
	rdc util.Redic
}

func NewRdsSession(rdc util.Redic) *RdsSession {
	return &RdsSession{
		rdc: rdc,
	}
}

func (s *RdsSession) subPrefix(ns, client string) string {
	return util.SysPrefix + util.DefaultDelimiter + "csub" + util.DefaultDelimiter + ns + util.DefaultDelimiter + client
}

func (s *RdsSession) curPrefix(ns, client string) string {
	return util.SysPrefix + util.DefaultDelimiter + "cur" + util.DefaultDelimiter + ns + util.DefaultDelimiter + client
}

func (s *RdsSession) messagePrefix(ns, client string) string {
	return util.SysPrefix + util.DefaultDelimiter + "message" + util.DefaultDelimiter + ns + util.DefaultDelimiter + client
}

func (s *RdsSession) unackPrefix(ns, client string) string {
	return util.SysPrefix + util.DefaultDelimiter + "unack" + util.DefaultDelimiter + ns + util.DefaultDelimiter + client
}

func (s *RdsSession) infoPrefix(ns, client string) string {
	return util.SysPrefix + util.DefaultDelimiter + "info" + util.DefaultDelimiter + ns + util.DefaultDelimiter + client
}

func (s *RdsSession) Subscribe(ns, client string, topic string, qos byte) error {
	key := s.subPrefix(ns, client)
	rds := s.rdc.Get(util.W, key)
	defer rds.Close()
	_, err := rds.Do("HSET", key, topic, int(qos))
	if err != nil {
		return err
	}
	return nil
}

func (s *RdsSession) Unsubscribe(ns, client string, topic string) error {
	key := s.subPrefix(ns, client)
	rds := s.rdc.Get(util.W, key)
	defer rds.Close()
	_, err := rds.Do("HDEL", key, topic)
	return err
}

func (s *RdsSession) Subscriptions(ns, client string) (map[string]byte, error) {
	key := s.subPrefix(ns, client)
	rds := s.rdc.Get(util.R, key)
	defer rds.Close()
	raws, err := redis.IntMap(rds.Do("HGETALL", key))
	if err != nil {
		return nil, err
	}

	subs := make(map[string]byte)
	for k, v := range raws {
		subs[k] = byte(v)
	}

	return subs, nil
}

func (s *RdsSession) DropSub(ns, client string) error {
	key := s.subPrefix(ns, client)
	rds := s.rdc.Get(util.W, key)
	defer rds.Close()
	_, err := rds.Do("DEL", key)
	return err
}

func (s *RdsSession) SetCursor(ns, client, topic string, cursor []byte) error {
	key := s.curPrefix(ns, client)
	rds := s.rdc.Get(util.W, key)
	defer rds.Close()
	_, err := rds.Do("HSET", key, topic, cursor)
	if err != nil {
		return err
	}
	return nil
}

func (s *RdsSession) Cursor(ns, client, topic string) ([]byte, error) {
	key := s.curPrefix(ns, client)
	rds := s.rdc.Get(util.R, key)
	defer rds.Close()
	cursor, err := redis.Bytes(rds.Do("HGET", key, topic))
	if err == redis.ErrNil {
		return nil, ErrInvalidRecord
	}
	return cursor, err
}

func (s *RdsSession) DeleteCursor(ns, client, topic string) error {
	key := s.curPrefix(ns, client)
	rds := s.rdc.Get(util.W, key)
	defer rds.Close()
	_, err := rds.Do("HDEL", key, topic)
	if err != nil && err != redis.ErrNil {
		return err
	}
	return nil
}

func (s *RdsSession) DropCursor(ns, client string) error {
	key := s.curPrefix(ns, client)
	rds := s.rdc.Get(util.W, key)
	defer rds.Close()
	_, err := rds.Do("DEL", key)
	return err
}

//TODO
func (s *RdsSession) ConndAddress(ns, client string) (*ClientInfo, error) {
	return nil, nil
}

// TODO
func (s *RdsSession) SetConndAddress(ns, client, address string, conneciontID int64) error {
	return nil
}

// UpdateConnAddress update client info return client old info
func (s *RdsSession) UpdateConndAddress(ns, client, address string, conneciontID int64) (*ClientInfo, error) {
	clientInfo := ClientInfo{}
	clientInfo.Address = address
	clientInfo.TimeStamp = time.Now().UnixNano()
	clientInfo.ConnectionID = conneciontID
	rawinfo, err := json.Marshal(clientInfo)
	if err != nil {
		return nil, err
	}

	key := s.infoPrefix(ns, client)
	rds := s.rdc.Get(util.W, key)
	defer rds.Close()
	data, err := redis.Bytes(rds.Do("GetSet", key, rawinfo))
	if data == nil {
		return nil, nil
	}

	if err := json.Unmarshal(data, &clientInfo); err != nil {
		return nil, err
	}
	return &clientInfo, nil
}

func (s *RdsSession) DeleteConndAddress(ns, client, address string, conneciontID int64) error {
	key := s.infoPrefix(ns, client)
	rds := s.rdc.Get(util.W, key)
	defer rds.Close()

	data, err := redis.Bytes(rds.Do("Get", key))
	if err != nil {
		if err == redis.ErrNil {
			return nil
		}
		return err
	}

	clientInfo := ClientInfo{}
	if err := json.Unmarshal(data, &clientInfo); err != nil {
		return err
	}

	if clientInfo.Address == address && clientInfo.ConnectionID == conneciontID {
		_, err = rds.Do("DEL", key)
		return nil
	}
	return nil
}

func (s *RdsSession) DropUnack(ns, client string) error {
	key := s.unackPrefix(ns, client)
	rds := s.rdc.Get(util.W, key)
	defer rds.Close()
	_, err := rds.Do("DEL", key)
	return err
}

func (s *RdsSession) Unack(ns, client string, messages []*pbMessage.Message) error {
	key := s.unackPrefix(ns, client)
	rds := s.rdc.Get(util.W, key)
	defer rds.Close()
	for _, desc := range messages {
		data, err := proto.Marshal(desc)
		if err != nil {
			return err
		}
		if _, err = rds.Do("HSET", key, desc.MessageID, data); err != nil {
			return err
		}
		if err := s.SetCursor(ns, client, desc.Topic, desc.Index); err != nil {
			return err
		}
	}
	return nil
}

func (s *RdsSession) Ack(ns, client string, mid int64) ([]byte, error) {
	key := s.unackPrefix(ns, client)
	rds := s.rdc.Get(util.W, key)
	defer rds.Close()
	data, err := redis.Bytes(rds.Do("HGET", key, mid))
	if err != nil {
		if err == redis.ErrNil {
			return nil, ErrEmpty
		}
		return nil, err
	}

	message := &pbMessage.Message{}
	if err := proto.Unmarshal(data, message); err != nil {
		log.Error("ack proto unmarshal failed", zap.String("client", client), zap.Int64("mid", mid))
	}
	if message.MessageID == mid {
		if _, err := rds.Do("HDEL", key, mid); err != nil {
			return nil, err
		}
		return message.BizID, nil
	}
	return nil, ErrEmpty
}

//TODO hscan
func (s *RdsSession) Unacks(ns, client string, pos []byte, count int64) ([]byte, []*pbMessage.Message, bool, error) {
	key := s.unackPrefix(ns, client)
	rds := s.rdc.Get(util.R, key)
	defer rds.Close()
	records, err := redis.StringMap(rds.Do("HGETALL", key))
	if err != nil {
		return nil, nil, false, err
	}
	messages := make([]*pbMessage.Message, 0, len(records))
	for _, data := range records {
		amsg := &pbMessage.Message{}
		err = proto.Unmarshal([]byte(data), amsg)
		if err != nil {
			return nil, nil, false, err
		}
		messages = append(messages, amsg)
	}
	sort.Sort(ByMessageID(messages))
	return nil, messages, true, nil
}

// SetMessageID is used to update message ID in storage
func (s *RdsSession) SetMessageID(ns, client string, messageID int64) error {
	key := s.messagePrefix(ns, client)
	rds := s.rdc.Get(util.R, key)
	defer rds.Close()
	_, err := rds.Do("SET", key, messageID)
	return err
}

// MessageID is used to query the message ID of specified client.
// if key is ErrNotFound ,return 0 nil
func (s *RdsSession) MessageID(ns, client string) (int64, error) {
	key := s.messagePrefix(ns, client)
	rds := s.rdc.Get(util.R, key)
	defer rds.Close()
	mid, err := redis.Int64(rds.Do("GET", key))
	if err == redis.ErrNil {
		return 0, nil
	}
	return mid, err
}

func (s *RdsSession) DeleteMessageID(ns, client string) error {
	key := s.messagePrefix(ns, client)
	rds := s.rdc.Get(util.W, key)
	defer rds.Close()
	_, err := rds.Do("DEL", key)
	return err
}

func (s *RdsSession) Drop(ns, client string) error {
	if err := s.DropUnack(ns, client); err != nil {
		//TODO log
	}
	if err := s.DeleteMessageID(ns, client); err != nil {
		//TODO log
	}
	if err := s.DropSub(ns, client); err != nil {
		//TODO log
	}
	if err := s.DropCursor(ns, client); err != nil {
		//TODO log
	}
	return nil
}

type ByMessageID []*pbMessage.Message

func (b ByMessageID) Len() int {
	return len(b)
}

func (b ByMessageID) Swap(i int, j int) {
	b[i], b[j] = b[j], b[i]
}

func (b ByMessageID) Less(i int, j int) bool {
	return b[i].MessageID < b[j].MessageID
}
