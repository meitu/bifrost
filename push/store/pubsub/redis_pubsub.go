package pubsub

import (
	"bytes"

	"github.com/garyburd/redigo/redis"
	"github.com/golang/protobuf/proto"
	pbMessage "github.com/meitu/bifrost/push/message"
	util "github.com/meitu/bifrost/push/store/redis_util"
)

type rdsPubsub struct {
	rdc   util.Redic
	queue util.Queue
}

func NewRdsPubsub(rdc util.Redic, queue util.Queue) *rdsPubsub {
	return &rdsPubsub{
		rdc:   rdc,
		queue: queue,
	}
}

// retainPrefix  joining together retain key
func (t *rdsPubsub) retainPrefix(ns, topic string) string {
	return util.SysPrefix + util.DefaultDelimiter + "retain" + util.DefaultDelimiter + ns + util.DefaultDelimiter + topic
}

func (t *rdsPubsub) subPrefix(ns, topic string) string {
	return util.SysPrefix + util.DefaultDelimiter + "tsub" + util.DefaultDelimiter + ns + util.DefaultDelimiter + topic
}

func (t *rdsPubsub) onlinePrefix(ns, topic string) string {
	return util.SysPrefix + util.DefaultDelimiter + "online" + util.DefaultDelimiter + ns + util.DefaultDelimiter + topic
}

func (t *rdsPubsub) offlinePrefix(ns, topic string) string {
	return util.SysPrefix + util.DefaultDelimiter + "offline" + util.DefaultDelimiter + ns + util.DefaultDelimiter + topic
}

// Publish publish a message
func (t *rdsPubsub) Publish(ns, topic string, message *pbMessage.Message) ([]byte, error) {
	index, err := t.queue.Enqueue(t.subPrefix(ns, topic), message)
	return index, err
}

// Last the latest location of the message
func (t *rdsPubsub) Last(ns, topic string) ([]byte, error) {
	index, err := t.queue.Index(t.subPrefix(ns, topic))
	if err != nil {
		return nil, err
	}
	return index, nil
}

// Range get the specified number of messages from the starting position
func (t *rdsPubsub) Range(ns, topic string, pos []byte, count int64) ([]byte, []*pbMessage.Message, bool, error) {
	key := t.subPrefix(ns, topic)
	// get n messages
	index, messages, err := t.queue.GetN(key, pos, count)
	if err != nil {
		return nil, nil, false, err
	}
	if len(messages) == 0 {
		return pos, messages, true, nil
	}

	cursor, err := t.queue.Index(key)
	if err != nil {
		return nil, nil, false, err
	}
	return index, messages, bytes.Compare(index, cursor) != -1, nil
}

//Drop destroy all data  in topic
func (t *rdsPubsub) Drop(ns, topic string) error {
	if err := t.DeleteRetain(ns, topic); err != nil {
		//TODO log
	}
	if err := t.DeleteAllSubscribers(ns, topic); err != nil {
		//TDO log

	}
	key := t.subPrefix(ns, topic)
	return t.queue.WatchDelete(key, func() (bool, error) { return true, nil })
}

// SetRetain set a retain message
func (t *rdsPubsub) SetRetain(ns, topic string, message *pbMessage.Message) error {
	data, err := proto.Marshal(message)
	if err != nil {
		return err
	}

	key := t.retainPrefix(ns, topic)
	rds := t.rdc.Get(util.W, key)
	defer rds.Close()
	_, err = rds.Do("SET", key, data)
	return err
}

// DeleteRetain delete a retain message
func (t *rdsPubsub) DeleteRetain(ns, topic string) error {
	key := t.retainPrefix(ns, topic)
	rds := t.rdc.Get(util.W, key)
	defer rds.Close()
	_, err := rds.Do("DEL", key)
	return err
}

// SetRetain set a retain message
func (t *rdsPubsub) Retain(ns, topic string) (*pbMessage.Message, error) {
	key := t.retainPrefix(ns, topic)
	rds := t.rdc.Get(util.R, key)
	defer rds.Close()
	raw, err := redis.Bytes(rds.Do("GET", key))
	if err != nil {
		if err == redis.ErrNil {
			return nil, nil
		}
		return nil, err
	}
	message := &pbMessage.Message{}
	if err := proto.Unmarshal(raw, message); err != nil {
		return nil, err
	}
	return message, nil
}

// Subscribe update the online list and online list
func (t *rdsPubsub) Subscribe(ns, topic, client string, online bool) error {
	if online {
		key := t.onlinePrefix(ns, topic)
		rds := t.rdc.Get(util.W, key)
		defer rds.Close()
		_, err := rds.Do("HSET", t.onlinePrefix(ns, topic), client, []byte("null"))
		return err
	}

	//TODO delete offline
	key := t.offlinePrefix(ns, topic)
	rds := t.rdc.Get(util.W, key)
	defer rds.Close()
	_, err := rds.Do("HSET", key, client, []byte("null"))
	return err
}

// Unsubscribe delete client from the online list and online list
func (t *rdsPubsub) Unsubscribe(ns, topic, client string) error {
	key := t.offlinePrefix(ns, topic)
	rds := t.rdc.Get(util.W, key)
	defer rds.Close()
	_, err := rds.Do("HDEL", key, client)
	if err != nil {
		return err
	}

	key = t.onlinePrefix(ns, topic)
	rds = t.rdc.Get(util.W, key)
	defer rds.Close()
	_, err = rds.Do("HDEL", key, client)

	checkFunc := func() (bool, error) {
		sub, err := t.HasSubscriber(ns, topic)
		return !sub, err
	}
	return t.queue.WatchDelete(t.subPrefix(ns, topic), checkFunc)
}

// HasSubscriber has subscriber to this topic
func (t *rdsPubsub) HasSubscriber(ns, topic string) (bool, error) {
	key := t.offlinePrefix(ns, topic)
	rds := t.rdc.Get(util.R, key)
	defer rds.Close()
	llen, err := redis.Int(rds.Do("HLEN", key))
	if err == nil && llen != 0 {
		return true, nil
	}

	key = t.onlinePrefix(ns, topic)
	rds = t.rdc.Get(util.R, key)
	defer rds.Close()
	llen, err = redis.Int(rds.Do("HLEN", key))
	if err == nil && llen != 0 {
		return true, nil
	}
	return false, nil
}

// ListOfflineSubscribers a offline list of clients that subscribe to this topic
func (t *rdsPubsub) ListOfflineSubscribers(ns, topic string) ([]string, error) {
	key := t.offlinePrefix(ns, topic)
	rds := t.rdc.Get(util.R, key)
	defer rds.Close()
	raws, err := redis.StringMap(rds.Do("HGETALL", key))
	if err != nil {
		return nil, err
	}
	offline := make([]string, 0, len(raws))
	for k, _ := range raws {
		offline = append(offline, k)
	}
	return offline, nil
}

// ListOnlineSubscribers a online list of clients that subscribe to this topic
func (t *rdsPubsub) ListOnlineSubscribers(ns, topic string) ([]string, error) {
	key := t.onlinePrefix(ns, topic)
	rds := t.rdc.Get(util.R, key)
	defer rds.Close()
	raws, err := redis.StringMap(rds.Do("HGETALL", key))
	if err != nil {
		return nil, err
	}
	online := make([]string, 0, len(raws))
	for k, _ := range raws {
		online = append(online, k)
	}
	return online, nil
}

// Destory subscribers in topic
func (t *rdsPubsub) DeleteAllSubscribers(ns, topic string) error {
	key := t.offlinePrefix(ns, topic)
	rds := t.rdc.Get(util.W, key)
	defer rds.Close()
	_, err := rds.Do("DEl", key)
	if err != nil {
		return err
	}

	key = t.onlinePrefix(ns, topic)
	rds = t.rdc.Get(util.W, key)
	defer rds.Close()
	_, err = rds.Do("DEL", key)
	if err != nil {
		return err
	}
	return nil
}
