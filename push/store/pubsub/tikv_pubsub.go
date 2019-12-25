package pubsub

import (
	"go.uber.org/zap"

	"github.com/gogo/protobuf/proto"
	"github.com/meitu/bifrost/commons/incr_uuid"
	"github.com/meitu/bifrost/commons/log"
	pbMessage "github.com/meitu/bifrost/push/message"
	util "github.com/meitu/bifrost/push/store/tikv_util"
	"github.com/pingcap/tidb/kv"
)

type tiPubsub struct {
	txn kv.Transaction
}

func NewTiPubsub(txn kv.Transaction) *tiPubsub {
	return &tiPubsub{
		txn: txn,
	}
}

// Publish publish a message
func (t *tiPubsub) Publish(ns, topic string, message *pbMessage.Message) ([]byte, error) {
	data, err := proto.Marshal(message)
	if err != nil {
		return nil, err
	}

	pubsub, err := t.getTopicMsgPubsub(ns, topic)
	if err != nil {
		return nil, err
	}
	offs, err := pubsub.Append([]util.Data{data})
	if err != nil {
		return nil, err
	}

	return offs[len(offs)-1], nil
}

// Last the latest location of the message
func (t *tiPubsub) Last(ns, topic string) ([]byte, error) {
	// TODO bug 如果业务控制messageid 生成，这个位置需要调整重新设置
	offset := &incr_uuid.Offset{TS: int64(t.txn.StartTS())}
	return offset.Bytes(), nil
}

// Range get the specified number of messages from the starting position
func (t *tiPubsub) Range(ns, topic string, begin []byte, limit int64) ([]byte, []*pbMessage.Message, bool, error) {
	pubsub, err := t.getTopicMsgPubsub(ns, topic)
	if err != nil {
		return nil, nil, false, err
	}

	off := begin
	messages := make([]*pbMessage.Message, 0, limit)
	complete := true

	handler := func(offset *incr_uuid.Offset, data util.Data) (bool, error) {
		message := &pbMessage.Message{}
		if err := proto.Unmarshal(data, message); err != nil {
			log.Error("proto unmarshal failed", zap.String("topic", topic), zap.String("service", ns))
			return true, err
		}
		message.Index = offset.Bytes()
		messages = append(messages, message)
		off = offset.Next().Bytes()

		if int(limit) == len(messages) {
			complete = false
			return true, nil
		}
		return false, nil
	}

	if err := pubsub.Scan(begin, handler); err != nil {
		return nil, nil, false, err
	}

	return off, messages, complete, nil
}

//Drop destroy all data  in topic
func (t *tiPubsub) Drop(ns, topic string) error {
	//Drop topic msg queue
	queue, err := t.getTopicMsgPubsub(ns, topic)
	if err != nil {
		return err
	}

	if err := queue.Drop(); err != nil {
		return err
	}

	//Drop topic info hashmap
	if err := t.DeleteRetain(ns, topic); err != nil {
		return err
	}

	online, err := t.getTopicOnlineMap(ns, topic)
	if err != nil {
		return err
	}
	if err = online.Destroy(); err != nil {
		return err
	}

	offline, err := t.getTopicOfflineMap(ns, topic)
	if err != nil {
		return err
	}
	if err = offline.Destroy(); err != nil {
		return err
	}
	return nil
}

// SetRetain set a retain message
func (t *tiPubsub) SetRetain(ns, topic string, message *pbMessage.Message) error {
	data, err := proto.Marshal(message)
	if err != nil {
		return err
	}
	info, err := t.getTopicInfoMap(ns, topic)
	if err != nil {
		return err
	}
	return info.Set("retain", data)
}

// DeleteRetain delete a retain message
func (t *tiPubsub) DeleteRetain(ns, topic string) error {
	info, err := t.getTopicInfoMap(ns, topic)
	if err != nil {
		return err
	}
	if err = info.Delete("retain"); err != nil {
		if util.IsErrNotFound(err) {
			// log.Warn("delete no-exist retain message", log.String("topic", topic))
			return nil
		}
		return err
	}
	return nil
}

// Retain get a retain message
func (t *tiPubsub) Retain(ns, topic string) (*pbMessage.Message, error) {
	info, err := t.getTopicInfoMap(ns, topic)
	if err != nil {
		return nil, err
	}
	raw, err := info.Get("retain")
	if err != nil {
		if util.IsErrNotFound(err) {
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
func (t *tiPubsub) Subscribe(ns, topic, client string, online bool) error {
	onlineMap, onErr := t.getTopicOnlineMap(ns, topic)
	if onErr != nil {
		return onErr
	}
	offlineMap, offErr := t.getTopicOfflineMap(ns, topic)
	if offErr != nil {
		return offErr
	}
	if online {
		if err := offlineMap.Delete(client); err != nil {
			return err
		}
		if err := onlineMap.Set(client, []byte("null")); err != nil {
			return err
		}
	} else {
		if err := onlineMap.Delete(client); err != nil {
			return err
		}
		if err := offlineMap.Set(client, []byte("null")); err != nil {
			return err
		}

	}
	return nil
}

// Unsubscribe delete client from the online list and online list
func (t *tiPubsub) Unsubscribe(ns, topic, client string) error {
	onlineMap, onErr := t.getTopicOnlineMap(ns, topic)
	if onErr != nil {
		return onErr
	}
	offlineMap, offErr := t.getTopicOfflineMap(ns, topic)
	if offErr != nil {
		return offErr
	}
	if err := onlineMap.Delete(client); err != nil && !util.IsErrNotFound(err) {
		return err
	}
	if err := offlineMap.Delete(client); err != nil && !util.IsErrNotFound(err) {
		return err
	}
	return nil
}

// HasSubscriber has subscriber to this topic
func (t *tiPubsub) HasSubscriber(ns, topic string) (exists bool, err error) {
	onlineMap, err := t.getTopicOnlineMap(ns, topic)
	if err == nil {
		exists = onlineMap.Exists()
		if exists {
			return
		}
	}
	offlineMap, err := t.getTopicOfflineMap(ns, topic)
	if err == nil {
		exists = offlineMap.Exists()
	}
	return
}

// ListOfflineSubscribers a offline list of clients that subscribe to this topic
func (t *tiPubsub) ListOfflineSubscribers(ns, topic string) ([]string, error) {
	offlineMap, err := t.getTopicOfflineMap(ns, topic)
	if err != nil {
		return nil, err
	}
	iter, iterErr := offlineMap.GetAll()
	if iterErr != nil {
		if util.IsErrNotFound(iterErr) {
			return []string{}, nil
		}
		return nil, iterErr
	}
	offliners := make([]string, 0, len(iter))
	for client, _ := range iter {
		offliners = append(offliners, client)
	}
	return offliners, nil
}

// ListOnlineSubscribers a online list of clients that subscribe to this topic
func (t *tiPubsub) ListOnlineSubscribers(ns, topic string) ([]string, error) {
	onlineMap, err := t.getTopicOnlineMap(ns, topic)
	if err != nil {
		return nil, err
	}
	iter, iterErr := onlineMap.GetAll()
	if iterErr != nil {
		if util.IsErrNotFound(iterErr) {
			return []string{}, nil
		}
		return nil, iterErr
	}
	onliners := make([]string, 0, len(iter))
	for client, _ := range iter {
		onliners = append(onliners, client)
	}
	return onliners, nil
}

// retain=> retainmsg
func (t *tiPubsub) getTopicInfoMap(ns, topic string) (util.HashMap, error) {
	return util.NewHashMap(t.txn, ns, topic, "INF", false)
}

//topic msg queue
func (t *tiPubsub) getTopicMsgPubsub(ns, topic string) (*util.Pubsub, error) {
	return util.NewPubsub(t.txn, ns, topic, "PUBSUB")
}

// online hash map
//client_id=> "null"
func (t *tiPubsub) getTopicOnlineMap(ns, topic string) (util.HashMap, error) {
	return util.NewHashMap(t.txn, ns, topic, "ONLINE", true)
}

// offline hash map
//client_id=> "null"
func (t *tiPubsub) getTopicOfflineMap(ns, topic string) (util.HashMap, error) {
	return util.NewHashMap(t.txn, ns, topic, "OFFLINE", true)
}
