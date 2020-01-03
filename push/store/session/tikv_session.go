package session

import (
	"encoding/json"
	"errors"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/meitu/bifrost/commons/incr_uuid"
	"github.com/meitu/bifrost/commons/log"
	pbMessage "github.com/meitu/bifrost/push/message"
	util "github.com/meitu/bifrost/push/store/tikv_util"
	"github.com/pingcap/tidb/kv"
	"go.uber.org/zap"
)

type tiSession struct {
	txn kv.Transaction
}

func NewTiSession(txn kv.Transaction) *tiSession {
	return &tiSession{
		txn: txn,
	}
}

//func (s *tiSession) Create(name string) (int64, error) {
//	key := []byte("ss-name" + name)
//	id, err := getInt64(s.txn, key)
//	if err != nil {
//		if kv.IsErrNotFound(err) {
//			id, err := genObjID(s.txn)
//			if err != nil {
//				return 0, err
//			}
//
//			if err := setInt64(s.txn, key, id); err != nil {
//				return 0, err
//			}
//
//			return id, nil
//		}
//		return 0, err
//	}
//
//	return id, nil
//}
//
//func (s *tiSession) ID(name string) (int64, error) {
//	return getInt64(s.txn, []byte("ss-name"+name))
//}

// Subscribe set the client of subscription information
func (s *tiSession) Subscribe(ns, client, topic string, qos byte) error {
	// we can not append qos to the end of topic, or we
	// should check if the client has subscribed this topic
	// with different Qos
	sub, err := s.getSessionSubMap(ns, client)
	if err != nil {
		return err
	}
	return sub.Set(topic, []byte{qos})
}

// Unsubscribe delete the client of subscription information
func (s *tiSession) Unsubscribe(ns, client, topic string) error {
	sub, err := s.getSessionSubMap(ns, client)
	if err != nil {
		return err
	}
	if err := sub.Delete(topic); !util.IsErrNotFound(err) {
		return err
	}
	log.Warn("unsubscribe a non-exist topic", zap.String("topic", topic), zap.String("clientID", client))
	return nil
}

// Subscriptions is used query the subscription records in storage.
// If no record, it will returm empty results.
func (s *tiSession) Subscriptions(ns, client string) (map[string]byte, error) {
	sub, err := s.getSessionSubMap(ns, client)
	if err != nil {
		return nil, err
	}
	results := make(map[string]byte)

	iter, iterErr := sub.GetAll()
	if iterErr != nil {
		if iterErr == util.ErrNotFound {
			return results, nil
		}
		return results, iterErr
	}

	for topic, val := range iter {
		if len(val) != 1 {
			return nil, errors.New("invalid record")
		}
		results[topic] = val[0]
	}
	return results, nil
}

// SetCursor record where the client message was sent
func (s *tiSession) SetCursor(ns, client string, topic string, cursor []byte) error {
	cur, err := s.getSessionCursorMap(ns, client)
	if err != nil {
		return err
	}

	if _, err = cur.Get(topic); err == nil {
		return nil
	}

	if util.IsErrNotFound(err) {
		return cur.Set(topic, cursor)
	}
	return err
}

// Cursor get the topic of record where the client message was sent
func (s *tiSession) Cursor(ns, client, topic string) ([]byte, error) {
	cur, err := s.getSessionCursorMap(ns, client)
	if err != nil {
		return nil, err
	}
	val, err := cur.Get(topic)
	if err != nil {
		if util.IsErrNotFound(err) {
			return nil, ErrInvalidRecord
		}
		return nil, err
	}
	return val, nil
}

// DeleteCursor delete the topic of record where the client message was sent
func (s *tiSession) DeleteCursor(ns, client string, topic string) error {
	cur, err := s.getSessionCursorMap(ns, client)
	if err != nil {
		return err
	}
	if err := cur.Delete(topic); err != nil && !util.IsErrNotFound(err) {
		return err
	}
	log.Warn("delete no-exist cursor", zap.String("clientID", client), zap.String("topic", topic))
	return nil
}

// ConndAddress gets the client login IP address
func (s *tiSession) ConndAddress(ns, client string) (*ClientInfo, error) {
	info, err := s.getSessionInfo(ns, client)
	if err != nil {
		return nil, err
	}

	rawinfo, err := info.Get("connd-address")
	if err != nil {
		if util.IsErrNotFound(err) {
			return nil, nil
		}
		return nil, err
	}

	ctime := time.Now()
	clientInfo := ClientInfo{}
	if err = json.Unmarshal(rawinfo, &clientInfo); err != nil {
		return nil, err
	}

	if ctime.UnixNano() < clientInfo.TimeStamp {
		// info.TimeStamp-Max+Min is valid time endline. ctime should before the endline
		return &clientInfo, ErrMinimalIntervalLimit
	}
	return &clientInfo, nil
}

// SetConndAddress set the client login IP address
func (s *tiSession) SetConndAddress(ns, client, address string, conneciontID int64) error {
	clientInfo := ClientInfo{}
	info, err := s.getSessionInfo(ns, client)
	if err != nil {
		return err
	}

	clientInfo.Address = address
	clientInfo.TimeStamp = time.Now().UnixNano()
	clientInfo.ConnectionID = conneciontID
	if rawinfo, err := json.Marshal(clientInfo); err != nil {
		return err
	} else {
		return info.Set("connd-address", rawinfo)
	}
}

// UpdateConndAddress update client message return client old message
func (s *tiSession) UpdateConndAddress(ns, client, address string, connectionID int64) (*ClientInfo, error) {
	clientInfo, err := s.ConndAddress(ns, client)
	if err != nil {
		return nil, err
	}
	return clientInfo, s.SetConndAddress(ns, client, address, connectionID)
}

// DeleteConndAddress delete the client login IP address
func (s *tiSession) DeleteConndAddress(ns, client, address string, conneciontID int64) error {
	cinfo, err := s.ConndAddress(ns, client)
	if err != nil && err != ErrMinimalIntervalLimit {
		return err
	}
	if cinfo == nil {
		return nil
	}

	info, err := s.getSessionInfo(ns, client)
	if err != nil {
		return err
	}

	if cinfo.Address == address && cinfo.ConnectionID == conneciontID {
		return info.Delete("connd-address")
	}

	return nil
}

// SetMessageID is used to update message ID in storage
func (s *tiSession) SetMessageID(ns, client string, mid int64) error {
	info, err := s.getSessionInfo(ns, client)
	if err != nil {
		return err
	}
	return info.Set("msgid", util.Int64ToBytes(mid))
}

// MessageID is used to query the message ID of specified client.
func (s *tiSession) MessageID(ns, client string) (int64, error) {
	info, err := s.getSessionInfo(ns, client)
	if err != nil {
		return 0, err
	}
	val, err := info.Get("msgid")
	if err != nil {
		if util.IsErrNotFound(err) {
			return 0, nil
		}
		return 0, err
	}

	if len(val) != 8 {
		return 0, errors.New("invalid record")
	}

	return util.Int64FromBytes(val), nil
}

// DeleteMessageID is used to delete message ID in storage
func (s *tiSession) DeleteMessageID(ns, client string) error {
	info, err := s.getSessionInfo(ns, client)
	if err != nil {
		return err
	}
	return info.Delete("msgid")
}

// Drop Destroy client data in the storage
func (s *tiSession) Drop(ns, client string) error {
	//Drop Unack queue
	if err := s.DropUnack(ns, client); err != nil {
		return err
	}
	//Drop Subscribe map
	sub, err := s.getSessionSubMap(ns, client)
	if err != nil {
		return err
	}
	if err = sub.Destroy(); err != nil {
		return err
	}

	//Drop cursor map
	cur, err := s.getSessionCursorMap(ns, client)
	if err != nil {
		return err
	}
	if err = cur.Destroy(); err != nil {
		return err
	}

	//Drop info map
	info, err := s.getSessionInfo(ns, client)
	if err != nil {
		return err
	}
	if err = info.Delete("msgid"); err != nil {
		return err
	}

	return nil
}

// DropUnack drop unack queue data in the storage
func (s *tiSession) DropUnack(ns, client string) error {
	pubsub, err := s.getSessionUackPubSub(ns, client)
	if err != nil {
		return err
	}
	if err := pubsub.Drop(); err != nil && util.IsErrNotFound(err) {
		return ErrEmpty
	}
	return err
}

// Unack writes a piece of data to the unack queue
func (s *tiSession) Unack(ns, client string, descs []*pbMessage.Message) error {
	pubsub, err := s.getSessionUackPubSub(ns, client)
	if err != nil {
		return err
	}

	datas := make([]util.Data, 0, len(descs))
	for _, desc := range descs {
		data, err := proto.Marshal(desc)
		if err != nil {
			return err
		}
		datas = append(datas, data)
		// update cursor
		cur, err := s.getSessionCursorMap(ns, client)
		if err != nil {
			return err
		}

		offset := incr_uuid.OffsetFromBytes(desc.Index).Next()
		if err := cur.Set(desc.Topic, offset.Bytes()); err != nil {
			return err
		}
	}

	if _, err := pubsub.Append(datas); err != nil {
		return err
	}
	return nil
}

// Ack delete a piece of data to the unack queue
func (s *tiSession) Ack(ns, client string, mid int64) ([]byte, error) {
	pubSub, err := s.getSessionUackPubSub(ns, client)
	if err != nil {
		return nil, err
	}

	//iter one data
	message := &pbMessage.Message{}
	offsetByte := []byte{}
	handler := func(offset *incr_uuid.Offset, data util.Data) (bool, error) {
		if err = proto.Unmarshal(data, message); err != nil {
			log.Error("ack proto unmarshal failed", zap.String("client", client), zap.Int64("mid", mid))
			return true, err
		}
		offsetByte = offset.Bytes()
		return true, nil
	}

	err = pubSub.Scan(nil, handler)
	//NOTE no matter the err is ErrNotFound or ErrEmpty, we can do nothing
	if err != nil {
		return nil, err
	}

	if message.MessageID == mid {
		if err = pubSub.DeleteIndex(offsetByte); err != nil {
			return nil, err
		}
		return message.BizID, nil
	}
	return nil, ErrEmpty
}

// Unacks getthe specified amount of message in the unack queue
func (s *tiSession) Unacks(ns, client string, begin []byte, count int64) ([]byte, []*pbMessage.Message, bool, error) {
	descs := make([]*pbMessage.Message, 0, count)
	pubSub, err := s.getSessionUackPubSub(ns, client)
	if err != nil {
		return nil, descs, false, err
	}

	complete := true
	offsetByte := begin
	handler := func(offset *incr_uuid.Offset, data util.Data) (bool, error) {
		desc := &pbMessage.Message{}
		if err := proto.Unmarshal(data, desc); err != nil {
			log.Error("unacks proto unmarshal failed",
				zap.String("client", client),
				zap.Reflect("begin", begin),
				zap.Int64("count", count))
			return true, err
		}
		offsetByte = offset.Next().Bytes()
		descs = append(descs, desc)
		if int(count) == len(descs) {
			complete = false
			return true, nil
		}
		return false, nil
	}

	err = pubSub.Scan(offsetByte, handler)
	if err != nil {
		if err == util.ErrEmpty {
			return nil, descs, true, nil
		}
		return nil, descs, false, err
	}

	return offsetByte, descs, complete, nil
}

// connd-address=>ClientInfo
// msgid => int64
func (s *tiSession) getSessionInfo(ns, client string) (util.HashMap, error) {
	return util.NewHashMap(s.txn, ns, client, "INFO", false)
}

// topic=>qos
func (s *tiSession) getSessionSubMap(ns, client string) (util.HashMap, error) {
	return util.NewHashMap(s.txn, ns, client, "SUB", true)
}

// topic:cursor
func (s *tiSession) getSessionCursorMap(ns, client string) (util.HashMap, error) {
	return util.NewHashMap(s.txn, ns, client, "CUR", true)
}

func (s *tiSession) getSessionUackPubSub(ns, client string) (*util.Pubsub, error) {
	return util.NewPubsub(s.txn, ns, client, "UNACK")
}
