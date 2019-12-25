package store

import (
	"context"

	"github.com/meitu/bifrost/commons/log"
	pbMessage "github.com/meitu/bifrost/push/message"
	"github.com/meitu/bifrost/push/store/session"
	"go.uber.org/zap"
)

// ResumeSession is used to query the session in storage.
// It can query the following session records:
// 1. the subscriptions records.
// 2. the cursor of every topic in the subscription records.
// 3. the lastest index of every topic in subscription records.
// 4. the messageID.
func (s *kvStore) ResumeSession(ctx context.Context, namespace, clientID string) (int64, []*Record, error) {
	var (
		records   []*Record
		messageID int64
	)
	job := func(ctx context.Context) error {
		txn, err := s.cli.Start()
		if err != nil {
			log.Error("start new transaction failed", zap.String("clientID", clientID), zap.Error(err))
			return err
		}
		defer rollback(txn, err)

		// query the messageID in session record
		// return any error include ErrNotFound.
		messageID, err = txn.SessionOps().MessageID(namespace, clientID)
		if err != nil {
			log.Error("query message ID failed", zap.String("clientID", clientID), zap.Error(err))
			return err
		}

		// query the subscription records
		subs, err := txn.SessionOps().Subscriptions(namespace, clientID)
		if err != nil {
			log.Error("query subscription failed", zap.String("clientID", clientID), zap.Error(err))
			return err
		}

		for topic, qos := range subs {
			// update subscriptions
			if err := txn.TopicOps().Subscribe(namespace, topic, clientID, true); err != nil {
				log.Error("update the status of topic subscription failed", zap.String("clientID", clientID), zap.String("topic", topic), zap.Error(err))
				return err
			}

			// query the cursor of every topic
			cursor, err := txn.SessionOps().Cursor(namespace, clientID, topic)
			if err != nil {
				log.Error("query cursor failed", zap.String("clientID", clientID), zap.String("topic", topic), zap.Error(err))
				return err
			}

			// query the lastest index of every topic
			index, err := txn.TopicOps().Last(namespace, topic)
			if err != nil {
				log.Error("query topic last pos failed", zap.String("clientID", clientID), zap.String("topic", topic), zap.Error(err))
				return err
			}
			record := &Record{
				Topic:        topic,
				Qos:          qos,
				CurrentIndex: cursor,
				LastestIndex: index,
			}
			records = append(records, record)
		}

		if err := txn.Commit(ctx); err != nil {
			log.Error("commit failed", zap.String("clientID", clientID), zap.Error(err))
			return err
		}
		return nil
	}

	return messageID, records, Retryable(job, 3, ctx)
}

// rollback the transaction
func rollback(txn Transaction, err error) {
	if err != nil {
		if err := txn.Rollback(); err != nil {
			log.Error("rollback failed", zap.Error(err))
		}
	}
}

// DropSession is used to clean the session records in storage.
// It can clean the following session records:
// 1. the subscriptions records.
// 2. the cursor of every topic in the subscription records.
// 3. the lastest index of every topic in subscription records.
// 4. the messageID.
// 5. the unack message queue.
func (s *kvStore) DropSession(ctx context.Context, namespace, clientID string) error {
	job := func(ctx context.Context) error {
		txn, err := s.cli.Start()
		if err != nil {
			log.Error("start new transaction failed", zap.String("clientID", clientID), zap.Error(err))
			return err
		}
		defer rollback(txn, err)

		if err = txn.SessionOps().Drop(namespace, clientID); err != nil {
			if err == session.ErrEmpty {
				log.Debug("drop session failed", zap.String("clientID", clientID))
				return nil
			} else {
				log.Error("drop session failed", zap.String("clientID", clientID), zap.Error(err))
				return err
			}
		}

		if err = txn.Commit(ctx); err != nil {
			log.Error("drop session failed", zap.String("clientID", clientID), zap.Error(err))
			return err
		}
		return nil
	}
	return Retryable(job, 3, ctx)
}

// Subscribe is used to update subscriptions session.
// If the cleanSession = true, it will update the subscriptions session in storage.
// If the topic doesn't exist, it will create the topic message queue.
func (s *kvStore) Subscribe(ctx context.Context, namespace, clientID string, topics []string, qoss []byte, cleanSession bool) ([][]byte, []*pbMessage.Message, error) {
	var (
		indexs  [][]byte
		retains []*pbMessage.Message
	)
	job := func(ctx context.Context) error {
		txn, err := s.cli.Start()
		if err != nil {
			log.Error("start new transaction failed", zap.String("clientID", clientID), zap.Error(err))
			return err
		}
		defer rollback(txn, err)

		for i, topic := range topics {
			// query lastest index
			index, err := txn.TopicOps().Last(namespace, topic)
			if err != nil {
				log.Error("query topic last failed", zap.String("clientID", clientID), zap.String("topic", topic), zap.Error(err))
				return err
			}

			// query retain message
			retain, err := txn.TopicOps().Retain(namespace, topic)
			if err != nil {
				log.Error("query retain message failed", zap.String("clientID", clientID), zap.String("topic", topic), zap.Error(err))
				return err
			}
			if retain != nil {
				retain.Topic = topic
			}

			// update session if cleanSession = false
			if !cleanSession {
				// update subscriptions
				/*
					if err := txn.TopicOps().Subscribe(namespace, topic, clientID, true); err != nil {
						log.Error("update topic subscrition status failed", zap.String("clientID", clientID), zap.String("topic", topic), zap.Error(err))
						return err
					}
				*/

				// update subscriptions
				err = txn.SessionOps().Subscribe(namespace, clientID, topic, qoss[i])
				if err != nil {
					log.Error("session subscribe failed", zap.String("clientID", clientID), zap.String("topic", topic), zap.Error(err))
					return err
				}
				// update read cursor
				err = txn.SessionOps().SetCursor(namespace, clientID, topic, index)
				if err != nil {
					log.Error("session set cursor failed", zap.String("clientID", clientID), zap.String("topic", topic), zap.Reflect("index", index), zap.Error(err))
					return err
				}
			}
			//NOTE the next element of this queue is the start pos
			indexs = append(indexs, index)
			retains = append(retains, retain)
		}
		if err = txn.Commit(ctx); err != nil {
			log.Error("commit failed", zap.Error(err))
			return err
		}
		return nil
	}

	return indexs, retains, Retryable(job, 3, ctx)
}

// Unsubscribe is used to remove the specified subscription records in storage
func (s *kvStore) Unsubscribe(ctx context.Context, namespace, clientID string, topics []string, cleanSession bool) error {
	job := func(ctx context.Context) error {
		txn, err := s.cli.Start()
		if err != nil {
			log.Error("start new transaction failed", zap.String("clientID", clientID), zap.Error(err))
			return err
		}
		defer rollback(txn, err)

		if cleanSession {
			for _, topic := range topics {
				err = txn.SessionOps().Unsubscribe(namespace, clientID, topic)
				if err != nil {
					log.Error("session unsubscribe failed", zap.String("clientID", clientID), zap.String("topic", topic), zap.Error(err))
					return err
				}

				err = txn.SessionOps().DeleteCursor(namespace, clientID, topic)
				if err != nil {
					log.Error("session delete cursor failed", zap.String("clientID", clientID), zap.String("topic", topic), zap.Error(err))
					return err
				}

				err = txn.TopicOps().Unsubscribe(namespace, topic, clientID)
				if err != nil {
					log.Error("update topic subscriptions failed", zap.String("clientID", clientID), zap.String("topic", topic), zap.Error(err))
					return err
				}

				//TODO drop the topic message queue if no subscriber
			}
		} else {
			for _, topic := range topics {
				if err := txn.TopicOps().Subscribe(namespace, topic, clientID, false); err != nil {
					log.Error("update topic subscrition status failed", zap.String("clientID", clientID), zap.String("topic", topic), zap.Error(err))
					return err
				}
			}
		}

		if err = txn.Commit(ctx); err != nil {
			log.Error("commit failed", zap.Error(err))
			return err
		}
		return nil
	}

	return Retryable(job, 3, ctx)
}

func (s *kvStore) AddRoute(ctx context.Context, namespace string, topic string, conndAddress string, version uint64) error {
	job := func(ctx context.Context) error {
		txn, err := s.cli.Start()
		if err != nil {
			log.Error("start new transaction failed", zap.String("conndAddress", conndAddress), zap.Error(err))
			return err
		}
		defer rollback(txn, err)

		if err := txn.RouteOps().Add(namespace, topic, conndAddress, version); err != nil {
			log.Error("add route failed", zap.String("topic", topic), zap.String("conndAddress", conndAddress), zap.Error(err))
			return err
		}

		if err = txn.Commit(ctx); err != nil {
			log.Error("commit failed", zap.String("conndAddress", conndAddress), zap.Error(err))
			return err
		}
		return nil
	}
	return Retryable(job, 3, ctx)
}

func (s *kvStore) RemoveRoute(ctx context.Context, namespace string, topic string, conndAddress string, version uint64) error {
	job := func(ctx context.Context) error {
		txn, err := s.cli.Start()
		if err != nil {
			log.Error("start new transaction failed", zap.String("topic", topic), zap.String("conndAddress", conndAddress), zap.Error(err))
			return err
		}

		if err := txn.RouteOps().Remove(namespace, topic, conndAddress, version); err != nil {
			log.Error("remove route failed", zap.String("topic", topic), zap.String("conndAddress", conndAddress), zap.Error(err))
			return err
		}
		//FIXME drop topic queue if no route

		if err = txn.Commit(ctx); err != nil {
			log.Error("commit failed", zap.String("topic", topic), zap.String("conndAddress", conndAddress), zap.Error(err))
			return err
		}
		return nil
	}

	return Retryable(job, 3, ctx)
}

func (s *kvStore) Publish(ctx context.Context, namespace string, msg *pbMessage.Message, isRetain, sessionRecovery bool) ([]string, []string, []byte, error) {
	var (
		connds []string
		subers []string
		index  []byte
	)
	job := func(ctx context.Context) error {
		txn, err := s.cli.Start()
		if err != nil {
			log.Error("start new transaction failed", zap.Reflect("message", msg), zap.Bool("isRetain", isRetain), zap.Error(err))
			return err
		}

		if isRetain {
			if len(msg.Payload) == 0 {
				err = txn.TopicOps().DeleteRetain(namespace, msg.Topic)
				if err != nil {
					log.Error("delete retain message failed", zap.Reflect("message", msg), zap.Bool("isRetain", isRetain), zap.Error(err))
					return err
				}
				err = txn.Commit(ctx)
				if err != nil {
					log.Error("commit failed", zap.Reflect("message", msg), zap.Bool("isRetain", isRetain), zap.Error(err))
					return err
				}
				return nil
			} else {
				err = txn.TopicOps().SetRetain(namespace, msg.Topic, msg)
				if err != nil {
					log.Error("set retain message failed", zap.Reflect("message", msg), zap.Bool("isRetain", isRetain), zap.Error(err))
					return err
				}
			}
		}

		//TODO maybe not need operation
		if sessionRecovery {
			subers, err = txn.TopicOps().ListOfflineSubscribers(namespace, msg.Topic)
			if err != nil {
				log.Error("query subscribers failed", zap.Reflect("message", msg), zap.Bool("isRetain", isRetain), zap.Error(err))
				return err
			}
		}

		connds, err = txn.RouteOps().Lookup(namespace, msg.Topic)
		if err != nil {
			log.Error("lookup failed", zap.Reflect("message", msg), zap.Bool("isRetain", isRetain), zap.Error(err))
			return err
		}

		// If no subscriber, that is no client subscribe this topic with cleansession=false
		// If no route, that is no online client
		// We should check both the subscription and the route.
		if len(subers) != 0 || len(connds) != 0 {
			index, err = txn.TopicOps().Publish(namespace, msg.Topic, msg)
			if err != nil {
				log.Error("publish failed", zap.Reflect("message", msg), zap.Bool("isRetain", isRetain), zap.Error(err))
				return err
			}
		}

		if err = txn.Commit(ctx); err != nil {
			log.Error("commit failed", zap.Reflect("message", msg), zap.Bool("isRetain", isRetain), zap.Error(err))
			return err
		}
		return nil
	}

	return connds, subers, index, Retryable(job, 3, ctx)
}

func (s *kvStore) DropTopicIfNoRoute(ctx context.Context, namespace, topic string) error {
	job := func(ctx context.Context) error {
		txn, err := s.cli.Start()
		if err != nil {
			log.Error("start new transaction failed", zap.String("topic", topic), zap.Error(err))
			return err
		}

		exist, err := txn.RouteOps().Exist(namespace, topic)
		if err != nil {
			log.Error("check route exist failed", zap.String("topic", topic), zap.Error(err))
			return err
		}

		if !exist {
			err = txn.TopicOps().Drop(namespace, topic)
			if err != nil {
				log.Error("drop topic failed", zap.String("topic", topic), zap.Error(err))
				return err
			}
		}

		if err = txn.Commit(ctx); err != nil {
			log.Error("drop topic failed", zap.String("topic", topic), zap.Error(err))
			return err
		}
		return nil
	}
	return Retryable(job, 3, ctx)
}

func (s *kvStore) Pull(ctx context.Context, namespace, traceID, topic string, offset []byte, limit int64) ([]byte, []*pbMessage.Message, bool, error) {
	var (
		off      []byte
		messages []*pbMessage.Message
		complete bool
	)
	job := func(ctx context.Context) error {
		txn, err := s.cli.Start()
		if err != nil {
			log.Error("start new transaction failed", zap.String("traceID", traceID), zap.String("topic", topic), zap.Reflect("offset", offset), zap.Int64("limit", limit), zap.Error(err))
			return err
		}

		off, messages, complete, err = txn.TopicOps().Range(namespace, topic, offset, limit)
		if err != nil {
			log.Error("range topic message failed", zap.String("traceID", traceID), zap.String("topic", topic), zap.Reflect("offset", offset), zap.Int64("limit", limit), zap.Error(err))
			return err
		}

		if err = txn.Commit(ctx); err != nil {
			log.Error("commit failed", zap.String("traceID", traceID), zap.String("topic", topic), zap.Reflect("offset", offset), zap.Int64("limit", limit), zap.Error(err))
			return err
		}
		return nil
	}
	return off, messages, complete, Retryable(job, 3, ctx)
}

func (s *kvStore) PutUnackMessage(ctx context.Context, namespace, clientID string, cleanSession bool, descs []*pbMessage.Message) error {
	job := func(ctx context.Context) error {
		txn, err := s.cli.Start()
		if err != nil {
			log.Error("start new transaction failed", zap.String("clientID", clientID), zap.Error(err))
			return err
		}
		if err := txn.SessionOps().Unack(namespace, clientID, descs); err != nil {
			return err
		}

		//update messageid
		if err = txn.SessionOps().SetMessageID(namespace, clientID, descs[len(descs)-1].MessageID); err != nil {
			return err
		}

		return txn.Commit(ctx)
	}
	return Retryable(job, 3, ctx)
}

func (s *kvStore) DelUnackMessage(ctx context.Context, namespace, clientID string, messageID int64) ([]byte, error) {
	var (
		bizID []byte
	)
	job := func(ctx context.Context) error {
		txn, err := s.cli.Start()
		if err != nil {
			log.Error("start new transaction failed", zap.String("clientID", clientID), zap.Error(err))
			return err
		}

		bizID, err = txn.SessionOps().Ack(namespace, clientID, messageID)
		if err != nil {
			return err
		}
		return txn.Commit(ctx)
	}
	return bizID, Retryable(job, 3, ctx)
}

func (s *kvStore) RangeUnackMessages(ctx context.Context, namespace, clientID string, offset []byte, limit int64) ([]byte, []*pbMessage.Message, bool, error) {
	var (
		begin    []byte
		messages []*pbMessage.Message
		complete bool
	)
	job := func(ctx context.Context) error {
		txn, err := s.cli.Start()
		if err != nil {
			log.Error("start new transaction failed", zap.String("clientID", clientID), zap.Error(err))
			return err
		}

		begin, messages, complete, err = txn.SessionOps().Unacks(namespace, clientID, offset, limit)
		if err != nil {
			if err == session.ErrEmpty {
				complete = true
				err = nil
			}
			return err
		}
		return txn.Commit(ctx)
	}
	return begin, messages, complete, Retryable(job, 3, ctx)
}

func (s *kvStore) KickIfNeeded(ctx context.Context, namespace, clientID, conndAddress string, connectionID int64, hook KickHook) error {
	job := func(ctx context.Context) error {
		txn, err := s.cli.Start()
		if err != nil {
			log.Error("start new transaction failed", zap.String("clientID", clientID), zap.Error(err))
			return err
		}
		defer rollback(txn, err)

		// check the old connd address and connection ID
		cinfo, err := txn.SessionOps().UpdateConndAddress(namespace, clientID, conndAddress, connectionID)
		if err != nil {
			// return if any error occur except not found
			log.Error("query connd address failed", zap.String("clientID", clientID), zap.String("conndAddress", conndAddress), zap.Error(err))
			return err
		}
		// kick the old connection if it's a different client
		if hook != nil && cinfo != nil && (cinfo.Address != conndAddress || cinfo.ConnectionID != connectionID) {
			hookErr := hook(ctx, clientID, namespace, cinfo.Address, cinfo.ConnectionID)
			if hookErr != nil {
				log.Error("notify client disconnect failed", zap.String("clientID", clientID), zap.String("conndAddress", conndAddress), zap.Int64("connectionID", connectionID), zap.Error(hookErr))
			}
		}

		if err := txn.Commit(ctx); err != nil {
			log.Error("commit failed", zap.String("clientID", clientID), zap.String("conndAddress", conndAddress), zap.Int64("connectionID", connectionID), zap.Error(err))
			return err
		}
		return nil
	}
	return Retryable(job, 3, ctx)
}

func (s *kvStore) Offline(ctx context.Context, namespace, clientID, conndAddress string, connectionID int64, cleanSession, kick bool) error {
	job := func(ctx context.Context) error {
		txn, err := s.cli.Start()
		if err != nil {
			log.Error("start new transaction failed", zap.String("clientID", clientID), zap.Error(err))
			return err
		}
		defer rollback(txn, err)

		if !kick {
			err = txn.SessionOps().DeleteConndAddress(namespace, clientID, conndAddress, connectionID)
			if err != nil {
				log.Error("delete connd address failed", zap.String("clientID", clientID), zap.String("conndAddress", conndAddress), zap.Int64("connectionID", connectionID), zap.Error(err))
				return err
			}
		}

		if cleanSession {
			if err := txn.SessionOps().DropUnack(namespace, clientID); err != nil {
				return err
			}
		}

		if err = txn.Commit(ctx); err != nil {
			log.Error("commit failed", zap.String("clientID", clientID), zap.String("conndAddress", conndAddress), zap.Int64("connectionID", connectionID), zap.Error(err))
			return err
		}
		return nil
	}
	return Retryable(job, 3, ctx)
}
