package util

import (
	"errors"
	"fmt"
	"strconv"

	"github.com/garyburd/redigo/redis"
	"github.com/golang/protobuf/proto"
	"github.com/meitu/bifrost/push/conf"
	pbMessage "github.com/meitu/bifrost/push/message"
)

//type Message struct {
//	Qos     byte
//	Payload []byte
//	TraceID string
//	BizID   []byte
//}

type CheckFunc func() (bool, error)

var (
	ERR_NOT_NECESSARY = errors.New("clean action is not necessary")
	NO_SERVER_START   = "queue has not started"
)

type Queue interface {
	// DedupEnqueue(clientid string, messageID uint16, topic string, message *pbMessage.Message) (uint64, error)
	// DedupRelease(clientid string, messageID uint16) error
	// Clear(clientid string) error
	Enqueue(key string, message *pbMessage.Message) ([]byte, error)
	GetN(key string, index []byte, count int64) ([]byte, []*pbMessage.Message, error)
	Index(key string) ([]byte, error)
	Delete(key string) error
	Close() error
	WatchDelete(key string, test CheckFunc) error
}

type queue struct {
	add  *redis.Script
	getn *redis.Script
	size uint64
	rds  Redic
	cfg  *conf.Redis
}

func NewQueue(config *conf.Queue) (Queue, error) {
	if config.Size == 0 {
		return nil, errors.New("invalid queue size")
	}
	// KEYS[1] = topic, ARGV[1] = data
	add := redis.NewScript(1, fmt.Sprintf(`
	local index = redis.call("incr", KEYS[1]..":index")
	redis.call("hset", KEYS[1]..":queue", index%%%d, ARGV[1])
	return index
	`, config.Size))

	// KEYS[1] = topic, KEYS[2] = index, KEYS[3] = count
	// { index, message }
	// index :1: no update
	// index other: update
	getn := redis.NewScript(3, fmt.Sprintf(`
	local abortOnOverflow = %t
	local cur = tonumber(redis.call("get", KEYS[1]..":index"))
	if cur == nil then
		return redis.error_reply("no message queue")
	end

	local index = tonumber(KEYS[2])
	local count = tonumber(KEYS[3])
	if index > cur then
		return {index, {}}
	end

	if cur - index > %d then
		if abortOnOverflow then 
		    return redis.error_reply("topic queue overflowed")
		end
		index = cur - %d + 1
	end

	local last = index + count - 1
	if last > cur then
		last  = cur
	end

	local args = {}
	local i = 1
	for pos = index, last do
		args[i] = pos%%%d
		i=i+1
	end

	return {last, redis.call("hmget", KEYS[1]..":queue", unpack(args))}
	`, config.AbortOnOverflow, config.Size, config.Size, config.Size))

	rds := New(config.Redis)
	// support hot load
	/*
		hotconf.Handle("broker.queue.redis.slowlog", config.Redis.Slowlog, func(key, val string) (interface{}, error) {
			t, err := strconv.ParseInt(val, 10, 64)
			if err != nil {
				return nil, err
			}

			rds.SetSlowlog(time.Duration(t) * time.Millisecond)
			log.Debug("hotconf update", log.Duration(key, rds.Slowlog()))
			return t, nil
		})
	*/

	return &queue{
		add:  add,
		getn: getn,
		rds:  rds,
		size: config.Size,
		cfg:  config.Redis,
	}, nil
}

func (q *queue) Enqueue(key string, message *pbMessage.Message) ([]byte, error) {
	data, err := proto.Marshal(message)
	if err != nil {
		return EncodeInt64(0), fmt.Errorf("marshal equeue message failed, topic: %s, %s", key, err)
	}

	conn := q.rds.Get(W, key)
	index, err := redis.Int64(q.add.Do(conn, key, data))
	conn.Close()
	if err != nil {
		return EncodeInt64(0), fmt.Errorf("insert message failed, topic: %s, %s", key, err)
	}

	return EncodeInt64(index + 1), nil
}

func (q *queue) GetN(topic string, cursor []byte, count int64) ([]byte, []*pbMessage.Message, error) {
	if count == 0 {
		return cursor, nil, nil
	}

	index := DecodeInt64(cursor)

	conn := q.rds.Get(R, topic)
	result, err := redis.Values(q.getn.Do(conn, topic, index, count))
	conn.Close()
	if err != nil {
		return cursor, nil, fmt.Errorf("getn failed, topic: %s, index: %d, count: %d, %s", topic, index, count, err)
	}

	var records [][]byte = nil
	var last int64
	if _, err = redis.Scan(result, &last, &records); err != nil {
		return cursor, nil, fmt.Errorf("scan failed, %s", err)
	}

	messages := make([]*pbMessage.Message, len(records))
	for i, data := range records {
		message := &pbMessage.Message{}
		err = proto.Unmarshal(data, message)
		if err != nil {
			return cursor, nil, fmt.Errorf("unmarshal enqueue message failed, topic: %s, index: %d, count: %d, %s",
				topic, index+int64(i), count, err)
		}
		messages[i] = message
		messages[i].Index = []byte(strconv.Itoa(int(index) + i))
	}

	return EncodeInt64(last + 1), messages, nil
}

func (q *queue) Index(topic string) ([]byte, error) {
	conn := q.rds.Get(R, topic)
	defer conn.Close()

	index, err := redis.Int64(conn.Do("GET", topic+":index"))
	if err == nil {
		return EncodeInt64(index + 1), nil
	} else if err == redis.ErrNil {
		return EncodeInt64(1), nil
	} else {
		return EncodeInt64(1), err
	}
}

//Delete a topic queue
func (q *queue) Delete(topic string) error {
	conn := q.rds.Get(W, topic)
	defer conn.Close()
	conn.Send("MULTI")
	conn.Send("DEL", topic+":queue")
	conn.Send("DEL", topic+":index")
	//TODO confirm if we should delete the following two records or not
	//conn.Send("DEL", topic+":version")
	_, err := conn.Do("EXEC")
	return err
}

func (q *queue) Close() error {
	return q.rds.Close()
}

func (q *queue) WatchDelete(key string, check CheckFunc) error {
	conn := q.rds.Get(W, key)
	defer conn.Close()

	_, err := conn.Do("WATCH", key+":queue")
	if err != nil {
		return fmt.Errorf("watch failed, %s", err)
	}

	var nosub bool = true
	if check != nil {
		nosub, err = check()
		if err != nil {
			return fmt.Errorf("check failed, %s", err)
		}
	}

	if nosub {
		conn.Send("MULTI")
		conn.Send("DEL", key+":queue")
		conn.Send("DEL", key+":index")
		//FIXME check if we need delete these records
		//conn.Send("DEL", topic+":version")
		results, err := conn.Do("EXEC")

		if err != nil {
			return fmt.Errorf("do multi exec failed, %s", err)
		}

		if results == nil {
			return fmt.Errorf("clean queue data failed")
		} else {
			return nil
		}
	} else {
		_, err = conn.Do("UNWATCH")
		if err != nil {
			return fmt.Errorf("unwatch failed, %s", err)
		}
		return ERR_NOT_NECESSARY
	}
}
