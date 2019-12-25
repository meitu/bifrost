package util

import (
	"hash/crc32"
	"time"

	"github.com/garyburd/redigo/redis"
	"github.com/meitu/bifrost/push/conf"
)

//Op is the operation type of redis, can be W(write) or R(read)
type Op string

//Valid redic.Op
const (
	R = Op("r")
	W = Op("w")
)

//Redic is a master + slave redis cluster
type Redic interface {
	Get(op Op, key string) redis.Conn
	LoadScripts(scripts ...*redis.Script) error
	Close() error
}

type master struct {
	cluster []string
	pools   []*redis.Pool
}

type slave master

type redic struct {
	m   *master
	ss  []*slave
	idx int //round-robin between multiple slave cluster
}

//New a Redis cluster
func New(c *conf.Redis) Redic {
	rds := &redic{}
	m := &master{}
	m.cluster = c.Masters
	m.pools = make([]*redis.Pool, len(m.cluster))

	create := redisPoolBuilder(c)

	for i := range m.cluster {
		m.pools[i] = create(m.cluster[i])
	}
	rds.m = m

	ss := make([]*slave, len(c.Slaves))
	for i := range c.Slaves {
		s := &slave{}
		s.cluster = c.Slaves[i]
		s.pools = make([]*redis.Pool, len(s.cluster))
		for j := range s.cluster {
			s.pools[j] = create(s.cluster[j])
		}
		ss[i] = s
	}

	rds.ss = ss
	return rds
}

//LoadScripts loads lua scripts to all master nodes
func (r *redic) LoadScripts(scripts ...*redis.Script) error {
	for _, script := range scripts {
		for _, p := range r.m.pools {
			rds := p.Get()
			err := script.Load(rds)
			rds.Close()

			if err != nil {
				return err
			}
		}
	}
	return nil
}

//Get a redis connection by key and op(can be R or W)
func (r *redic) Get(op Op, key string) redis.Conn {
	//keep the same as gocommons sharding
	hash := (crc32.ChecksumIEEE([]byte(key)) >> 16) & 0x7fff
	idx := hash % uint32(len(r.m.cluster))

	//When there is no slave, read from master
	if len(r.ss) == 0 {
		op = W
	}

	switch op {
	case R:
		i := r.idx % len(r.ss)
		r.idx++
		return r.ss[i].pools[idx].Get()
	case W:
		return r.m.pools[idx].Get()
	}
	return nil
}

//Close all redis connections
func (r *redic) Close() error {
	for _, p := range r.m.pools {
		p.Close()
	}
	for _, s := range r.ss {
		for _, p := range s.pools {
			p.Close()
		}
	}
	return nil
}

func redisPoolBuilder(c *conf.Redis) func(string) *redis.Pool {
	return func(address string) *redis.Pool {
		to := time.Duration(time.Duration(c.Timeout) * time.Millisecond)
		return &redis.Pool{
			MaxActive:   c.MaxActive,
			MaxIdle:     c.MaxIdle,
			IdleTimeout: time.Duration(c.IdleTimeout) * time.Millisecond,
			Dial: func() (redis.Conn, error) {
				c, err := redis.DialURL(address, redis.DialConnectTimeout(to),
					redis.DialReadTimeout(to), redis.DialWriteTimeout(to),
					redis.DialPassword(c.Auth))
				if err != nil {
					return nil, err
				}
				return c, err
			},
			TestOnBorrow: func(c redis.Conn, t time.Time) error {
				_, err := c.Do("PING")
				return err
			},
			Wait: c.Wait,
		}
	}
}
