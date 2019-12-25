package route

import (
	"github.com/garyburd/redigo/redis"
	util "github.com/meitu/bifrost/push/store/redis_util"
)

type rdsRoute struct {
	rdc util.Redic
}

func NewRdsRoute(rdc util.Redic) *rdsRoute {
	return &rdsRoute{
		rdc: rdc,
	}
}

func (r *rdsRoute) getRoutePrefix(ns, topic string) string {
	return util.SysPrefix + util.DefaultDelimiter + "route" + util.DefaultDelimiter + ns + util.DefaultDelimiter + topic
}

// Add a routing information for the specified topic
// hset topic address version
func (r *rdsRoute) Add(ns, topic, address string, version uint64) error {
	key := r.getRoutePrefix(ns, topic)
	rds := r.rdc.Get(util.W, key)
	defer rds.Close()
	_, err := rds.Do("HSET", key, address, version)
	if err != nil {
		return err
	}
	return nil
}

// Remove a routing information for the specified topic
// version  ensure operation sequence , the version less than the current record version not be deleted
func (r *rdsRoute) Remove(ns, topic, address string, version uint64) error {
	key := r.getRoutePrefix(ns, topic)
	rds := r.rdc.Get(util.W, key)
	defer rds.Close()
	preVersion, err := redis.Uint64(rds.Do("HGET", key, address))
	if err != nil {
		if err == redis.ErrNil {
			return nil
		}
		return err
	}

	if preVersion < version {
		_, err = rds.Do("HDEL", key, address)
		return err
	}
	return ErrUnexpectedDelete
}

// Exist routing information for the specified topic
func (r *rdsRoute) Exist(ns, topic string) (bool, error) {
	key := r.getRoutePrefix(ns, topic)
	rds := r.rdc.Get(util.R, key)
	defer rds.Close()
	reply, err := redis.Int(rds.Do("HLEN", key))
	if err != nil || reply == 0 {
		return false, err
	}
	return true, nil
}

// Lookup all routing information for the specified topic
func (r *rdsRoute) Lookup(ns, topic string) ([]string, error) {
	key := r.getRoutePrefix(ns, topic)
	rds := r.rdc.Get(util.R, key)
	defer rds.Close()
	raws, err := redis.StringMap(rds.Do("HGETALL", key))
	if err != nil {
		return nil, err
	}
	addrs := make([]string, 0, len(raws))
	for k, _ := range raws {
		addrs = append(addrs, k)
	}
	return addrs, nil
}
