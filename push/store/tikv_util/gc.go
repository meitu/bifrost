package util

import (
	"bytes"
	"context"
	"encoding/binary"
	"time"

	"github.com/meitu/bifrost/commons/log"
	"github.com/meitu/bifrost/push/conf"
	"github.com/meitu/bifrost/push/status"
	"github.com/pingcap/tidb/kv"
	"github.com/twinj/uuid"
	"go.uber.org/zap"
)

var (
	sysNamespace = []byte("$sys")
	sysGCLeader  = []byte("GCL:GCLeader")
	gInstanceID  = uuid.NewV4()
)

func toGCKey(key []byte) []byte {
	b := sysKey(nil)
	b = append(b, 'G', 'C', defaultDelimiter)
	b = append(b, key...)
	return b
}

func sysKey(key []byte) []byte {
	b := []byte{}
	b = append(b, prefixByte, defaultDelimiter)
	b = append(b, sysNamespace...)
	b = append(b, defaultDelimiter)
	b = append(b, key...)
	return b
}

// {sys.ns}:{sys.id}:{GC}:{prefix}
// prefix: {user.ns}:{user.id}:{M/D}:{user.objectID}
func gc(txn kv.Transaction, prefix []byte) error {
	if env := log.Check(zap.DebugLevel, "[GC] remove prefix"); env != nil {
		env.Write(zap.Reflect("prefix", prefix))
	}
	status.M.GCKeysCounterVec.WithLabelValues("add").Inc()
	key := toGCKey(prefix)
	return txn.Set(key, []byte{0})
}

func gcDeleteRange(txn kv.Transaction, prefix []byte, limit int64) (int64, error) {
	var count int64
	itr, err := txn.Iter(prefix, kv.Key(prefix).PrefixNext())
	if err != nil {
		return count, err
	}
	defer itr.Close()

	for itr.Valid() && itr.Key().HasPrefix(prefix) {
		if err := txn.Delete(itr.Key()); err != nil {
			return count, err
		}

		count++
		if limit > 0 && count >= limit {
			return count, nil
		}
		if err := itr.Next(); err != nil {
			return count, err
		}
	}
	return count, nil
}

func doGC(store kv.Storage, limit int64) error {
	txn, err := store.Begin()
	if err != nil {
		log.Error("[GC] transection begin failed", zap.Error(err))
		return err
	}
	txn.SetOption(kv.KeyOnly, true)

	gcPrefix := toGCKey(nil)
	itr, err := txn.Iter(gcPrefix, kv.Key(gcPrefix).PrefixNext())
	if err != nil {
		return err
	}
	defer itr.Close()
	if !itr.Valid() || !itr.Key().HasPrefix(gcPrefix) {
		if env := log.Check(zap.DebugLevel, "[GC] not need gc data"); env != nil {
			env.Write(zap.Reflect("gcPrefix", gcPrefix), zap.Int64("BatchLimit", limit))
		}
		return nil
	}
	var (
		count        int64
		gcKeyCount   int64 = 0
		dataKeyCount int64 = 0
	)
	for itr.Valid() && itr.Key().HasPrefix(gcPrefix) {
		dataPrefix := itr.Key()[len(gcPrefix):]
		if env := log.Check(zap.DebugLevel, "[GC] start to delete prefix"); env != nil {
			env.Write(zap.Reflect("dataPrefix", dataPrefix), zap.Int64("BatchLimit", limit))
		}

		count = 0
		if count, err = gcDeleteRange(txn, dataPrefix, limit); err != nil {
			return err
		}

		//check and delete gc key
		if limit > 0 && count < limit || limit <= 0 && count > 0 {
			if env := log.Check(zap.DebugLevel, "[GC] delete dataPrefix succeed"); env != nil {
				env.Write(zap.Reflect("dataPrefix", dataPrefix), zap.Int64("BatchLimit", limit), zap.Int64("count", count))
			}

			if err := txn.Delete(itr.Key()); err != nil {
				txn.Rollback()
				return err
			}
			gcKeyCount++
		}

		dataKeyCount += count
		if limit-(gcKeyCount+dataKeyCount) <= 0 {
			break
		}
		if err := itr.Next(); err != nil {
			log.Error("[GC] iter dataPrefix err", zap.Reflect("dataPrefix", dataPrefix), zap.Error(err))
			break
		}
	}

	if err := txn.Commit(context.Background()); err != nil {
		txn.Rollback()
		return err
	}
	if env := log.Check(zap.DebugLevel, "[GC] txn commit success"); env != nil {
		env.Write(zap.Int64("gcKey", gcKeyCount), zap.Int64("dataKey", dataKeyCount))
	}

	status.M.GCKeysCounterVec.WithLabelValues("data_delete").Add(float64(dataKeyCount))
	status.M.GCKeysCounterVec.WithLabelValues("gc_delete").Add(float64(gcKeyCount))
	return nil
}

func flushLease(txn kv.Transaction, key []byte, id []byte, lease time.Duration) error {
	databytes := make([]byte, 24)
	copy(databytes, id)
	ts := uint64((time.Now().Add(lease).Unix()))
	binary.BigEndian.PutUint64(databytes[16:], ts)

	if err := txn.Set(key, databytes); err != nil {
		return err
	}
	return nil
}

func checkLeader(txn kv.Transaction, key []byte, id []byte, lease time.Duration) (bool, error) {
	val, err := txn.Get(key)
	if err != nil {
		if !IsErrNotFound(err) {
			log.Error("[GC]  query leader message faild", zap.Reflect("key", key), zap.Reflect("id", id), zap.Duration("lease", lease), zap.Error(err))
			return false, err
		}

		if err := flushLease(txn, key, id, lease); err != nil {
			log.Error("[GC]  create lease failed", zap.Reflect("key", key), zap.Reflect("id", id), zap.Duration("lease", lease), zap.Error(err))
			return false, err
		}

		return true, nil
	}

	curID := val[0:16]
	ts := int64(binary.BigEndian.Uint64(val[16:]))

	if time.Now().Unix() > ts {
		log.Error("[GC] lease expire, create new lease")
		if err := flushLease(txn, key, id, lease); err != nil {
			log.Error("[GC]  create lease failed", zap.Reflect("key", key), zap.Reflect("id", id), zap.Duration("lease", lease), zap.Error(err))
			return false, err
		}
		return true, nil
	}

	if bytes.Equal(curID, id) {
		if err := flushLease(txn, key, id, lease); err != nil {
			log.Error("[GC] flush lease failed", zap.Reflect("id", id), zap.Duration("lease", lease), zap.Error(err))
			return false, err
		}
		return true, nil
	}
	if env := log.Check(zap.DebugLevel, "[GC] leader deffernt id"); env != nil {
		env.Write(zap.Reflect("current-id", curID), zap.Reflect("store-id", id))
	}

	return false, nil
}

type GC struct {
	store  kv.Storage
	SysGC  *conf.GC
	TikvGC *conf.TikvGC
}

func NewGC(store kv.Storage, sconf *conf.GC, tconf *conf.TikvGC) *GC {
	gc := &GC{
		store:  store,
		SysGC:  sconf,
		TikvGC: tconf,
	}
	if gc.SysGC.Enable {
		go gc.startGC()
	}
	if gc.TikvGC.Enable {
		go gc.startTikvGC()
	}
	return gc
}

type SysGC struct {
	BatchLimit     int64
	Enable         bool
	Interval       time.Duration
	LeaderLifeTime time.Duration
}

type TikvGC struct {
	Enable            bool
	Interval          time.Duration
	LeaderLifeTime    time.Duration
	SafePointLifeTime time.Duration
	Concurrency       int
}

func (s *GC) isLeader(leader []byte) (bool, error) {
	count := 0
	for {
		txn, err := s.store.Begin()
		if err != nil {
			return false, err
		}

		isLeader, err := checkLeader(txn, leader, gInstanceID.Bytes(), s.SysGC.LeaderLifeTime)
		if err != nil {
			txn.Rollback()
			if kv.IsRetryableError(err) {
				count++
				if count < 3 {
					continue
				}
			}
			return isLeader, err
		}

		if err := txn.Commit(context.Background()); err != nil {
			txn.Rollback()
			if kv.IsRetryableError(err) {
				count++
				if count < 3 {
					continue
				}
			}
			return isLeader, err
		}
		return isLeader, err
	}
}

func (s *GC) startGC() error {
	ticker := time.NewTicker(s.SysGC.Interval)
	gcLeader := sysKey(sysGCLeader)
	for _ = range ticker.C {
		isLeader, err := s.isLeader(gcLeader)
		if err != nil {
			status.M.IsLeaderGaugeVec.WithLabelValues("GC").Set(0)
			log.Error("[GC] check GC leader failed", zap.String("leader-key", string(gcLeader)), zap.Duration("interval", s.SysGC.Interval), zap.Error(err))
			continue
		}
		if !isLeader {
			status.M.IsLeaderGaugeVec.WithLabelValues("GC").Set(0)
			if env := log.Check(zap.DebugLevel, "[GC] not GC leader"); env != nil {
				env.Write(zap.String("leader-key", string(sysGCLeader)), zap.Duration("interval", s.SysGC.Interval), zap.Bool("is leader", isLeader))
			}
			continue
		}

		status.M.IsLeaderGaugeVec.WithLabelValues("GC").Set(1)
		if err := doGC(s.store, s.SysGC.BatchLimit); err != nil {
			log.Error("[GC] do GC failed", zap.Int64("batch-limit", s.SysGC.BatchLimit), zap.Duration("interval", s.SysGC.Interval), zap.Error(err))
			continue
		}
	}
	return nil
}
