package util

import (
	"context"
	"time"

	"github.com/meitu/bifrost/commons/log"
	"github.com/meitu/bifrost/push/status"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/store/tikv/gcworker"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/twinj/uuid"
	"go.uber.org/zap"
)

var (
	sysTikvGCLeader        = []byte("TGC:GCLeader")
	sysTikvGCLastSafePoint = []byte("TGC:LastSafePoint")
)

const (
	tikvGcTimeFormat = "20060102-15:04:05 -0700 MST"
)

// startTikvGC start tikv gcwork
func (s *GC) startTikvGC() error {
	ticker := time.NewTicker(s.TikvGC.Interval)
	defer ticker.Stop()
	ctx := context.Background()
	tikvGCLeader := sysKey(sysTikvGCLeader)
	for range ticker.C {
		isLeader, err := s.isLeader(tikvGCLeader)
		if err != nil {
			status.M.IsLeaderGaugeVec.WithLabelValues("TikvGC").Set(0)
			log.Error("[TikvGC] check TikvGC leader failed", zap.Reflect("leader-key", tikvGCLeader), zap.Duration("interval", s.TikvGC.Interval), zap.Error(err))
			continue
		}
		if !isLeader {
			status.M.IsLeaderGaugeVec.WithLabelValues("TikvGC").Set(0)
			if env := log.Check(zap.DebugLevel, "[TikvGC] not TikvGC leader"); env != nil {
				env.Write(zap.Reflect("leader-key", sysTikvGCLeader), zap.Duration("interval", s.TikvGC.Interval), zap.Bool("is leader", isLeader))
			}
			continue
		}

		status.M.IsLeaderGaugeVec.WithLabelValues("TikvGC").Set(1)
		if err := runTikvGC(ctx, s.store, s.TikvGC.SafePointLifeTime, s.TikvGC.Concurrency); err != nil {
			log.Error("[TikvGC] do TikvGC failed", zap.Duration("safe-point-life-time", s.TikvGC.SafePointLifeTime), zap.Error(err))
			continue
		}
	}
	return nil
}

func runTikvGC(ctx context.Context, store kv.Storage, lifeTime time.Duration, concurrency int) error {
	uuid := uuid.NewV4()
	newPoint, err := getNewSafePoint(store, lifeTime)
	if err != nil {
		return err
	}

	lastPoint, err := getLastSafePoint(store)
	if err != nil {
		return err
	}

	if lastPoint != nil && newPoint.Before(*lastPoint) {
		log.Info("[TikvGC] last safe point is later than current on,no need to gc.",
			zap.Time("last", *lastPoint), zap.Time("current", *newPoint))
		return nil
	}

	if lastPoint == nil {
		log.Info("[TikvGC] current safe point ", zap.Time("current", *newPoint))
	} else {
		log.Info("[TikvGC] current safe point ", zap.Time("current", *newPoint), zap.Time("last", *lastPoint))
	}

	if err := saveLastSafePoint(ctx, store, newPoint); err != nil {
		log.Error("[TikvGC] save last safe point err ", zap.Time("current", *newPoint))
		return err
	}
	safePoint := oracle.ComposeTS(oracle.GetPhysical(*newPoint), 0)
	if err := gcworker.RunGCJob(ctx, store.(tikv.Storage), safePoint, uuid.String(), concurrency); err != nil {
		return err
	}
	return nil

}

func saveLastSafePoint(ctx context.Context, store kv.Storage, safePoint *time.Time) error {
	txn, err := store.Begin()
	if err != nil {
		return err
	}
	if err := txn.Set(sysTikvGCLastSafePoint, []byte(safePoint.Format(tikvGcTimeFormat))); err != nil {
		return err
	}
	if err := txn.Commit(ctx); err != nil {
		txn.Rollback()
		return err
	}
	return nil
}

func getNewSafePoint(store kv.Storage, lifeTime time.Duration) (*time.Time, error) {
	currentVer, err := store.CurrentVersion()
	if err != nil {
		return nil, err
	}
	physical := oracle.ExtractPhysical(currentVer.Ver)
	sec, nsec := physical/1e3, (physical%1e3)*1e6
	now := time.Unix(sec, nsec)
	safePoint := now.Add(-lifeTime)
	return &safePoint, nil
}

func getLastSafePoint(store kv.Storage) (*time.Time, error) {
	txn, err := store.Begin()
	if err != nil {
		return nil, err
	}
	val, err := txn.Get(sysKey(sysTikvGCLastSafePoint))
	if err != nil {
		if IsErrNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	str := string(val)
	if len(str) == 0 {
		return nil, nil
	}
	t, err := time.Parse(tikvGcTimeFormat, str)
	if err != nil {
		return nil, err
	}
	return &t, nil
}
