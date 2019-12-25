package store

import (
	"github.com/meitu/bifrost/commons/log"
	"github.com/pingcap/tidb/kv"
	"go.uber.org/zap"
	"golang.org/x/net/context"
)

func Retryable(job func(ctx context.Context) error, times int64, ctx context.Context) error {
	for {
		if err := job(ctx); err != nil {
			if err := ctx.Err(); err != nil {
				return err
			}
			if IsRetryableError(err) {
				log.Warn("retry store", zap.Int64("times", times), zap.Error(err))
				if times > 0 {
					times--
					continue
				}
				return err
			}
			return err
		}
		return nil
	}
}

func IsRetryableError(err error) bool {
	return kv.IsRetryableError(err)
}
