package logger

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/chaos-io/chaos/logs"
	"github.com/chaos-io/chaos/redis"
)

var ctx = context.Background()

func LogRecent(name, message, severity string, pipeliner redis.Pipeliner) {
	if severity == "" {
		severity = "INFO"
	}
	dest := fmt.Sprintf("recent:%s:%s", name, severity)
	message = time.Now().Local().String() + " " + message

	if pipeliner == nil {
		pipeliner = redis.Pipeline()
	}

	pipeliner.LPush(ctx, dest, message)
	pipeliner.LTrim(ctx, dest, 0, 99)
	if _, err := pipeliner.Exec(ctx); err != nil {
		logs.Warnw("failed to exec pipeline", "name", name, "message", message, "error", err)
	}
}

func LogCommon(name, message, severity string, timeout int64) {
	dest := fmt.Sprintf("common:%s:%s", name, severity)
	startKey := dest + ":start"
	end := time.Now().Add(time.Duration(timeout) * time.Millisecond)

	for time.Now().Before(end) {
		err := redis.Watch(ctx, func(tx *redis.Tx) error {
			hourStart := time.Now().Local().Hour()
			existing, _ := strconv.Atoi(tx.Get(ctx, startKey).Val())

			if _, err := tx.Pipelined(ctx, func(pipe redis.Pipeliner) error {
				if existing != 0 && existing < hourStart {
					pipe.Rename(ctx, dest, dest+":last")
					pipe.Rename(ctx, startKey, dest+":pstart")
					pipe.Set(ctx, startKey, hourStart, 0)
				} else {
					pipe.Set(ctx, startKey, hourStart, 0)
				}

				pipe.ZIncrBy(ctx, dest, 1, message)
				LogRecent(name, message, severity, pipe)
				return nil
			}); err != nil {
				logs.Warnw("failed to exec pipeline", "name", name, "message", message, "error", err)
				return err
			}
			return nil
		})
		if err != nil {
			logs.Warnw("failed to watch pipeline", "name", name, "message", message, "error", err)
		}
	}
}
