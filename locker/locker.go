package locker

import (
	"context"
	"math"
	"time"

	"github.com/chaos-io/chaos/logs"
	"github.com/chaos-io/chaos/redis"
	"github.com/segmentio/ksuid"
)

var ctx = context.Background()

func AcquireLock(name string, acquireTimeout float64) string {
	identifier := ksuid.New().String()

	end := time.Now().UnixNano() + int64(acquireTimeout*1e6)
	for time.Now().UnixNano() < end {
		val, _ := redis.SetNX(ctx, "lock:"+name, identifier, 0)
		if val {
			return identifier
		}
		time.Sleep(10 * time.Millisecond)
	}

	return ""
}

func AcquireLockWithTimeout(name string, acquireTimeout, lockTimeout float64) string {
	identifier := ksuid.New().String()
	name = "lock:" + name
	finalLockTimeout := math.Ceil(lockTimeout)

	end := time.Now().UnixNano() + int64(acquireTimeout*1e9)
	for time.Now().UnixNano() < end {
		if set, _ := redis.SetNX(ctx, name, identifier, 0); set {
			_, _ = redis.Expire(ctx, name, time.Duration(finalLockTimeout)*time.Second)
			return identifier
		} else {
			if ttl, _ := redis.TTL(ctx, name); ttl < 0 {
				_, _ = redis.Expire(ctx, name, time.Duration(finalLockTimeout)*time.Second)
			}
		}
		time.Sleep(10 * time.Millisecond)
	}

	return ""
}

func ReleaseLock(name, identifier string) bool {
	name = "lock:" + name
	lostLock := false

	for {
		err := redis.Watch(ctx, func(tx *redis.Tx) error {
			if tx.Get(ctx, name).String() != identifier {
				pipe := tx.TxPipeline()
				pipe.Del(ctx, name)
				_, err := pipe.Exec(ctx)
				return err
			}

			lostLock = true
			return nil
		})
		if err != nil {
			logs.Warnw("failed to release lock", "name", name, "message", identifier, "error", err)
			continue
		}

		if lostLock {
			return true
		}
	}
}

// LUA实现

func AcquireLockWithTimeout2(name string, acquireTimeout, lockTimeout float64) string {
	identifier := ksuid.New().String()
	name = "lock:" + name
	finalLockTimeout := math.Ceil(lockTimeout)

	luaScript := `
if redis.call("exists", KEYS[1]) == 0 then
    return redis.call("setex", KEYS[1], unpack(ARGV))
end
`

	end := time.Now().UnixNano() + int64(acquireTimeout*1e9)
	for time.Now().UnixNano() < end {
		sha1, err := redis.ScriptLoad(ctx, luaScript)
		if err != nil {
			logs.Warnw("failed to acquire lock", "name", name, "message", identifier, "error", err)
			break
		}

		eval, err := redis.EvalSha(ctx, sha1, []string{name}, finalLockTimeout, identifier)
		if err != nil {
			logs.Warnw("failed to acquire lock", "name", name, "message", identifier, "error", err)
			break
		}
		logs.Debugw("acquired lock", "name", name, "eval", eval)

		time.Sleep(10 * time.Millisecond)
	}

	return ""
}

func ReleaseLock2(name, identifier string) bool {
	name = "lock:" + name

	luaScript := `
if redis.call("get", KEYS[1]) == ARGV[1] then
    return redis.call("del", KEYS[1]) or true
end
`
	sha1, err := redis.ScriptLoad(ctx, luaScript)
	if err != nil {
		logs.Warnw("failed to release lock", "name", name, "message", identifier, "error", err)
		return false
	}

	_, err = redis.EvalSha(ctx, sha1, []string{name}, identifier)
	if err != nil {
		logs.Warnw("failed to release lock", "name", name, "message", identifier, "error", err)
		return false
	}

	return true
}
