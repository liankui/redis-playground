package retailer

import (
	"context"
	"encoding/json"
	"time"

	"github.com/chaos-io/chaos/logs"
	"github.com/chaos-io/chaos/redis"
)

type Inventory struct {
	Id     string
	Data   string
	Cached int64
}

func NewInventory(id, data string, cached int64) Inventory {
	return Inventory{
		Id:     id,
		Data:   data,
		Cached: cached,
	}
}

func Get(id string) Inventory {
	return NewInventory(id, "data to cache...", time.Now().Unix())
}

func ScheduleRowCache(ctx context.Context, rowId string, delay int64) {
	// 先设置数据行的延迟值
	_, _ = redis.ZAdd(ctx, "delay:", float64(delay), rowId)
	// 立即对需要缓存的数据行进行调度
	_, _ = redis.ZAdd(ctx, "schedule:", float64(time.Now().Unix()), rowId)
}

func CacheRows(ctx context.Context) {
	for !QUIT {
		now := time.Now().Unix()
		// 尝试获取下一个需要被缓存的数据行以及该行的调度时间戳
		next, _ := redis.ZRangeWithScores(ctx, "schedule:", 0, 0)
		// 暂时没有行需要被缓存，休眠50ms后重试
		if len(next) == 0 || next[0].Score > float64(now) {
			time.Sleep(50 * time.Millisecond)
			continue
		}

		rowId := next[0].Member.(string)
		// 提前获取下一次调度的延迟时间
		delayScore, _ := redis.ZScore(ctx, "delay:", rowId)
		if delayScore <= 0 {
			_, _ = redis.ZRem(ctx, "delay:", rowId)
			_, _ = redis.ZRem(ctx, "schedule:", rowId)
			_ = redis.Del(ctx, "inv:"+rowId)
			continue
		}

		row := Get(rowId)
		_, _ = redis.ZAdd(ctx, "schedule:", float64(now), rowId)
		jsonRow, err := json.Marshal(row)
		if err != nil {
			logs.Warnw("json marshal err", "error", err)
		}
		// 更新调度时间并设置缓存值
		_, _ = redis.Set(ctx, "inv:"+rowId, jsonRow, 0)
	}
}
