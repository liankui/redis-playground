package pub_sub

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/chaos-io/chaos/logs"
	"github.com/chaos-io/chaos/redis"
	redis2 "github.com/redis/go-redis/v9"
)

var ctx = context.Background()

func publisher(n int) {
	time.Sleep(1 * time.Second)

	for n > 0 {
		redis.Publish(ctx, "channel", n)
		n--
	}
}

func RunPublish() {
	pubSub := redis.Subscribe(ctx, "channel")
	defer pubSub.Close()

	var count int32
	for item := range pubSub.Channel() {
		fmt.Println(item)
		atomic.AddInt32(&count, 1)
		fmt.Println(count)

		switch count {
		case 4:
			if err := pubSub.Unsubscribe(ctx, "channel"); err != nil {
				logs.Warnw("unsubscribe error", "error", err)
			} else {
				logs.Infow("unsubscribe success")
			}
		case 5:
			break
		default:
		}
	}
}

func PurchaseItem(buyerId, itemId, sellerId string, lprice int64) bool {
	buyer := fmt.Sprintf("users:%s", buyerId)
	seller := fmt.Sprintf("users:%s", sellerId)
	item := fmt.Sprintf("item:%s", itemId)
	inventory := fmt.Sprintf("inventory:%s", buyerId)
	end := time.Now().Unix() + 10

	for time.Now().Unix() < end {
		err := redis.Watch(ctx, func(tx *redis.Tx) error {
			if _, err := tx.TxPipelined(ctx, func(pipeliner redis2.Pipeliner) error {
				price := int64(tx.ZScore(ctx, "market:", item).Val())
				funds, _ := tx.HGet(ctx, buyer, "funds").Int64()
				if price != lprice || price > funds {
					return errors.New("can not afford this item")
				}

				pipeliner.HIncrBy(ctx, seller, "funds", price)
				pipeliner.HIncrBy(ctx, buyer, "funds", -price)
				pipeliner.SAdd(ctx, inventory, itemId)
				pipeliner.ZRem(ctx, "market:", item)
				return nil
			}); err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			logs.Warnw("failed to do tx", "error", err)
			return false
		}

		return true
	}

	return false
}

func UpdateToken(token, user, item string) {
	ts := float64(time.Now().UnixNano())
	_, _ = redis.HSet(ctx, "login:", token, user)
	_, _ = redis.ZAdd(ctx, "recent:", redis.Z{Score: ts, Member: token})
	if len(item) > 0 {
		_, _ = redis.ZAdd(ctx, "viewed:"+token, redis.Z{Score: ts, Member: item})
		_, _ = redis.ZRemRangeByRank(ctx, "viewed:"+token, 0, -26)
		_, _ = redis.ZIncrBy(ctx, "viewed:", -1, item)
	}
}

func UpdateTokenPipeline(token, user, item string) {
	ts := float64(time.Now().UnixNano())
	pipe := redis.Pipeline()
	pipe.HSet(ctx, "login:", token, user)
	pipe.ZAdd(ctx, "recent:", redis.Z{Score: ts, Member: token})
	if len(item) > 0 {
		pipe.ZAdd(ctx, "viewed:"+token, redis.Z{Score: ts, Member: item})
		pipe.ZRemRangeByRank(ctx, "viewed:"+token, 0, -26)
		pipe.ZIncrBy(ctx, "viewed:", -1, item)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		logs.Warnw("failed to do tx", "error", err)
	}
}
