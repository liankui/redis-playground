package retailer

import (
	"context"
	"crypto"
	"encoding/hex"
	"net/url"
	"sync/atomic"
	"time"

	"github.com/chaos-io/chaos/logs"
	"github.com/chaos-io/chaos/redis"
)

var (
	QUIT        = false
	LIMIT int64 = 10000000
	FLAG  int32 = 1
)

func CheckToken(ctx context.Context, token string) (string, error) {
	return redis.HGet(ctx, "login:", token)
}

func UpdateToken(ctx context.Context, token, user, item string) error {
	now := time.Now().Unix()

	// 维护令牌与已登录用户之间的映射
	_, err := redis.HSet(ctx, "login:", token, user)
	if err != nil {
		return err
	}

	// 记录令牌最后一次出现的时间
	_, err = redis.ZAdd(ctx, "recent:", float64(now), token)
	if err != nil {
		return err
	}

	// 记录用户浏览过的商品
	if len(item) > 0 {
		_, err := redis.ZAdd(ctx, "viewed:"+token, float64(now), item)
		if err != nil {
			return err
		}

		// 移除旧的记录，只保留用户最近浏览过的25个商品
		_, err = redis.ZRemRangeByRank(ctx, "viewed:"+token, 0, -26)
		if err != nil {
			return err
		}

		//
		_, _ = redis.ZIncrBy(ctx, "viewed:", -1, item)
	}

	return nil
}

// 假设每天有500w用户访问，5000000/86400 = 58，需要每秒清理58个令牌，才能防止令牌过多问题发生。
func CleanSession(ctx context.Context) {
	for !QUIT {
		// 获取目前已有令牌的数量
		size, err := redis.ZCard(ctx, "recent:")
		if err != nil {
			logs.Warnw("failed to get recent: zcard", "error", err)
		}

		// 令牌数量未超过限制，休眠并在之后重新检查
		if size <= LIMIT {
			time.Sleep(time.Second)
			continue
		}

		// 获取需要移除的令牌Id
		endIndex := min(size-LIMIT, 100)
		tokens, err := redis.ZRange(ctx, "recent:", 0, endIndex-1)
		if err != nil {
			logs.Warnw("failed to get recent: zrange", "error", err)
		}

		sessions := make([]string, 0, len(tokens))
		for _, token := range tokens {
			sessions = append(sessions, "viewed:"+token)
			sessions = append(sessions, "cart:"+token)
		}

		// 移除最旧的令牌
		_ = redis.Del(ctx, sessions...)
		_, _ = redis.HDel(ctx, "login:", tokens...)
		_, _ = redis.ZRem(ctx, "recent:", tokens)
	}

	defer atomic.AddInt32(&FLAG, -1)
}

func AddToCart(ctx context.Context, session, item string, count int) error {
	if count <= 0 {
		_, err := redis.HDel(ctx, "cart:"+session, item)
		if err != nil {
			return err
		}
	} else {
		_, err := redis.HSet(ctx, "cart:"+session, item, count)
		if err != nil {
			return err
		}
	}
	return nil
}

// 该缓存函数可以让网站在5分钟之内不再重复动态生成视图页面。
// 查询本地redis延迟值通常低于1ms，查询位于同一个数据中心的延迟值通常低于5ms
func CacheRequest(ctx context.Context, request string, callback func(string) string) string {
	// 对于不能被缓存的请求，直接调用回调函数
	if !canCache(ctx, request) {
		return callback(request)
	}

	// 将请求转换成一个简单的字符串键，方便之后查找
	pageKey := "cache:" + hashRequest(request)
	content, _ := redis.Get(ctx, pageKey)

	// 如何页面没有被缓存，调用函数并放到缓存里面
	if len(content) == 0 {
		content = callback(request)
		_, _ = redis.Set(ctx, pageKey, content, 300*time.Second)
	}

	// 返回页面
	return content
}

func RescaleViewed(ctx context.Context) {
	for !QUIT {
		// 删除所有排名在20000名之后的商品
		_, _ = redis.ZRemRangeByRank(ctx, "viewed:", 20000, -1)
		// 将浏览次数降低为原来的一半
		_, _ = redis.ZInterStore(ctx, "viewed:", []string{"viewed:"}, []float64{0.5}, "")
		time.Sleep(300 * time.Second)
	}
}

func canCache(ctx context.Context, request string) bool {
	item := getUrlItem(request)
	if len(item) == 0 {
		return false
	}

	rank, err := redis.ZRank(ctx, "viewed:", item)
	if err != nil {
		logs.Warnw("failed to get viewed: zrank", "error", err)
	}

	return rank > 0 && rank < 10000
}

func getUrlItem(request string) string {
	parsed, _ := url.Parse(request)
	parseQuery, _ := url.ParseQuery(parsed.RawQuery)
	return parseQuery.Get("item")
}

func hashRequest(request string) string {
	hash := crypto.MD5.New()
	hash.Write([]byte(request))
	res := hash.Sum(nil)
	return hex.EncodeToString(res)
}
