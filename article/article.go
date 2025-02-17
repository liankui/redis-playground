package article

import (
	"context"
	"strconv"
	"strings"
	"time"

	"github.com/chaos-io/chaos/logs"
	"github.com/chaos-io/chaos/redis"
)

const (
	OneWeekInSeconds = 7 * 86400
	VoteScore        = 432
	ArticlesPrePage  = 25
)

func ArticleVote(ctx context.Context, article, user string) error {
	// 计算文章的投票截止时间
	cutoff := time.Now().Unix() - OneWeekInSeconds

	// 检查是否还可以对文章进行投票
	score, err := redis.ZScore(ctx, "time:", article)
	if err != nil {
		return err
	}
	if score < float64(cutoff) {
		logs.Infow("vote time out", "article", article, "score", score, "cutoff", cutoff)
		return nil
	}

	split := strings.Split(article, ":")
	if len(split) != 2 {
		return logs.NewErrorw("invalid article format", article)
	}

	articleId := split[1]
	sadd, err := redis.SAdd(ctx, "voted:"+articleId, user)
	if err != nil {
		return err
	}
	// 如果用户第一次为这篇文章投票，增加这篇文章的评分和投票数量
	if sadd > 0 {
		if _, err := redis.ZIncrBy(ctx, "score:", VoteScore, article); err != nil {
			return err
		}

		if _, err := redis.HIncrBy(ctx, article, "votes", 1); err != nil {
			return err
		}
	}

	return nil
}

func PostArticle(ctx context.Context, user, title, link string) (string, error) {
	// 生成一个新的文章Id
	incrId, err := redis.Incr(ctx, "article:")
	if err != nil {
		return "", err
	}
	articleId := strconv.Itoa(int(incrId))

	// 将发布的文章的用户添加到文章的已投票用户名单中
	// 并将这个名单的过期时间设置为一周
	voted := "voted:" + articleId
	_, _ = redis.SAdd(ctx, voted, user)
	_, _ = redis.Expire(ctx, voted, OneWeekInSeconds*time.Second)

	// 将文章信息存储到一个散列里面
	now := time.Now().Unix()
	article := "article:" + articleId
	if _, err := redis.HSet(ctx, article, "title", title, "link", link, "poster", user, "time", now, "votes", 1); err != nil {
		logs.Warnw("failed to HSet article", "error", err)
		return "", err
	}

	// 将文章添加到根据发布时间排序的有序集合和根据评分排序的有序集合中
	if _, err := redis.ZAdd(ctx, "score:", float64(now+VoteScore), article); err != nil {
		logs.Warnw("failed to ZAdd article score", "error", err)
		return "", err
	}

	if _, err := redis.ZAdd(ctx, "time:", float64(now), article); err != nil {
		logs.Warnw("failed to ZAdd article time", "error", err)
		return "", err
	}

	return articleId, nil
}

func GetArticle(ctx context.Context, page int64, order string) ([]map[string]string, error) {
	if order == "" {
		order = "score:"
	}

	// 设置获取文章的起始索引和结束索引
	start := (page - 1) * ArticlesPrePage
	end := start + ArticlesPrePage - 1

	ids, err := redis.ZRevRange(ctx, order, start, end)
	if err != nil {
		return nil, err
	}

	articles := make([]map[string]string, 0)
	for _, id := range ids {
		data, _ := redis.HGetAll(ctx, id)
		data["id"] = id
		articles = append(articles, data)
	}

	return articles, nil
}

func AddRemoveGroups(ctx context.Context, articleId string, toAdd, toRemove []string) error {
	article := "article:" + articleId

	// 将文章添加到它所属的群组里面
	for _, group := range toAdd {
		_, _ = redis.SAdd(ctx, "group:"+group, article)
	}

	// 从群组里面移除文章
	for _, group := range toRemove {
		_, _ = redis.SRem(ctx, "group:"+group, article)
	}

	return nil
}

func GetGroupArticles(ctx context.Context, group, order string, page int64) ([]map[string]string, error) {
	if order == "" {
		order = "score:"
	}

	// 为每个群组的每种排列顺序都创建一个键
	key := order + group
	exists, _ := redis.Exists(ctx, key)
	if !exists {
		// 根据评分或者发布时间对群组文章进行排序
		res, _ := redis.ZInterStore(ctx, key, []string{"group:" + group, order}, []float64{}, "")
		if res <= 0 {
			logs.Warnw("zinterstore return 0", "key", key, "group", group, "order", order)
		}

		// 缓存60s
		_, _ = redis.Expire(ctx, key, 60*time.Second)
	}

	return GetArticle(ctx, page, key)
}
