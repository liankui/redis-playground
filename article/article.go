package article

import (
	"context"
	"strings"
	"time"

	"github.com/chaos-io/chaos/logs"
	"github.com/chaos-io/chaos/redis"
)

const (
	ONE_WEEK_IN_SECONDS = 7 * 86400
	VOTE_SCORE          = 432
)

func ArticleVote(ctx context.Context, article, user string) error {
	cutoff := time.Now().Unix() - ONE_WEEK_IN_SECONDS

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
	if _, err := redis.SAdd(ctx, "voted:"+articleId, user); err != nil {
		return err
	}

	if _, err := redis.ZIncrBy(ctx, "score:", VOTE_SCORE, article); err != nil {
		return err
	}

	if _, err := redis.HIncrBy(ctx, article, "votes", 1); err != nil {
		return err
	}

	return nil
}
