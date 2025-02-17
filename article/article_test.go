package article

import (
	"context"
	"testing"

	"github.com/chaos-io/chaos/logs"
	"github.com/chaos-io/chaos/redis"
	"github.com/stretchr/testify/assert"
)

func Test(t *testing.T) {
	ctx := context.Background()

	articleId, err := PostArticle(ctx, "username", "a title", "https://g.cn")
	assert.NoError(t, err)
	logs.Infow("", "articleId", articleId)

	articleKey := "article:" + articleId
	all, err := redis.HGetAll(ctx, articleKey)
	assert.NoError(t, err)
	logs.Infow("", "article:"+articleId, all)

	err = ArticleVote(ctx, articleKey, "other_user")
	assert.NoError(t, err)

	hGet, err := redis.HGet(ctx, articleKey, "votes")
	assert.NoError(t, err)
	logs.Infow("", "hGet", hGet)

	getArticles, err := GetArticle(ctx, 1, "")
	assert.NoError(t, err)
	logs.Infow("", "getArticle", getArticles)

	err = AddRemoveGroups(ctx, articleId, []string{"new-group"}, []string{})
	assert.NoError(t, err)

	articles, err := GetGroupArticles(ctx, "new-group", "score:", 1)
	assert.NoError(t, err)
	logs.Infow("", "articles", articles)

	// _, err = redis.Do(ctx, "FLUSHDB")
	// assert.NoError(t, err)
}
