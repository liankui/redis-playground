package retailer

import (
	"context"
	"testing"
	"time"

	"github.com/chaos-io/chaos/redis"
	"github.com/segmentio/ksuid"
	"github.com/stretchr/testify/assert"
)

func Test_Cookie(t *testing.T) {
	ctx := context.Background()
	token := ksuid.New().String()
	username := "username"

	err := UpdateToken(ctx, token, username, "item1")
	assert.Nil(t, err)

	checkToken, err := CheckToken(ctx, token)
	assert.Nil(t, err)
	assert.Equal(t, username, checkToken)

	LIMIT = 0
	go CleanSession(ctx)
	time.Sleep(1 * time.Second)
	QUIT = true
	time.Sleep(2 * time.Second)

	assert.Equal(t, int32(0), FLAG)

	hLen, err := redis.HLen(ctx, "login:")
	assert.Nil(t, err)
	assert.Equal(t, int64(0), hLen)

	// reset(ctx)
}

func reset(ctx context.Context) {
	_, _ = redis.Do(ctx, "FLUSHDB")

	QUIT = false
	LIMIT = 10000000
	FLAG = 1
}
