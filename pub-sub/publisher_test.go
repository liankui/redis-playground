package pub_sub

import (
	"testing"

	"github.com/chaos-io/chaos/redis"
)

func BenchmarkUpdateToken(b *testing.B) {
	b.Run("updateToken", func(b *testing.B) {
		count := 0
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			count++
			UpdateToken("token", "user", "item")
		}
		defer redis.Do(ctx, "FLUSHDB")
	})

	b.Run("updateTokenPipeline", func(b *testing.B) {
		count := 0
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			count++
			UpdateTokenPipeline("token", "user", "item")
		}
		defer redis.Do(ctx, "FLUSHDB")
	})
}

/*
go test -bench .
BenchmarkUpdateToken/updateToken-8                  8964            135006 ns/op
BenchmarkUpdateToken/updateTokenPipeline-8         35094             34103 ns/op
*/
