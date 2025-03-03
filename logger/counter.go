package logger

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/chaos-io/chaos/logs"
	"github.com/chaos-io/chaos/redis"
)

var PRECISION = []int64{1, 5, 60, 300, 3600, 18000, 86400}

var (
	QUIT        = false
	SAMPLECOUNT = 100
)

var (
	LASTCHECKED        int64 = 0
	ISUNDERMAINTENANCE       = false
)

var (
	CONFIG  = map[string]map[string]interface{}{}
	CHECKED = map[string]int64{}
)

func UpdateCounter(name string, count, now int64) {
	if now == 0 {
		now = time.Now().Unix()
	}

	pipe := redis.Pipeline()
	for _, prec := range PRECISION {
		// 取得当前时间片的开始时间
		pnow := (now / prec) * prec
		hash := fmt.Sprintf("%d:%s", prec, name)
		pipe.ZAdd(ctx, "known:", redis.Z{Score: 0, Member: hash})
		pipe.HIncrBy(ctx, "count:"+hash, strconv.Itoa(int(pnow)), count)
	}

	if _, err := pipe.Exec(ctx); err != nil {
		logs.Warnw("failed to exec pipeline", "name", name, "error", err)
	}
}

func GetCounter(name, precision string) [][]int {
	hash := fmt.Sprintf("%s:%s", precision, name)
	data, _ := redis.HGetAll(ctx, "count:"+hash)
	res := make([][]int, 0, len(data))
	for k, v := range data {
		tmp := make([]int, 2)
		_k, _ := strconv.Atoi(k)
		_v, _ := strconv.Atoi(v)
		tmp[0], tmp[1] = _k, _v
		res = append(res, tmp)
	}

	sort.Slice(res, func(i, j int) bool { return res[i][0] < res[j][0] })
	return res
}

func CleanCounter() {
	passes := 0

	for !QUIT {
		start := time.Now().Unix()
		var index int64 = 0
		zCard, _ := redis.ZCard(ctx, "known:")
		for index < zCard {
			hash, _ := redis.ZRange(ctx, "known:", index, index)
			index++

			if len(hash) == 0 {
				break
			}

			hashVal := hash[0]
			prec, _ := strconv.Atoi(strings.Split(hashVal, ":")[0])
			bprec := prec / 60
			if bprec == 0 {
				bprec = 1
			}
			if passes%bprec != 0 {
				continue
			}

			hkey := "count:" + hashVal
			cutoff := int(time.Now().Unix()) - SAMPLECOUNT*prec
			samples, _ := redis.HKeys(ctx, hkey)
			sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
			remove := sort.SearchStrings(samples, strconv.Itoa(cutoff))

			if remove != 0 {
				_, _ = redis.HDel(ctx, hkey, samples[:remove]...)
				if remove == len(samples) {
					err := redis.Watch(ctx, func(tx *redis.Tx) error {
						if tx.HLen(ctx, hkey).Val() == 0 {
							pipe := tx.Pipeline()
							pipe.ZRem(ctx, "known:", hashVal)
							if _, err := pipe.Exec(ctx); err != nil {
								logs.Warnw("failed to exec pipeline", "name", hash, "error", err)
								return err
							}
							index--
						}
						return nil
					})
					if err != nil {
						logs.Warnw("failed to watch pipeline", "name", hash, "error", err)
						continue
					}
				}
			}
		}
		passes++
		dur := min(time.Now().Unix()-start+1, 60)
		time.Sleep(time.Duration(max(60-dur)) * time.Minute)
	}
}
