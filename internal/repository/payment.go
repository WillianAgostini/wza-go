package repository

import (
	"context"
	"encoding/json"
	"log"
	"strconv"
	"time"
	"wza/internal/config"
	"wza/internal/entity"

	"github.com/redis/go-redis/v9"
)

var (
	rdb *redis.Client
	ctx = context.Background()
)

func Init() {
	connect()
	log.Println("Redis connected successfully")
}

func connect() {
	addr := config.GetEnv("STORAGE_URL", "localhost:6379")
	rdb = redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: "",
		DB:       0,
	})
	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Panicf("failed to connect to Redis: %v", err)
	}
}

func insertPayment(set string, req *entity.PaymentRequest) {
	b, err := json.Marshal(req)
	if err != nil {
		log.Printf("insertPayment marshal error: %v", err)
		return
	}
	score := float64(req.RequestedAt.Unix())
	if err := rdb.ZAdd(ctx, set, redis.Z{Score: score, Member: b}).Err(); err != nil {
		log.Printf("insertPayment(%s) error: %v", set, err)
	}
}

func totalByPeriod(set string, from, to time.Time) (int, float64) {
	fromScore := float64(from.Unix())
	toScore := float64(to.Unix())

	vals, err := rdb.ZRangeByScore(ctx, set, &redis.ZRangeBy{
		Min: formatFloat(fromScore),
		Max: formatFloat(toScore),
	}).Result()
	if err != nil {
		log.Printf("totalByPeriod(%s) error: %v", set, err)
		return 0, 0
	}

	count := 0
	total := 0.0
	for _, v := range vals {
		var req entity.PaymentRequest
		if err := json.Unmarshal([]byte(v), &req); err != nil {
			continue
		}
		count++
		total += req.Amount
	}
	return count, total
}

func formatFloat(f float64) string {
	return strconv.FormatFloat(f, 'f', -1, 64)
}

func InsertDefault(req *entity.PaymentRequest) {
	insertPayment("paymentsDefault", req)
}

func TotalByPeriodDefault(from, to time.Time) (int, float64) {
	return totalByPeriod("paymentsDefault", from, to)
}

func InsertFallback(req *entity.PaymentRequest) {
	insertPayment("paymentsFallback", req)
}

func TotalByPeriodFallback(from, to time.Time) (int, float64) {
	return totalByPeriod("paymentsFallback", from, to)
}

func PurgeAllData() error {
	tables := []string{"paymentsDefault", "paymentsFallback"}
	for _, set := range tables {
		if err := rdb.Del(ctx, set).Err(); err != nil {
			log.Printf("failed to purge set %s: %v", set, err)
			return err
		}
	}
	log.Println("All payment sets purged successfully")
	return nil
}
