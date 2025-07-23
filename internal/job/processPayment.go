package job

import (
	"errors"
	"log"
	"strconv"
	"wza/internal/broker"
	"wza/internal/config"
	"wza/internal/entity"
	"wza/internal/repository"
	"wza/internal/request"
)

func Init() {
	request.Init()

	numConsumers, _ := strconv.Atoi(config.GetEnv("MAX_WORKERS", "1"))
	if numConsumers < 1 {
		numConsumers = 1
	}

	for i := 0; i < numConsumers; i++ {
		go consumerWorker(i)
	}
}

func consumerWorker(i int) {
	log.Printf("Starting consumer %d", i)

	broker.Subscribe(func(payment entity.PaymentRequest) error {
		if _, err := request.PostDefault(&payment); err == nil {
			repository.InsertDefault(&payment)
			return nil
		}
		if _, err := request.PostFallback(&payment); err == nil {
			repository.InsertFallback(&payment)
			return nil
		}
		return errors.New("can not process")
	})

	select {}
}
