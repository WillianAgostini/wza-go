package job

import (
	"errors"
	"log"
	"strconv"
	"sync"
	"wza/internal/broker"
	"wza/internal/config"
	"wza/internal/entity"
	"wza/internal/repository"
	"wza/internal/request"
)

var numConsumers = 1

func Init() {
	request.Init()

	numConsumers, _ := strconv.Atoi(config.GetEnv("MAX_WORKERS", "1"))

	var wg sync.WaitGroup
	for i := range numConsumers {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			consumerWorker(i)
		}(i)
	}

	wg.Wait()
}

func consumerWorker(i int) {
	log.Printf("Starting consumer %d", i)

	broker.Subscribe(func(payment entity.PaymentRequest) error {
		response, err := request.PostDefault(&payment)

		if err != nil {
			response, err = request.PostFallback(&payment)
			if err != nil {
				return errors.New("can not process")
			}
			repository.InsertFallback(response)
			return nil
		}

		repository.InsertDefault(response)
		return nil
	})

	select {}
}
