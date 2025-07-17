package job

import (
	"errors"
	"log"
	"sync"
	"wza/internal/broker"
	"wza/internal/entity"
	"wza/internal/repository"
	"wza/internal/request"
)

const (
	numConsumers = 1
)

func Init() {
	request.Init()

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
			log.Println("saved on Fallback")
			return nil
		}

		repository.InsertDefault(response)
		log.Println("saved on Default")
		return nil
	})

	log.Printf("Message processed successfully")

	select {}
}
