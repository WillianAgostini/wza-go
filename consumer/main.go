package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/sony/gobreaker/v2"
)

var cb *gobreaker.CircuitBreaker[[]byte]

type PaymentConfig struct {
	url string
	cb  *gobreaker.CircuitBreaker[[]byte]
}

var defaultPayment = PaymentConfig{
	url: "http://localhost:4000/payments",
	cb:  nil,
}

var fallbackPayment = PaymentConfig{
	url: "http://localhost:4001/payments",
	cb:  nil,
}

const (
	numConsumers = 1
)

func main() {
	defaultPayment.cb = gobreaker.NewCircuitBreaker[[]byte](gobreaker.Settings{
		Name:    "Default",
		Timeout: 1 * time.Millisecond,
	})

	fallbackPayment.cb = gobreaker.NewCircuitBreaker[[]byte](gobreaker.Settings{
		Name:    "Fallback",
		Timeout: 1 * time.Millisecond,
	})

	nc, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		log.Fatal(err)
	}
	defer nc.Close()

	js := setupBroker(nc)
	if js == nil {
		log.Fatal("Failed to set up JetStream")
		return
	}

	var wg sync.WaitGroup
	for i := range numConsumers {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			consumerWorker(js, i)
		}(i)
	}

	wg.Wait()
}

type Data struct {
	CorrelationId string    `json:"correlationId"`
	Amount        float64   `json:"amount"`
	RequestedAt   time.Time `json:"requestedAt"`
}

func consumerWorker(js jetstream.JetStream, i int) {
	var count = 0
	log.Printf("Starting consumer %d", i)

	cons, err := js.CreateOrUpdateConsumer(context.Background(), "PAYMENT", jetstream.ConsumerConfig{
		AckPolicy:     jetstream.AckExplicitPolicy,
		FilterSubject: "PAYMENT.*",
		Durable:       "worker-group",
	})

	if err != nil {
		log.Printf("Failed to create consumer: %v", err)
		time.Sleep(time.Second)
		return
	}

	if cons == nil {
		log.Printf("Consumer is nil")
		time.Sleep(time.Second)
		return
	}

	consContext, err := cons.Consume(func(msg jetstream.Msg) {
		count++
		data := Data{}
		json.Unmarshal(msg.Data(), &data)

		if _, err := PostRequest(defaultPayment, data); err != nil {
			log.Printf("Failed to post request: %v", err)
			if _, err := PostRequest(fallbackPayment, data); err != nil {
				log.Printf("Failed to post request to fallback: %v", err)
				msg.NakWithDelay(time.Second * 1)
				return
			}
		}
		log.Printf("Message processed successfully: %s", msg.Subject())
		msg.Ack()
	})

	if err != nil {
		log.Printf("Consume error: %v", err)
		time.Sleep(time.Second)
		return
	}

	defer consContext.Stop()
	select {}

}

func SetupPaymentStream(nc *nats.Conn) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	js, err := jetstream.New(nc)
	if err != nil {
		return err
	}

	_, err = js.CreateStream(ctx, jetstream.StreamConfig{
		Name:     "PAYMENT",
		Subjects: []string{"PAYMENT.*"},
	})
	if err != nil {
		if err == jetstream.ErrStreamNameAlreadyInUse {
			fmt.Println("Stream PAYMENT já existe.")
			return nil
		}
		return err
	}

	fmt.Println("Stream PAYMENT criado com sucesso!")
	return nil
}

func setupBroker(nc *nats.Conn) jetstream.JetStream {
	js, err := jetstream.New(nc)
	if err != nil {
		log.Fatal(err)
	}

	if err := SetupPaymentStream(nc); err != nil {
		log.Fatalf("Erro ao configurar stream: %v", err)
	}

	return js
}

func PostRequest(config PaymentConfig, data Data) ([]byte, error) {
	body, err := config.cb.Execute(func() ([]byte, error) {
		return request(config, data)
	})
	if err != nil {
		return nil, err
	}

	return body, nil
}

func request(config PaymentConfig, data Data) ([]byte, error) {
	data.RequestedAt = time.Now()
	payload, _ := json.Marshal(data)
	_, err := http.Post(config.url, "application/json", bytes.NewBuffer(payload))
	if err != nil {
		return nil, err
	}

	return nil, nil
}
