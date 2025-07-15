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
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

type PaymentConfig struct {
	url string
	cb  *gobreaker.CircuitBreaker[Data]
}

var defaultPayment = PaymentConfig{
	url: "http://localhost:4000/payments",
	cb:  nil,
}

var fallbackPayment = PaymentConfig{
	url: "http://localhost:4001/payments",
	cb:  nil,
}

type Data struct {
	CorrelationId string    `json:"correlationId"`
	Amount        float64   `json:"amount"`
	RequestedAt   time.Time `json:"requestedAt"`
}

const (
	numConsumers = 1
)

func main() {
	client, _ := mongo.Connect(options.Client().ApplyURI("mongodb://localhost:27017"))
	collection := client.Database("testing").Collection("numbers")
	defaultPayment.cb = gobreaker.NewCircuitBreaker[Data](gobreaker.Settings{
		Name:    "Default",
		Timeout: 1 * time.Millisecond,
	})

	fallbackPayment.cb = gobreaker.NewCircuitBreaker[Data](gobreaker.Settings{
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
			consumerWorker(i, js, collection)
		}(i)
	}

	wg.Wait()
}

func consumerWorker(i int, js jetstream.JetStream, collection *mongo.Collection) {
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
		payload := Data{}
		json.Unmarshal(msg.Data(), &payload)

		data, err := req(payload, msg)
		if err != nil {
			log.Printf("Failed to process message: %v", err)
			msg.NakWithDelay(time.Second * 1)
			return
		}
		result, err := collection.InsertOne(context.TODO(), data)
		if err != nil {
			log.Fatalf("Failed to insert data: %v", err)
		}

		log.Printf("Inserted document with _id: %v\n", result.InsertedID)
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

func req(data Data, msg jetstream.Msg) (Data, error) {
	if data, err := PostRequest(defaultPayment, data); err != nil {
		log.Printf("Failed to post request: %v", err)
		if _, err := PostRequest(fallbackPayment, data); err != nil {
			log.Printf("Failed to post request to fallback: %v", err)
			return Data{}, err
		}
	}
	return data, nil
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

func PostRequest(config PaymentConfig, payload Data) (Data, error) {
	data, err := config.cb.Execute(func() (Data, error) {
		return request(config, payload)
	})
	if err != nil {
		return Data{}, err
	}

	return data, nil
}

func request(config PaymentConfig, data Data) (Data, error) {
	data.RequestedAt = time.Now().UTC()
	payload, _ := json.Marshal(data)
	_, err := http.Post(config.url, "application/json", bytes.NewBuffer(payload))
	if err != nil {
		return Data{}, err
	}

	return data, nil
}
