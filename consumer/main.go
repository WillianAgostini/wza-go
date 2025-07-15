package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const (
	numConsumers = 10
)

func main() {
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
		log.Printf("[%d,%d] %s => %s", i, count, msg.Subject(), string(msg.Data()))
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
