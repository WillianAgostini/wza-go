package broker

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"wza/internal/entity"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

var js jetstream.JetStream
var nc nats.Conn

func Init() {
	nc, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		log.Fatal(err)
	}

	js = setupBroker(nc)
	if js == nil {
		log.Fatal("Failed to set up JetStream")
		return
	}
	log.Println("Broker connected successfully")
}

func setupBroker(nc *nats.Conn) jetstream.JetStream {
	js, err := jetstream.New(nc)
	if err != nil {
		log.Fatal(err)
	}

	if err := setupPaymentStream(nc); err != nil {
		log.Fatalf("Erro ao configurar stream: %v", err)
	}

	return js
}

func setupPaymentStream(nc *nats.Conn) error {
	js, err := jetstream.New(nc)
	if err != nil {
		return err
	}

	_, err = js.CreateStream(context.Background(), jetstream.StreamConfig{
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

func Publish(payment *entity.PaymentRequest) error {
	payload, err := json.Marshal(payment)
	if err != nil {
		log.Printf("Failed to marshal payment: %v", err)
		return err
	}
	_, err = js.PublishAsync("PAYMENT.created", payload)
	if err != nil {
		log.Printf("Failed to publish: %v", err)
		return err
	}
	log.Println("Published message to PAYMENT.created")
	return nil
}

func Subscribe(handler func(entity.PaymentRequest) error) {
	cons, err := js.CreateOrUpdateConsumer(context.Background(), "PAYMENT", jetstream.ConsumerConfig{
		AckPolicy:     jetstream.AckExplicitPolicy,
		FilterSubject: "PAYMENT.*",
		Durable:       "worker-group",
		MaxAckPending: 100,
	})
	if err != nil {
		log.Printf("Failed to create consumer: %v", err)
		return
	}
	if cons == nil {
		log.Printf("Consumer is nil")
		return
	}

	consContext, err := cons.Consume(func(msg jetstream.Msg) {
		go func(msg jetstream.Msg) {
			var payload entity.PaymentRequest
			if err := json.Unmarshal(msg.Data(), &payload); err != nil {
				log.Printf("Failed to unmarshal: %v", err)
				msg.Ack()
				return
			}
			if err := handler(payload); err != nil {
				log.Printf("Handler error: %v", err)
				// msg.NakWithDelay(time.Millisecond)
				msg.Nak()
				return
			}
			log.Printf("Message processed successfully")
			msg.Ack()
		}(msg)
	})

	if err != nil {
		log.Printf("Consume error: %v", err)
		return
	}

	defer consContext.Stop()
	select {}
}
