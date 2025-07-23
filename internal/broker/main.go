package broker

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"wza/internal/config"
	"wza/internal/entity"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

var js jetstream.JetStream
var nc nats.Conn

func Init() {
	url := config.GetEnv("BROKER_URL", nats.DefaultURL)
	nc, err := nats.Connect(url)
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
	return nil
}

func Subscribe(handler func(entity.PaymentRequest) error) {
	maxAckPending, _ := strconv.Atoi(config.GetEnv("MAX_ACK_PENDING", "50"))
	cons, err := js.CreateOrUpdateConsumer(context.Background(), "PAYMENT", jetstream.ConsumerConfig{
		AckPolicy:     jetstream.AckExplicitPolicy,
		FilterSubject: "PAYMENT.*",
		Durable:       "worker-group",
		MaxAckPending: maxAckPending,
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
				msg.Nak()
				return
			}
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
