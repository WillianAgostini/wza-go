package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
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

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		go publishToQueue(js, "!!!!!")
		w.WriteHeader(http.StatusNoContent)
	})
	http.ListenAndServe(":8080", nil)
}

func publishToQueue(js jetstream.JetStream, msg string) error {
	_, err := js.PublishAsync("PAYMENT.created", []byte(msg))
	if err != nil {
		log.Printf("Failed to publish: %v", err)
		return err
	}
	log.Println("Published message to PAYMENT.created")
	return nil
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
