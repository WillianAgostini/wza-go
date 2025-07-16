package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

type Data struct {
	CorrelationId string    `json:"correlationid"`
	Amount        float64   `json:"amount"`
	RequestedAt   time.Time `json:"requestedat"`
}

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

	http.HandleFunc("/payments", func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "", http.StatusBadRequest)
			return
		}
		go publishToQueue(js, body)
		w.WriteHeader(http.StatusNoContent)
	})

	// Create this endpoint GET /payments-summary?from=2020-07-10T12:34:56.000Z&to=2020-07-10T12:35:56.000Z

	client, _ := mongo.Connect(options.Client().ApplyURI("mongodb://localhost:27017"))
	database := client.Database("rinha")
	defaultCollection := database.Collection("default")
	fallbackCollection := database.Collection("fallback")

	http.HandleFunc("/payments-summary", func(w http.ResponseWriter, r *http.Request) {
		from := r.URL.Query().Get("from")
		to := r.URL.Query().Get("to")
		if from == "" || to == "" {
			http.Error(w, "Missing 'from' or 'to' query parameters", http.StatusBadRequest)
			return
		}

		fromTime, err := time.Parse(time.RFC3339Nano, from)
		if err != nil {
			http.Error(w, "Invalid 'from' format", http.StatusBadRequest)
			return
		}
		toTime, err := time.Parse(time.RFC3339Nano, to)
		if err != nil {
			http.Error(w, "Invalid 'to' format", http.StatusBadRequest)
			return
		}

		filter := map[string]interface{}{
			"requestedat": map[string]interface{}{
				"$gte": fromTime,
				"$lte": toTime,
			},
		}

		type summary struct {
			TotalRequests int     `bson:"totalRequests"`
			TotalAmount   float64 `bson:"totalAmount"`
		}

		aggregate := []map[string]interface{}{
			{"$match": filter},
			{"$group": map[string]interface{}{
				"_id":           nil,
				"totalRequests": map[string]interface{}{"$sum": 1},
				"totalAmount":   map[string]interface{}{"$sum": "$amount"},
			}},
		}

		var defaultSummary, fallbackSummary summary
		var wg sync.WaitGroup
		wg.Add(2)

		go func() {
			defer wg.Done()
			cursor, err := defaultCollection.Aggregate(context.Background(), aggregate)
			if err == nil && cursor.Next(context.Background()) {
				cursor.Decode(&defaultSummary)
			}
			cursor.Close(context.Background())
		}()

		go func() {
			defer wg.Done()
			cursor, err := fallbackCollection.Aggregate(context.Background(), aggregate)
			if err == nil && cursor.Next(context.Background()) {
				cursor.Decode(&fallbackSummary)
			}
			cursor.Close(context.Background())
		}()

		wg.Wait()

		result := map[string]interface{}{
			"default": map[string]interface{}{
				"totalRequests": defaultSummary.TotalRequests,
				"totalAmount":   defaultSummary.TotalAmount,
			},
			"fallback": map[string]interface{}{
				"totalRequests": fallbackSummary.TotalRequests,
				"totalAmount":   fallbackSummary.TotalAmount,
			},
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(result)
	})

	http.ListenAndServe(":3000", nil)
}

func publishToQueue(js jetstream.JetStream, body []byte) error {
	_, err := js.PublishAsync("PAYMENT.created", body)
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
