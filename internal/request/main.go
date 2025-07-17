package request

import (
	"bytes"
	"encoding/json"
	"net/http"
	"time"
	"wza/internal/entity"

	"github.com/sony/gobreaker/v2"
)

type paymentConfig struct {
	url string
	cb  *gobreaker.CircuitBreaker[*entity.PaymentRequest]
}

var defaultConfig = paymentConfig{
	url: "http://localhost:8001/payments",
	cb:  nil,
}

var fallbackConfig = paymentConfig{
	url: "http://localhost:8002/payments",
	cb:  nil,
}

func Init() {
	defaultConfig.cb = gobreaker.NewCircuitBreaker[*entity.PaymentRequest](gobreaker.Settings{
		Name:    "Default",
		Timeout: 1 * time.Millisecond,
	})

	fallbackConfig.cb = gobreaker.NewCircuitBreaker[*entity.PaymentRequest](gobreaker.Settings{
		Name:    "Fallback",
		Timeout: 1 * time.Millisecond,
	})
}

func post(client *http.Client, payment *entity.PaymentRequest, config paymentConfig) (*entity.PaymentRequest, error) {
	data, err := config.cb.Execute(func() (*entity.PaymentRequest, error) {
		entity.SetRequestedAt(payment)
		payload, _ := json.Marshal(payment)
		_, err := client.Post(config.url, "application/json", bytes.NewBuffer(payload))
		if err != nil {
			return nil, err
		}

		return payment, nil
	})
	if err != nil {
		return nil, err
	}

	return data, nil
}

func PostDefault(payment *entity.PaymentRequest) (*entity.PaymentRequest, error) {
	client := &http.Client{
		Timeout: 1 * time.Second,
	}
	return post(client, payment, defaultConfig)
}

func PostFallback(payment *entity.PaymentRequest) (*entity.PaymentRequest, error) {
	client := &http.Client{
		Timeout: 10 * time.Second,
	}
	return post(client, payment, fallbackConfig)
}
