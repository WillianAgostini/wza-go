package request

import (
	"bytes"
	"encoding/json"
	"net/http"
	"time"
	"wza/internal/config"
	"wza/internal/entity"

	"github.com/sony/gobreaker/v2"
)

type paymentConfig struct {
	url    string
	cb     *gobreaker.CircuitBreaker[*entity.PaymentRequest]
	client http.Client
}

var defaultConfig = paymentConfig{}

var fallbackConfig = paymentConfig{}

func Init() {
	defaultConfig.url = config.GetEnv("DEFAULT_URL", "http://localhost:8001/payments")
	defaultConfig.cb = gobreaker.NewCircuitBreaker[*entity.PaymentRequest](gobreaker.Settings{
		Name:    "Default",
		Timeout: 1 * time.Millisecond,
	})
	defaultConfig.client = http.Client{
		Timeout: 500 * time.Millisecond,
	}

	fallbackConfig.url = config.GetEnv("FALLBACK_URL", "http://localhost:8002/payments")
	fallbackConfig.cb = gobreaker.NewCircuitBreaker[*entity.PaymentRequest](gobreaker.Settings{
		Name:    "Fallback",
		Timeout: 1 * time.Millisecond,
	})
	fallbackConfig.client = http.Client{
		Timeout: 2 * time.Second,
	}
}

func post(payment *entity.PaymentRequest, config paymentConfig) (*entity.PaymentRequest, error) {
	return config.cb.Execute(func() (*entity.PaymentRequest, error) {
		payload, _ := json.Marshal(payment)
		_, err := config.client.Post(config.url, "application/json", bytes.NewBuffer(payload))
		if err != nil {
			return nil, err
		}

		return payment, nil
	})
}

func PostDefault(payment *entity.PaymentRequest) (*entity.PaymentRequest, error) {
	return post(payment, defaultConfig)
}

func PostFallback(payment *entity.PaymentRequest) (*entity.PaymentRequest, error) {
	return post(payment, fallbackConfig)
}
