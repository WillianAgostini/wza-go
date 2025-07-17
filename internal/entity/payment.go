package entity

import (
	"encoding/json"
	"errors"
	"time"
)

type PaymentRequest struct {
	CorrelationId string    `json:"correlationid"`
	Amount        float64   `json:"amount"`
	RequestedAt   time.Time `json:"requestedat"`
}

var (
	ErrInvalidBody = errors.New("invalid JSON body")
)

func ParsePayment(body []byte) (*PaymentRequest, error) {
	var req PaymentRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, ErrInvalidBody
	}

	return &req, nil
}

func SetRequestedAt(payment *PaymentRequest) {
	payment.RequestedAt = time.Now()
}
