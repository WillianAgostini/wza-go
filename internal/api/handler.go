package api

import (
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/valyala/fasthttp"

	"wza/internal/broker"
	"wza/internal/entity"
	"wza/internal/repository"
)

func HandlePostPayments(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)

	payment, err := entity.ParsePayment(body)

	if err != nil {
		http.Error(w, "", http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusNoContent)

	go broker.Publish(payment)
}

func HandlePostPaymentsFast(ctx *fasthttp.RequestCtx) {
	payment, err := entity.ParsePayment(ctx.PostBody())
	if err != nil {
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		return
	}
	ctx.SetStatusCode(fasthttp.StatusNoContent)
	go broker.Publish(payment)
}

func HandleGetPaymentsSummary(w http.ResponseWriter, r *http.Request) {
	fromStr := r.URL.Query().Get("from")
	toStr := r.URL.Query().Get("to")

	now := time.Now()
	from := now
	to := now

	if fromStr != "" {
		if t, err := time.Parse(time.RFC3339Nano, fromStr); err == nil {
			from = t
		}
	}

	if toStr != "" {
		if t, err := time.Parse(time.RFC3339Nano, toStr); err == nil {
			to = t
		}
	}

	defaultCount, defaultTotalAmount := repository.TotalByPeriodDefault(from, to)
	fallbackCount, fallbackTotalAmount := repository.TotalByPeriodFallback(from, to)

	body := fmt.Sprintf(
		`{"default":{"totalRequests":%d,"totalAmount":%.2f},"fallback":{"totalRequests":%d,"totalAmount":%.2f}}`,
		defaultCount, defaultTotalAmount, fallbackCount, fallbackTotalAmount,
	)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(body))

}

func HandleGetPaymentsSummaryFast(ctx *fasthttp.RequestCtx) {
	fromStr := string(ctx.QueryArgs().Peek("from"))
	toStr := string(ctx.QueryArgs().Peek("to"))

	var from, to *time.Time

	if fromStr != "" {
		if t, err := time.Parse(time.RFC3339Nano, fromStr); err == nil {
			from = &t
		}
	}
	if toStr != "" {
		if t, err := time.Parse(time.RFC3339Nano, toStr); err == nil {
			to = &t
		}
	}

	defaultCount, defaultTotalAmount := repository.TotalByPeriodDefault(*from, *to)
	fallbackCount, fallbackTotalAmount := repository.TotalByPeriodFallback(*from, *to)

	body := fmt.Sprintf(`{"default":{"totalRequests":%d,"totalAmount":%.2f},"fallback":{"totalRequests":%d,"totalAmount":%.2f}}`,
		defaultCount, defaultTotalAmount, fallbackCount, fallbackTotalAmount,
	)

	ctx.SetContentType("application/json")
	ctx.SetStatusCode(fasthttp.StatusOK)
	ctx.Write([]byte(body))
}

func HandlePurgePayments(w http.ResponseWriter, r *http.Request) {
	repository.PurgeAllData()
	w.WriteHeader(http.StatusNoContent)
}

func HandlePurgePaymentsFast(ctx *fasthttp.RequestCtx) {
	repository.PurgeAllData()
	ctx.SetStatusCode(fasthttp.StatusNoContent)
}
