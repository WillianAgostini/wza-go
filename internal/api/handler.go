package api

import (
	"fmt"
	"time"

	"github.com/valyala/fasthttp"

	"wza/internal/broker"
	"wza/internal/entity"
	"wza/internal/repository"
)

func HandlePostPayments(ctx *fasthttp.RequestCtx) {
	payment, err := entity.ParsePayment(ctx.PostBody())
	if err != nil {
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		return
	}
	ctx.SetStatusCode(fasthttp.StatusNoContent)
	go broker.Publish(payment)
}

func HandleGetPaymentsSummary(ctx *fasthttp.RequestCtx) {
	fromStr := string(ctx.QueryArgs().Peek("from"))
	toStr := string(ctx.QueryArgs().Peek("to"))

	to := time.Now()
	from := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)

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

	body := fmt.Sprintf(`{"default":{"totalRequests":%d,"totalAmount":%.2f},"fallback":{"totalRequests":%d,"totalAmount":%.2f}}`,
		defaultCount, defaultTotalAmount, fallbackCount, fallbackTotalAmount,
	)

	ctx.SetContentType("application/json")
	ctx.SetStatusCode(fasthttp.StatusOK)
	ctx.Write([]byte(body))
}

func HandlePurgePayments(ctx *fasthttp.RequestCtx) {
	repository.PurgeAllData()
	ctx.SetStatusCode(fasthttp.StatusNoContent)
}
