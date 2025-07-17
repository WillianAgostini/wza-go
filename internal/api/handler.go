package api

import (
	"fmt"
	"time"
	"wza/internal/broker"
	"wza/internal/entity"
	"wza/internal/repository"

	"github.com/valyala/fasthttp"
)

func HandleRequest(ctx *fasthttp.RequestCtx) {
	switch string(ctx.Path()) {
	case "/payments":
		if ctx.IsPost() {
			handlePostPayments(ctx)
			return
		}
	case "/payments-summary":
		if ctx.IsGet() {
			handleGetPaymentsSummary(ctx)
			return
		}
	case "/purge-payments":
		if ctx.IsPost() {
			handlePurgePayments(ctx)
			return
		}
	}

	ctx.SetStatusCode(fasthttp.StatusNotFound)
	ctx.SetBodyString("404 - Not Found")
}

func handlePostPayments(ctx *fasthttp.RequestCtx) {
	body := ctx.PostBody()
	payment, err := entity.ParsePayment(body)

	if err != nil {
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		ctx.SetBodyString(err.Error())
		return
	}

	err = broker.Publish(payment)
	if err != nil {
		ctx.SetStatusCode(fasthttp.StatusInternalServerError)
		return
	}
	ctx.SetStatusCode(fasthttp.StatusNoContent)
}

func handleGetPaymentsSummary(ctx *fasthttp.RequestCtx) {
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

	body := fmt.Sprintf(
		`{"default":{"totalRequests":%d,"totalAmount":%.2f},"fallback":{"totalRequests":%d,"totalAmount":%.2f}}`,
		defaultCount, defaultTotalAmount, fallbackCount, fallbackTotalAmount,
	)

	ctx.SetStatusCode(fasthttp.StatusOK)
	ctx.SetContentType("application/json")
	ctx.SetBodyString(body)
}

func handlePurgePayments(ctx *fasthttp.RequestCtx) {
	repository.PurgeAllData()
	ctx.SetStatusCode(fasthttp.StatusOK)
}
