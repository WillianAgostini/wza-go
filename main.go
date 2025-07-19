package main

import (
	"log"
	"runtime"

	"github.com/fasthttp/router"
	"github.com/valyala/fasthttp"

	"wza/internal/api"
	"wza/internal/broker"
	"wza/internal/job"
	"wza/internal/repository"
)

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	broker.Init()
	repository.Init()
	go job.Init()
	HttpInit()
}

func HttpInit() {
	r := router.New()
	r.POST("/payments", api.HandlePostPaymentsFast)
	r.GET("/payments-summary", api.HandleGetPaymentsSummaryFast)
	r.GET("/purge-payments", api.HandlePurgePaymentsFast)
	log.Fatal(fasthttp.ListenAndServe(":9999", r.Handler))
}
