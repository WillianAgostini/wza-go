package main

import (
	"net/http"
	"wza/internal/api"
	"wza/internal/broker"
	"wza/internal/job"
	"wza/internal/repository"
)

func main() {
	broker.Init()
	repository.Init()
	go job.Init()
	HttpInit()
}

func HttpInit() {
	http.HandleFunc("/payments", api.HandlePostPayments)
	http.HandleFunc("/payments-summary", api.HandleGetPaymentsSummary)
	http.HandleFunc("/purge-payments", api.HandlePurgePayments)
	http.ListenAndServe(":9999", nil)
}
