package main

import (
	"log"
	"wza/internal/api"
	"wza/internal/broker"
	"wza/internal/job"
	"wza/internal/repository"

	"github.com/valyala/fasthttp"
)

func main() {
	broker.Init()
	repository.Init()
	go job.Init()
	HttpInit()
}

func HttpInit() {
	log.Println("Serve running on :9999")
	if err := fasthttp.ListenAndServe(":9999", safeHandler(api.HandleRequest)); err != nil {
		log.Fatalf("Error starting server: %v", err)
	}
}

func safeHandler(h fasthttp.RequestHandler) fasthttp.RequestHandler {
	return func(ctx *fasthttp.RequestCtx) {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("Recovered from panic: %v", r)
				ctx.SetStatusCode(fasthttp.StatusInternalServerError)
				ctx.SetBodyString("Internal Server Error")
			}
		}()
		h(ctx)
	}
}
