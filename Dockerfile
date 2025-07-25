FROM golang:1.24-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o wza

FROM alpine:latest

WORKDIR /app

COPY --from=builder /app/wza .

EXPOSE 9999

ENTRYPOINT ["./wza"]
