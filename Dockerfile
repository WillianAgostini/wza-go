FROM golang:1.24-alpine AS builder

WORKDIR /app

RUN apk add --no-cache build-base sqlite-dev

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=1 GOOS=linux GOARCH=amd64 go build -o wza

FROM alpine:latest

WORKDIR /app

RUN apk add --no-cache sqlite-libs

COPY --from=builder /app/wza .

EXPOSE 9999

ENTRYPOINT ["./wza"]
