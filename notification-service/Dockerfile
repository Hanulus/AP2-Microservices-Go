FROM golang:1.22-alpine AS builder

WORKDIR /app

COPY notification-service/ ./notification-service/

WORKDIR /app/notification-service
RUN go mod tidy && go build -o notification-service ./cmd/main

FROM alpine:3.19
WORKDIR /app
COPY --from=builder /app/notification-service/notification-service .
CMD ["./notification-service"]
