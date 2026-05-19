FROM golang:1.24-alpine AS builder

WORKDIR /app

# Copy generated-repo so the replace directive in go.mod works
COPY generated-repo/ ./generated-repo/

# Copy payment-service source
COPY payment-service/ ./payment-service/

WORKDIR /app/payment-service
RUN go mod tidy && go build -o payment-service ./cmd/main

FROM alpine:3.19
WORKDIR /app
COPY --from=builder /app/payment-service/payment-service .
EXPOSE 9081
CMD ["./payment-service"]
