FROM golang:1.24-alpine AS builder

WORKDIR /app

# Copy generated-repo so the replace directive in go.mod works
COPY generated-repo/ ./generated-repo/

# Copy order-service source
COPY order-service/ ./order-service/

WORKDIR /app/order-service
RUN go mod tidy && go build -o order-service ./cmd/main

FROM alpine:3.19
WORKDIR /app
COPY --from=builder /app/order-service/order-service .
EXPOSE 9080
CMD ["./order-service"]
