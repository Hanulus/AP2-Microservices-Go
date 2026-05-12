package main

import (
	"log"
	"net"
	"os"
	"strconv"
	"time"

	pbStream "github.com/Hanulus/ap2-generated/orderstream"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"order-service/internal/app"
	"order-service/internal/cache"
	"order-service/internal/repository"
	grpcTransport "order-service/internal/transport/grpc"
	httpTransport "order-service/internal/transport/http"
	"order-service/internal/usecase"
)

func main() {
	db, err := app.NewDB()
	if err != nil {
		log.Fatalf("db connection failed: %v", err)
	}
	defer db.Close()

	redisClient := redis.NewClient(&redis.Options{
		Addr: envOrDefault("REDIS_ADDR", "localhost:6379"),
	})

	cacheTTL := parseDuration("CACHE_TTL_SECONDS", 300)
	rateMax := parseInt("RATE_LIMIT_MAX", 10)
	rateWindow := parseDuration("RATE_LIMIT_WINDOW_SECONDS", 60)

	paymentGRPCAddr := envOrDefault("PAYMENT_GRPC_ADDR", "localhost:9082")
	paymentClient, err := repository.NewPaymentGRPCClient(paymentGRPCAddr)
	if err != nil {
		log.Fatalf("failed to connect to payment gRPC: %v", err)
	}

	// Wrap postgres repo with Redis cache-aside decorator
	pgRepo := repository.NewPostgresOrderRepo(db)
	cachedRepo := cache.NewCachedOrderRepo(pgRepo, redisClient, cacheTTL)

	orderUC := usecase.NewOrderUseCase(cachedRepo, paymentClient)

	streamPort := envOrDefault("GRPC_PORT", "9083")
	go startStreamingServer(orderUC, streamPort)

	handler := httpTransport.NewOrderHandler(orderUC)
	router := httpTransport.NewRouter(handler, redisClient, rateMax, rateWindow)

	port := envOrDefault("PORT", "9080")
	log.Printf("Order REST server starting on :%s", port)
	log.Fatal(router.Run(":" + port))
}

func startStreamingServer(uc *usecase.OrderUseCase, port string) {
	lis, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatalf("failed to listen on streaming port %s: %v", port, err)
	}
	grpcServer := grpc.NewServer()
	pbStream.RegisterOrderServiceServer(grpcServer, grpcTransport.NewOrderGRPCServer(uc))
	log.Printf("Order streaming gRPC server starting on :%s", port)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("streaming gRPC server error: %v", err)
	}
}

func envOrDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// parseDuration reads an env var as seconds and returns a time.Duration.
func parseDuration(key string, defaultSec int) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return time.Duration(defaultSec) * time.Second
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return time.Duration(defaultSec) * time.Second
	}
	return time.Duration(n) * time.Second
}

func parseInt(key string, def int) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	return n
}
