package main

import (
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/redis/go-redis/v9"
	"notification-service/internal/consumer"
	"notification-service/internal/provider"
	"notification-service/internal/worker"
)

func main() {
	rabbitmqURL := envOrDefault("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/")

	redisClient := redis.NewClient(&redis.Options{
		Addr: envOrDefault("REDIS_ADDR", "localhost:6379"),
	})

	maxRetries := parseInt("MAX_RETRIES", 3)

	// Choose provider based on env variable (REAL or SIMULATED)
	var sender provider.EmailSender
	if os.Getenv("PROVIDER_MODE") == "REAL" {
		log.Fatal("REAL provider not configured in this build; set PROVIDER_MODE=SIMULATED")
	} else {
		log.Println("[Main] Using SIMULATED email provider")
		sender = provider.NewSimulatedProvider()
	}

	w := worker.NewWorker(sender, redisClient, maxRetries)

	c, err := consumer.NewConsumer(rabbitmqURL, w)
	if err != nil {
		log.Fatalf("failed to create consumer: %v", err)
	}
	defer c.Close()

	go func() {
		if err := c.Start(); err != nil {
			log.Fatalf("consumer error: %v", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("Shutting down notification service...")
}

func envOrDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
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
