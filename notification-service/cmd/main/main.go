package main

import (
	"log"
	"notification-service/internal/consumer"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	rabbitmqURL := os.Getenv("RABBITMQ_URL")
	if rabbitmqURL == "" {
		rabbitmqURL = "amqp://guest:guest@localhost:5672/"
	}

	c, err := consumer.NewConsumer(rabbitmqURL)
	if err != nil {
		log.Fatalf("failed to create consumer: %v", err)
	}
	defer c.Close()

	// Start consuming in background goroutine
	go func() {
		if err := c.Start(); err != nil {
			log.Fatalf("consumer error: %v", err)
		}
	}()

	// Graceful shutdown on SIGINT / SIGTERM
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("Shutting down notification service...")
}
