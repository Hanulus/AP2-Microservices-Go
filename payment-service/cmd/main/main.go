package main

import (
	"context"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	pb "github.com/Hanulus/ap2-generated/payment"
	"google.golang.org/grpc"
	"payment-service/internal/app"
	"payment-service/internal/infrastructure/rabbitmq"
	"payment-service/internal/repository"
	grpcTransport "payment-service/internal/transport/grpc"
	httpTransport "payment-service/internal/transport/http"
	"payment-service/internal/usecase"
)

func main() {
	db, err := app.NewDB()
	if err != nil {
		log.Fatalf("db connection failed: %v", err)
	}
	defer db.Close()

	// Connect to RabbitMQ (best-effort: service starts even if broker is temporarily down)
	rabbitmqURL := os.Getenv("RABBITMQ_URL")
	if rabbitmqURL == "" {
		rabbitmqURL = "amqp://guest:guest@localhost:5672/"
	}
	publisher, err := rabbitmq.NewPublisher(rabbitmqURL)
	if err != nil {
		log.Printf("WARNING: RabbitMQ unavailable, events will not be published: %v", err)
	}
	if publisher != nil {
		defer publisher.Close()
	}

	paymentRepo := repository.NewPostgresPaymentRepo(db)
	paymentUC := usecase.NewPaymentUseCase(paymentRepo, publisher)

	// gRPC server
	grpcPort := os.Getenv("GRPC_PORT")
	if grpcPort == "" {
		grpcPort = "9082"
	}
	grpcServer := grpc.NewServer(
		grpc.UnaryInterceptor(grpcTransport.LoggingInterceptor),
	)
	pb.RegisterPaymentServiceServer(grpcServer, grpcTransport.NewPaymentGRPCServer(paymentUC))

	go func() {
		lis, err := net.Listen("tcp", ":"+grpcPort)
		if err != nil {
			log.Fatalf("gRPC listen error: %v", err)
		}
		log.Printf("Payment gRPC server starting on :%s", grpcPort)
		if err := grpcServer.Serve(lis); err != nil {
			log.Printf("gRPC server stopped: %v", err)
		}
	}()

	// HTTP server
	port := os.Getenv("PORT")
	if port == "" {
		port = "9081"
	}
	handler := httpTransport.NewPaymentHandler(paymentUC)
	router := httpTransport.NewRouter(handler)
	httpServer := &http.Server{
		Addr:    ":" + port,
		Handler: router,
	}
	go func() {
		log.Printf("Payment REST server starting on :%s", port)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("HTTP server error: %v", err)
		}
	}()

	// Graceful shutdown on SIGINT / SIGTERM
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Println("Shutting down payment service...")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := httpServer.Shutdown(ctx); err != nil {
		log.Printf("HTTP shutdown error: %v", err)
	}
	grpcServer.GracefulStop()
	log.Println("Payment service stopped.")
}
