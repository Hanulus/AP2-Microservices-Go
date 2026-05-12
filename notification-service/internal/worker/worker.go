package worker

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/redis/go-redis/v9"
	"notification-service/internal/provider"
)

const idempotencyTTL = 24 * time.Hour

// NotificationJob holds everything needed to send one notification.
type NotificationJob struct {
	PaymentID     string
	OrderID       string
	CustomerEmail string
	Amount        int64
}

// Worker sends notifications reliably: idempotency via Redis + exponential backoff retries.
type Worker struct {
	sender     provider.EmailSender
	redis      *redis.Client
	maxRetries int
}

func NewWorker(sender provider.EmailSender, redisClient *redis.Client, maxRetries int) *Worker {
	return &Worker{sender: sender, redis: redisClient, maxRetries: maxRetries}
}

// Process attempts to send a notification for the given job.
// Returns nil if the notification was sent (or already processed before).
func (w *Worker) Process(job NotificationJob) error {
	ctx := context.Background()
	key := fmt.Sprintf("notif:sent:%s", job.PaymentID)

	// Idempotency check — skip if already processed
	exists, err := w.redis.Exists(ctx, key).Result()
	if err == nil && exists > 0 {
		log.Printf("[Worker] DUPLICATE skipped payment_id=%s", job.PaymentID)
		return nil
	}

	subject := fmt.Sprintf("Payment confirmed for order %s", job.OrderID)
	body := fmt.Sprintf("Your payment of $%.2f has been processed.", float64(job.Amount)/100.0)

	// Retry loop with exponential backoff: 2s, 4s, 8s, ...
	delay := 2 * time.Second
	for attempt := 1; attempt <= w.maxRetries; attempt++ {
		err := w.sender.Send(job.CustomerEmail, subject, body)
		if err == nil {
			// Mark as processed so duplicate jobs are ignored
			w.redis.Set(ctx, key, "sent", idempotencyTTL)
			log.Printf("[Worker] SUCCESS payment_id=%s attempt=%d", job.PaymentID, attempt)
			return nil
		}

		log.Printf("[Worker] FAIL payment_id=%s attempt=%d/%d err=%v, retrying in %s",
			job.PaymentID, attempt, w.maxRetries, err, delay)

		if attempt < w.maxRetries {
			time.Sleep(delay)
			delay *= 2 // exponential backoff
		}
	}

	return fmt.Errorf("failed to send notification for payment %s after %d attempts", job.PaymentID, w.maxRetries)
}
