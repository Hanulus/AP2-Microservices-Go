package consumer

import (
	"encoding/json"
	"fmt"
	"log"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	mainQueue = "payment.completed"
	dlxName   = "payment.dlx"
	dlqName   = "payment.dead-letter"
	maxRetries = 3
)

// PaymentEvent mirrors the producer's event struct.
type PaymentEvent struct {
	EventID       string `json:"event_id"`
	OrderID       string `json:"order_id"`
	Amount        int64  `json:"amount"`
	CustomerEmail string `json:"customer_email"`
	Status        string `json:"status"`
}

// Consumer listens for payment events and simulates email notifications.
type Consumer struct {
	conn *amqp.Connection
	ch   *amqp.Channel

	mu           sync.Mutex
	processedIDs map[string]bool  // idempotency store
	retryCounts  map[string]int   // DLQ retry tracking
}

func NewConsumer(url string) (*Consumer, error) {
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, fmt.Errorf("rabbitmq connect: %w", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("rabbitmq channel: %w", err)
	}

	if err := declareTopology(ch); err != nil {
		ch.Close()
		conn.Close()
		return nil, err
	}

	// Prefetch 1: process one message at a time, fair dispatch
	if err := ch.Qos(1, 0, false); err != nil {
		ch.Close()
		conn.Close()
		return nil, fmt.Errorf("rabbitmq qos: %w", err)
	}

	return &Consumer{
		conn:         conn,
		ch:           ch,
		processedIDs: make(map[string]bool),
		retryCounts:  make(map[string]int),
	}, nil
}

func declareTopology(ch *amqp.Channel) error {
	// Dead-letter exchange
	if err := ch.ExchangeDeclare(dlxName, "direct", true, false, false, false, nil); err != nil {
		return fmt.Errorf("declare dlx: %w", err)
	}

	// Dead-letter queue
	_, err := ch.QueueDeclare(dlqName, true, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("declare dlq: %w", err)
	}
	if err := ch.QueueBind(dlqName, mainQueue, dlxName, false, nil); err != nil {
		return fmt.Errorf("bind dlq: %w", err)
	}

	// Main durable queue with DLX configured
	_, err = ch.QueueDeclare(mainQueue, true, false, false, false, amqp.Table{
		"x-dead-letter-exchange": dlxName,
	})
	if err != nil {
		return fmt.Errorf("declare main queue: %w", err)
	}

	return nil
}

// Start begins consuming messages. Blocks until the channel is closed.
func (c *Consumer) Start() error {
	msgs, err := c.ch.Consume(
		mainQueue,
		"",    // auto-generated consumer tag
		false, // auto-ack: DISABLED — manual ACK only
		false, // exclusive
		false, // no-local
		false, // no-wait
		nil,
	)
	if err != nil {
		return fmt.Errorf("rabbitmq consume: %w", err)
	}

	log.Println("[Notification] Consumer started, waiting for payment events...")
	for msg := range msgs {
		c.handle(msg)
	}
	return nil
}

func (c *Consumer) handle(msg amqp.Delivery) {
	var event PaymentEvent
	if err := json.Unmarshal(msg.Body, &event); err != nil {
		log.Printf("[Notification] ERROR: unparseable message, sending to DLQ: %v", err)
		msg.Nack(false, false) // reject without requeue → DLQ
		return
	}

	// --- Idempotency check ---
	c.mu.Lock()
	alreadyProcessed := c.processedIDs[event.EventID]
	if !alreadyProcessed {
		// Mark before processing so a crash/retry doesn't double-process
		c.processedIDs[event.EventID] = true
	}
	c.mu.Unlock()

	if alreadyProcessed {
		log.Printf("[Notification] DUPLICATE skipped: event_id=%s order_id=%s", event.EventID, event.OrderID)
		msg.Ack(false) // ACK to remove duplicate from queue
		return
	}

	// --- DLQ demo: simulate a permanent processing error ---
	// Any event whose OrderID starts with "fail-" will be rejected after maxRetries.
	if shouldFail(event.OrderID) {
		c.mu.Lock()
		c.retryCounts[event.EventID]++
		attempts := c.retryCounts[event.EventID]
		c.mu.Unlock()

		if attempts < maxRetries {
			log.Printf("[Notification] TRANSIENT ERROR (attempt %d/%d) for order %s, requeuing...",
				attempts, maxRetries, event.OrderID)
			// Remove from idempotency store so the retry is accepted
			c.mu.Lock()
			delete(c.processedIDs, event.EventID)
			c.mu.Unlock()
			msg.Nack(false, true) // requeue
		} else {
			log.Printf("[Notification] PERMANENT ERROR after %d attempts for order %s → moving to DLQ",
				attempts, event.OrderID)
			msg.Nack(false, false) // reject without requeue → DLQ
		}
		return
	}

	// --- Normal processing: simulate sending email ---
	log.Printf("[Notification] Sent email to %s for Order #%s. Amount: $%.2f",
		event.CustomerEmail, event.OrderID, float64(event.Amount)/100.0)

	// Manual ACK — only after successful log (at-least-once delivery guarantee)
	msg.Ack(false)
}

func shouldFail(orderID string) bool {
	return len(orderID) >= 5 && orderID[:5] == "fail-"
}

func (c *Consumer) Close() {
	c.ch.Close()
	c.conn.Close()
}
