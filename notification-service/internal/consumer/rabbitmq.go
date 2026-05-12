package consumer

import (
	"encoding/json"
	"fmt"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
	"notification-service/internal/worker"
)

const (
	mainQueue = "payment.completed"
	dlxName   = "payment.dlx"
	dlqName   = "payment.dead-letter"
)

// PaymentEvent mirrors the producer's event struct.
type PaymentEvent struct {
	EventID       string `json:"event_id"`
	OrderID       string `json:"order_id"`
	Amount        int64  `json:"amount"`
	CustomerEmail string `json:"customer_email"`
	Status        string `json:"status"`
}

// Consumer listens for payment events and hands each one to the Worker.
type Consumer struct {
	conn   *amqp.Connection
	ch     *amqp.Channel
	worker *worker.Worker
}

func NewConsumer(url string, w *worker.Worker) (*Consumer, error) {
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

	// Process one message at a time (fair dispatch)
	if err := ch.Qos(1, 0, false); err != nil {
		ch.Close()
		conn.Close()
		return nil, fmt.Errorf("rabbitmq qos: %w", err)
	}

	return &Consumer{conn: conn, ch: ch, worker: w}, nil
}

func declareTopology(ch *amqp.Channel) error {
	if err := ch.ExchangeDeclare(dlxName, "direct", true, false, false, false, nil); err != nil {
		return fmt.Errorf("declare dlx: %w", err)
	}
	if _, err := ch.QueueDeclare(dlqName, true, false, false, false, nil); err != nil {
		return fmt.Errorf("declare dlq: %w", err)
	}
	if err := ch.QueueBind(dlqName, mainQueue, dlxName, false, nil); err != nil {
		return fmt.Errorf("bind dlq: %w", err)
	}
	if _, err := ch.QueueDeclare(mainQueue, true, false, false, false, amqp.Table{
		"x-dead-letter-exchange": dlxName,
	}); err != nil {
		return fmt.Errorf("declare main queue: %w", err)
	}
	return nil
}

// Start begins consuming messages. Blocks until the channel is closed.
func (c *Consumer) Start() error {
	msgs, err := c.ch.Consume(mainQueue, "", false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("rabbitmq consume: %w", err)
	}

	log.Println("[Consumer] Started, waiting for payment events...")
	for msg := range msgs {
		c.handle(msg)
	}
	return nil
}

func (c *Consumer) handle(msg amqp.Delivery) {
	var event PaymentEvent
	if err := json.Unmarshal(msg.Body, &event); err != nil {
		log.Printf("[Consumer] Unparseable message → DLQ: %v", err)
		msg.Nack(false, false)
		return
	}

	job := worker.NotificationJob{
		PaymentID:     event.EventID,
		OrderID:       event.OrderID,
		CustomerEmail: event.CustomerEmail,
		Amount:        event.Amount,
	}

	// Worker handles idempotency, retries, and backoff internally
	if err := c.worker.Process(job); err != nil {
		log.Printf("[Consumer] Worker failed permanently for payment %s → DLQ: %v", event.EventID, err)
		msg.Nack(false, false) // send to DLQ after all retries exhausted
		return
	}

	msg.Ack(false)
}

func (c *Consumer) Close() {
	c.ch.Close()
	c.conn.Close()
}
