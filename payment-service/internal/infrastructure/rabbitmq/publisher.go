package rabbitmq

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"payment-service/internal/domain"
)

const (
	mainQueue   = "payment.completed"
	dlxName     = "payment.dlx"
	dlqName     = "payment.dead-letter"
)

// Publisher sends payment events to RabbitMQ.
type Publisher struct {
	conn *amqp.Connection
	ch   *amqp.Channel
}

func NewPublisher(url string) (*Publisher, error) {
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

	return &Publisher{conn: conn, ch: ch}, nil
}

// declareTopology sets up the DLX, DLQ, and main durable queue.
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

	// Main durable queue that routes failures to DLX
	_, err = ch.QueueDeclare(mainQueue, true, false, false, false, amqp.Table{
		"x-dead-letter-exchange": dlxName,
	})
	if err != nil {
		return fmt.Errorf("declare main queue: %w", err)
	}

	return nil
}

// PublishPaymentEvent marshals and publishes the event as a persistent message.
func (p *Publisher) PublishPaymentEvent(event domain.PaymentEvent) error {
	body, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	return p.ch.PublishWithContext(ctx,
		"",        // default exchange
		mainQueue, // routing key
		false,     // mandatory
		false,     // immediate
		amqp.Publishing{
			ContentType:  "application/json",
			DeliveryMode: amqp.Persistent, // survives broker restart
			MessageId:    event.EventID,
			Body:         body,
		},
	)
}

func (p *Publisher) Close() {
	p.ch.Close()
	p.conn.Close()
}
