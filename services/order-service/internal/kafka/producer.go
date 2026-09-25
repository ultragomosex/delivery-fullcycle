package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/segmentio/kafka-go"
)

type OrderItemEvent struct {
	ProductID    int64   `json:"product_id"`
	ProductTitle string  `json:"product_title"`
	Quantity     int     `json:"quantity"`
	UnitPrice    float64 `json:"unit_price"`
	TotalPrice   float64 `json:"total_price"`
}

type OrderCreatedEvent struct {
	OrderID    int64            `json:"order_id"`
	UserID     int64            `json:"user_id"`
	TotalPrice float64          `json:"total_price"`
	Status     string           `json:"status"`
	Items      []OrderItemEvent `json:"items"`
	CreatedAt  time.Time        `json:"created_at"`
}

type Producer struct {
	writer *kafka.Writer
	logger *slog.Logger
}

func NewProducer(brokers []string, topic string, logger *slog.Logger) *Producer {
	writer := &kafka.Writer{
		Addr:                   kafka.TCP(brokers...),
		Topic:                  topic,
		Balancer:               &kafka.LeastBytes{},
		BatchTimeout:           10 * time.Millisecond,
		RequiredAcks:           kafka.RequireOne,
		AllowAutoTopicCreation: true,
	}

	return &Producer{
		writer: writer,
		logger: logger,
	}
}

func (p *Producer) PublishOrderCreated(ctx context.Context, event *OrderCreatedEvent) error {
	payload, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal order event: %w", err)
	}

	msg := kafka.Message{
		Key:   []byte(strconv.FormatInt(event.OrderID, 10)),
		Value: payload,
		Time:  time.Now().UTC(),
	}

	if err := p.writer.WriteMessages(ctx, msg); err != nil {
		p.logger.Error("failed to publish kafka order event",
			slog.Int64("order_id", event.OrderID),
			slog.String("error", err.Error()),
		)
		return fmt.Errorf("write kafka message: %w", err)
	}

	p.logger.Info("published order event to kafka",
		slog.Int64("order_id", event.OrderID),
		slog.Float64("total", event.TotalPrice),
	)

	return nil
}

func (p *Producer) Close() error {
	return p.writer.Close()
}
