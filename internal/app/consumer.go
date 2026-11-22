package app

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Consumer struct {
	svc *Service
	log *slog.Logger
}

func NewConsumer(svc *Service, log *slog.Logger) *Consumer {
	return &Consumer{svc: svc, log: log}
}

// Handle never panics and always settles the delivery. Poison messages go to
// the dead-letter queue; a database blip is retried a few times first.
func (c *Consumer) Handle(delivery amqp.Delivery) {
	defer func() {
		if r := recover(); r != nil {
			c.log.Error("panic while applying update", "panic", r)
			_ = delivery.Nack(false, false)
		}
	}()

	var update TaskUpdateMessage
	if err := json.Unmarshal(delivery.Body, &update); err != nil {
		c.log.Error("poison update: invalid JSON", "error", err)
		_ = delivery.Nack(false, false)
		return
	}
	if err := update.Validate(); err != nil {
		c.log.Error("poison update: invalid message", "error", err)
		_ = delivery.Nack(false, false)
		return
	}

	var lastErr error
	for attempt := 1; attempt <= 3; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		outcome, err := c.svc.ApplyUpdate(ctx, update)
		cancel()

		if err == nil {
			c.log.Info("update settled",
				"outcome", outcome,
				"message_id", update.MessageID,
				"task_id", update.TaskID,
				"status", update.Status)
			_ = delivery.Ack(false)
			return
		}

		if errors.Is(err, ErrUnknownTask) {
			c.log.Error("poison update: unknown task", "task_id", update.TaskID)
			_ = delivery.Nack(false, false)
			return
		}

		lastErr = err
		c.log.Warn("update failed; retrying",
			"attempt", attempt, "task_id", update.TaskID, "error", err)
		if attempt < 3 {
			time.Sleep(time.Duration(attempt) * 200 * time.Millisecond)
		}
	}

	c.log.Error("update failed after retries; dead-lettering",
		"task_id", update.TaskID, "error", lastErr)
	_ = delivery.Nack(false, false)
}
