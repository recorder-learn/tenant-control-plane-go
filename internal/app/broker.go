package app

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	TasksQueue   = "worker.tasks"
	UpdatesQueue = "controlplane.task-updates"
	UpdatesDLQ   = "controlplane.task-updates.dlq"
)

// Broker owns one publishing connection and creates separate consuming
// connections. Everything uses the default exchange, so there are no
// exchanges or bindings to declare: just three queues.
type Broker struct {
	url string
	log *slog.Logger

	mu   sync.Mutex
	conn *amqp.Connection
	pub  *amqp.Channel
}

func NewBroker(url string, log *slog.Logger) *Broker {
	return &Broker{url: url, log: log}
}

func (b *Broker) Close() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.resetLocked()
}

func (b *Broker) resetLocked() {
	if b.pub != nil {
		_ = b.pub.Close()
		b.pub = nil
	}
	if b.conn != nil {
		_ = b.conn.Close()
		b.conn = nil
	}
}

func (b *Broker) ensureLocked() error {
	if b.conn != nil && !b.conn.IsClosed() && b.pub != nil {
		return nil
	}
	b.resetLocked()

	conn, err := amqp.Dial(b.url)
	if err != nil {
		return fmt.Errorf("dial rabbitmq: %w", err)
	}
	ch, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return fmt.Errorf("open rabbitmq channel: %w", err)
	}
	if err := ch.Confirm(false); err != nil {
		_ = ch.Close()
		_ = conn.Close()
		return fmt.Errorf("enable publisher confirms: %w", err)
	}
	b.conn = conn
	b.pub = ch
	return nil
}

func (b *Broker) Declare() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if err := b.ensureLocked(); err != nil {
		return err
	}
	if err := declareQueues(b.pub); err != nil {
		b.resetLocked()
		return err
	}
	return nil
}

func declareQueues(ch *amqp.Channel) error {
	if _, err := ch.QueueDeclare(TasksQueue, true, false, false, false, nil); err != nil {
		return fmt.Errorf("declare %s: %w", TasksQueue, err)
	}
	if _, err := ch.QueueDeclare(UpdatesDLQ, true, false, false, false, nil); err != nil {
		return fmt.Errorf("declare %s: %w", UpdatesDLQ, err)
	}
	if _, err := ch.QueueDeclare(UpdatesQueue, true, false, false, false, amqp.Table{
		"x-dead-letter-exchange":    "",
		"x-dead-letter-routing-key": UpdatesDLQ,
	}); err != nil {
		return fmt.Errorf("declare %s: %w", UpdatesQueue, err)
	}
	return nil
}

// Publish sends one persistent message and waits for the broker confirm.
// The mutex makes one channel safe for the relay and the worker.
func (b *Broker) Publish(ctx context.Context, queue, messageID string, body []byte) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if err := b.ensureLocked(); err != nil {
		return err
	}

	dc, err := b.pub.PublishWithDeferredConfirmWithContext(ctx, "", queue, false, false, amqp.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp.Persistent,
		MessageId:    messageID,
		Timestamp:    time.Now().UTC(),
		Body:         body,
	})
	if err != nil {
		b.resetLocked()
		return fmt.Errorf("publish to %s: %w", queue, err)
	}

	ack, err := dc.WaitContext(ctx)
	if err != nil {
		b.resetLocked()
		return fmt.Errorf("wait for confirm: %w", err)
	}
	if !ack {
		return fmt.Errorf("broker rejected message %s", messageID)
	}
	return nil
}

// Consume runs until the context is cancelled. A lost connection is retried
// with a fixed pause; the queues are declared again on the new channel.
func (b *Broker) Consume(ctx context.Context, queue string, prefetch int, handle func(amqp.Delivery)) error {
	for {
		if err := ctx.Err(); err != nil {
			return nil
		}

		err := b.consumeOnce(ctx, queue, prefetch, handle)
		if err != nil && ctx.Err() == nil {
			b.log.Error("consumer connection failed", "queue", queue, "error", err)
		}

		select {
		case <-ctx.Done():
			return nil
		case <-time.After(2 * time.Second):
		}
	}
}

func (b *Broker) consumeOnce(ctx context.Context, queue string, prefetch int, handle func(amqp.Delivery)) error {
	conn, err := amqp.Dial(b.url)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()

	ch, err := conn.Channel()
	if err != nil {
		return err
	}
	defer func() { _ = ch.Close() }()

	if err := declareQueues(ch); err != nil {
		return err
	}
	if err := ch.Qos(prefetch, 0, false); err != nil {
		return err
	}

	deliveries, err := ch.Consume(queue, "", false, false, false, false, nil)
	if err != nil {
		return err
	}
	b.log.Info("consuming", "queue", queue)

	closed := conn.NotifyClose(make(chan *amqp.Error, 1))
	for {
		select {
		case <-ctx.Done():
			return nil
		case err, ok := <-closed:
			if ok && err != nil {
				return err
			}
			return fmt.Errorf("connection closed")
		case delivery, ok := <-deliveries:
			if !ok {
				return fmt.Errorf("delivery channel closed")
			}
			handle(delivery)
		}
	}
}

type Relay struct {
	store  *Store
	broker *Broker
	log    *slog.Logger
}

func NewRelay(store *Store, broker *Broker, log *slog.Logger) *Relay {
	return &Relay{store: store, broker: broker, log: log}
}

func (r *Relay) Run(ctx context.Context) {
	if err := r.publishBatch(ctx); err != nil && ctx.Err() == nil {
		r.log.Error("outbox batch failed", "error", err)
	}

	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := r.publishBatch(ctx); err != nil && ctx.Err() == nil {
				r.log.Error("outbox batch failed", "error", err)
			}
		}
	}
}

// publishBatch claims unpublished tasks in a transaction, publishes each with
// a confirm, and marks it. A failure rolls the batch back, so nothing is lost,
// only possibly published twice (at-least-once).
func (r *Relay) publishBatch(ctx context.Context) error {
	return r.store.withTx(ctx, func(db DB) error {
		tasks, err := claimUnpublished(ctx, db, 20)
		if err != nil {
			return err
		}

		for _, task := range tasks {
			body, err := json.Marshal(TaskEvent{
				ID:       task.ID,
				Type:     task.Type,
				TenantID: task.TenantID,
				Status:   task.Status,
			})
			if err != nil {
				return err
			}
			if err := r.broker.Publish(ctx, TasksQueue, task.ID.String(), body); err != nil {
				return err
			}
			if err := markPublished(ctx, db, task.ID); err != nil {
				return err
			}
		}
		return nil
	})
}
