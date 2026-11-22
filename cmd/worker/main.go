package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/aashishGithub/tenant-control-plane-go/internal/app"
	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	log := slog.New(slog.NewTextHandler(os.Stdout, nil))
	slog.SetDefault(log)
	if err := run(log); err != nil {
		log.Error("fatal", "error", err)
		os.Exit(1)
	}
}

type worker struct {
	broker    *app.Broker
	log       *slog.Logger
	failRate  float64
	minDelay  int
	maxDelay  int
	duplicate bool
	reorder   bool
}

func run(log *slog.Logger) error {
	failRate := flag.Float64("fail-rate", 0, "chance (0-1) that a task ends failed")
	minDelay := flag.Int("min-delay-ms", 200, "minimum simulated delay in milliseconds")
	maxDelay := flag.Int("max-delay-ms", 600, "maximum simulated delay in milliseconds")
	duplicate := flag.Bool("duplicate", false, "publish each terminal update twice")
	reorder := flag.Bool("reorder", false, "publish the outcome before in_progress")
	flag.Parse()

	if *failRate < 0 || *failRate > 1 {
		return fmt.Errorf("fail-rate must be between 0 and 1")
	}
	if *minDelay < 0 || *maxDelay < *minDelay {
		return fmt.Errorf("delays must satisfy 0 <= min-delay-ms <= max-delay-ms")
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	broker := app.NewBroker(env("AMQP_URL", "amqp://tenants:tenants@localhost:5672/"), log)
	defer broker.Close()
	if err := broker.Declare(); err != nil {
		return err
	}

	w := &worker{
		broker:    broker,
		log:       log,
		failRate:  *failRate,
		minDelay:  *minDelay,
		maxDelay:  *maxDelay,
		duplicate: *duplicate,
		reorder:   *reorder,
	}
	log.Info("worker ready",
		"fail_rate", w.failRate,
		"min_delay_ms", w.minDelay,
		"max_delay_ms", w.maxDelay,
		"duplicate", w.duplicate,
		"reorder", w.reorder)

	return broker.Consume(ctx, app.TasksQueue, 1, w.handle)
}

func (w *worker) handle(delivery amqp.Delivery) {
	defer func() {
		if r := recover(); r != nil {
			w.log.Error("worker panic", "panic", r)
			time.Sleep(time.Second)
			_ = delivery.Nack(false, true)
		}
	}()

	var event app.TaskEvent
	if err := json.Unmarshal(delivery.Body, &event); err != nil {
		w.log.Error("malformed task message", "error", err)
		_ = delivery.Nack(false, false)
		return
	}

	w.log.Info("task received", "task_id", event.ID, "type", event.Type, "tenant_id", event.TenantID)

	progress := app.TaskUpdateMessage{
		MessageID: uuid.New(),
		TaskID:    event.ID,
		Status:    app.TaskInProgress,
	}
	terminal := app.TaskUpdateMessage{
		MessageID: uuid.New(),
		TaskID:    event.ID,
		Status:    app.TaskDone,
	}
	if rand.Float64() < w.failRate {
		terminal.Status = app.TaskFailed
		terminal.Error = "simulated provisioning failure"
	}

	w.pause()
	if w.reorder {
		if err := w.publish(terminal); err != nil {
			w.requeue(delivery, err)
			return
		}
		if w.duplicate {
			if err := w.publish(terminal); err != nil {
				w.requeue(delivery, err)
				return
			}
		}
		if err := w.publish(progress); err != nil {
			w.requeue(delivery, err)
			return
		}
	} else {
		if err := w.publish(progress); err != nil {
			w.requeue(delivery, err)
			return
		}
		w.pause()
		if err := w.publish(terminal); err != nil {
			w.requeue(delivery, err)
			return
		}
		if w.duplicate {
			if err := w.publish(terminal); err != nil {
				w.requeue(delivery, err)
				return
			}
		}
	}

	w.log.Info("task finished", "task_id", event.ID, "status", terminal.Status)
	_ = delivery.Ack(false)
}

func (w *worker) publish(update app.TaskUpdateMessage) error {
	body, err := json.Marshal(update)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return w.broker.Publish(ctx, app.UpdatesQueue, update.MessageID.String(), body)
}

func (w *worker) requeue(delivery amqp.Delivery, err error) {
	w.log.Error("could not report progress; requeueing", "error", err)
	time.Sleep(time.Second)
	_ = delivery.Nack(false, true)
}

func (w *worker) pause() {
	span := w.maxDelay - w.minDelay
	delay := w.minDelay
	if span > 0 {
		delay += rand.IntN(span + 1)
	}
	time.Sleep(time.Duration(delay) * time.Millisecond)
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
