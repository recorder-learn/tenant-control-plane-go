package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/aashishGithub/tenant-control-plane-go/internal/app"
)

func main() {
	log := slog.New(slog.NewTextHandler(os.Stdout, nil))
	slog.SetDefault(log)
	if err := run(log); err != nil {
		log.Error("fatal", "error", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	store, err := app.Open(ctx, env("DATABASE_URL", "postgres://tenants:tenants@localhost:5432/tenants?sslmode=disable"))
	if err != nil {
		return err
	}
	defer store.Close()

	broker := app.NewBroker(env("AMQP_URL", "amqp://tenants:tenants@localhost:5672/"), log)
	defer broker.Close()
	if err := broker.Declare(); err != nil {
		return err
	}

	svc := app.NewService(store, log)
	server := &http.Server{
		Addr:              env("HTTP_ADDR", ":8080"),
		Handler:           app.NewAPI(svc, log).Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	var wg sync.WaitGroup
	wg.Add(2)

	relay := app.NewRelay(store, broker, log)
	go func() {
		defer wg.Done()
		relay.Run(ctx)
	}()

	consumer := app.NewConsumer(svc, log)
	go func() {
		defer wg.Done()
		if err := broker.Consume(ctx, app.UpdatesQueue, 4, consumer.Handle); err != nil {
			log.Error("consumer stopped", "error", err)
		}
	}()

	go func() {
		log.Info("http listening", "addr", server.Addr)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("http server failed", "error", err)
			stop()
		}
	}()

	<-ctx.Done()
	log.Info("shutting down")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = server.Shutdown(shutdownCtx)
	wg.Wait()
	return nil
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
