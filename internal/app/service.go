package app

import (
	"context"
	"errors"
	"log/slog"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// Outcome says what happened to an inbound update.
type Outcome string

const (
	OutcomeApplied Outcome = "applied"
	OutcomeStale   Outcome = "stale"
)

// ErrUnknownTask is permanent: retrying cannot help, so the consumer
// dead-letters the message.
var ErrUnknownTask = errors.New("unknown task")

type Service struct {
	store *Store
	log   *slog.Logger
}

func NewService(store *Store, log *slog.Logger) *Service {
	return &Service{store: store, log: log}
}

func (s *Service) Ping(ctx context.Context) error {
	return s.store.ping(ctx)
}

func (s *Service) CreateTenant(ctx context.Context, slug, name string) (Tenant, Task, error) {
	if err := validateSlug(slug); err != nil {
		return Tenant{}, Task{}, err
	}
	if err := validateName(name); err != nil {
		return Tenant{}, Task{}, err
	}

	tenant := Tenant{
		ID:      uuid.New(),
		Slug:    slug,
		Name:    name,
		Status:  TenantProvisioning,
		Version: 1,
	}
	task := Task{
		ID:       uuid.New(),
		TenantID: tenant.ID,
		Type:     TaskDeploy,
		Status:   TaskAccepted,
	}

	err := s.store.withTx(ctx, func(db DB) error {
		if err := insertTenant(ctx, db, &tenant); err != nil {
			return err
		}
		return insertTask(ctx, db, &task)
	})
	if err != nil {
		if isUniqueViolation(err) {
			return Tenant{}, Task{}, tenantAlreadyExists(slug)
		}
		return Tenant{}, Task{}, err
	}
	return tenant, task, nil
}

func (s *Service) UpdateTenant(ctx context.Context, id uuid.UUID, name string, version int64) (Tenant, Task, error) {
	if err := validateName(name); err != nil {
		return Tenant{}, Task{}, err
	}
	if version < 1 {
		return Tenant{}, Task{}, validationError("version must be a positive integer")
	}

	var tenant Tenant
	task := Task{
		ID:       uuid.New(),
		TenantID: id,
		Type:     TaskUpdate,
		Status:   TaskAccepted,
	}

	err := s.store.withTx(ctx, func(db DB) error {
		updated, ok, err := transitionTenant(
			ctx, db, id, inFlight(TaskUpdate), allowedFrom(TaskUpdate), &name, &version)
		if err != nil {
			return err
		}
		if !ok {
			return s.explainRejection(ctx, db, id, TaskUpdate, &version)
		}
		tenant = updated
		return insertTask(ctx, db, &task)
	})
	if err != nil {
		return Tenant{}, Task{}, err
	}
	return tenant, task, nil
}

func (s *Service) DeleteTenant(ctx context.Context, id uuid.UUID) (Tenant, Task, error) {
	var tenant Tenant
	task := Task{
		ID:       uuid.New(),
		TenantID: id,
		Type:     TaskDestroy,
		Status:   TaskAccepted,
	}

	err := s.store.withTx(ctx, func(db DB) error {
		updated, ok, err := transitionTenant(
			ctx, db, id, inFlight(TaskDestroy), allowedFrom(TaskDestroy), nil, nil)
		if err != nil {
			return err
		}
		if !ok {
			return s.explainRejection(ctx, db, id, TaskDestroy, nil)
		}
		tenant = updated
		return insertTask(ctx, db, &task)
	})
	if err != nil {
		return Tenant{}, Task{}, err
	}
	return tenant, task, nil
}

// explainRejection runs after a guarded UPDATE matched no row: re-read the
// tenant to say which guard failed.
func (s *Service) explainRejection(
	ctx context.Context,
	db DB,
	id uuid.UUID,
	taskType TaskType,
	expectedVersion *int64,
) error {
	current, err := getTenant(ctx, db, id)
	if errors.Is(err, pgx.ErrNoRows) {
		return tenantNotFound(id)
	}
	if err != nil {
		return err
	}
	if expectedVersion != nil && current.Version != *expectedVersion {
		return tenantVersionConflict(current.Version, *expectedVersion)
	}
	if !canStart(taskType, current.Status) {
		return tenantUpdateNotAllowed(taskType, current.Status)
	}
	return tenantUpdateNotAllowedConcurrent()
}

func (s *Service) GetTenant(ctx context.Context, id uuid.UUID) (Tenant, error) {
	tenant, err := getTenant(ctx, s.store.pool, id)
	if errors.Is(err, pgx.ErrNoRows) {
		return Tenant{}, tenantNotFound(id)
	}
	return tenant, err
}

func (s *Service) ListTenants(ctx context.Context, status string, limit, offset int) ([]Tenant, error) {
	if status != "" && !validTenantStatus(TenantStatus(status)) {
		return nil, validationError("invalid tenant status %q", status)
	}
	return listTenants(ctx, s.store.pool, status, limit, offset)
}

func (s *Service) GetTask(ctx context.Context, id uuid.UUID) (Task, error) {
	task, err := getTask(ctx, s.store.pool, id)
	if errors.Is(err, pgx.ErrNoRows) {
		return Task{}, taskNotFound(id)
	}
	return task, err
}

func (s *Service) ListTasks(ctx context.Context, tenantID *uuid.UUID, status string, limit, offset int) ([]Task, error) {
	if status != "" && !validTaskStatus(TaskStatus(status)) {
		return nil, validationError("invalid task status %q", status)
	}
	return listTasks(ctx, s.store.pool, tenantID, status, limit, offset)
}

// ApplyUpdate applies one worker update in one transaction. The same
// last_message_id or a task that is already in a later status makes the update
// a no-op. A terminal outcome moves the tenant in the same transaction.
func (s *Service) ApplyUpdate(ctx context.Context, update TaskUpdateMessage) (Outcome, error) {
	outcome := OutcomeApplied

	err := s.store.withTx(ctx, func(db DB) error {
		applied, ok, err := applyTaskUpdate(
			ctx, db, update.MessageID, update.TaskID, update.Status, update.Error)
		if err != nil {
			return err
		}

		if !ok {
			if _, err := getTask(ctx, db, update.TaskID); errors.Is(err, pgx.ErrNoRows) {
				return ErrUnknownTask
			} else if err != nil {
				return err
			}
			outcome = OutcomeStale
			return nil
		}

		if !update.Status.Terminal() {
			return nil
		}

		tenantStatus := tenantStatusAfter(applied.Type, update.Status)
		tenant, moved, err := transitionTenant(
			ctx, db, applied.TenantID, tenantStatus,
			[]TenantStatus{inFlight(applied.Type)}, nil, nil)
		if err != nil {
			return err
		}
		if !moved {
			s.log.Error("task finished but tenant was not in the expected status",
				"task_id", update.TaskID,
				"tenant_id", applied.TenantID,
				"expected", inFlight(applied.Type),
				"wanted", tenantStatus)
			return nil
		}
		s.log.Info("tenant moved",
			"tenant_id", tenant.ID,
			"status", tenant.Status,
			"version", tenant.Version)
		return nil
	})

	return outcome, err
}
