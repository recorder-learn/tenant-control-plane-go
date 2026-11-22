package app

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed schema.sql
var schemaSQL string

// DB is the query surface shared by a pool and a transaction.
type DB interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

type Store struct {
	pool *pgxpool.Pool
}

func Open(ctx context.Context, databaseURL string) (*Store, error) {
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return nil, fmt.Errorf("connect postgres: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}
	if err := applySchema(ctx, pool); err != nil {
		pool.Close()
		return nil, err
	}
	return &Store{pool: pool}, nil
}

// applySchema sends each DDL statement separately: the extended protocol only
// accepts one command per Exec.
func applySchema(ctx context.Context, pool *pgxpool.Pool) error {
	for _, stmt := range strings.Split(schemaSQL, ";") {
		if strings.TrimSpace(stmt) == "" {
			continue
		}
		if _, err := pool.Exec(ctx, stmt); err != nil {
			return fmt.Errorf("apply schema: %w", err)
		}
	}
	return nil
}

func (s *Store) Close() {
	s.pool.Close()
}

func (s *Store) ping(ctx context.Context) error {
	return s.pool.Ping(ctx)
}

func (s *Store) withTx(ctx context.Context, fn func(DB) error) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if err := fn(tx); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}

func insertTenant(ctx context.Context, db DB, t *Tenant) error {
	return db.QueryRow(ctx, `
		INSERT INTO tenants (id, slug, name, status, version)
		VALUES ($1, $2, $3, $4, $5)
		RETURNING created_at, updated_at`,
		t.ID, t.Slug, t.Name, string(t.Status), t.Version,
	).Scan(&t.CreatedAt, &t.UpdatedAt)
}

type rowScanner interface {
	Scan(dest ...any) error
}

func scanTenant(row rowScanner) (Tenant, error) {
	var t Tenant
	var status string
	err := row.Scan(&t.ID, &t.Slug, &t.Name, &status, &t.Version, &t.CreatedAt, &t.UpdatedAt)
	t.Status = TenantStatus(status)
	return t, err
}

func getTenant(ctx context.Context, db DB, id uuid.UUID) (Tenant, error) {
	row := db.QueryRow(ctx, `
		SELECT id, slug, name, status, version, created_at, updated_at
		FROM tenants
		WHERE id = $1`, id)
	return scanTenant(row)
}

func listTenants(ctx context.Context, db DB, status string, limit, offset int) ([]Tenant, error) {
	rows, err := db.Query(ctx, `
		SELECT id, slug, name, status, version, created_at, updated_at
		FROM tenants
		WHERE ($1 = '' OR status = $1::text)
		ORDER BY created_at DESC, id DESC
		LIMIT $2 OFFSET $3`, status, limit, offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	items := make([]Tenant, 0)
	for rows.Next() {
		t, err := scanTenant(rows)
		if err != nil {
			return nil, err
		}
		items = append(items, t)
	}
	return items, rows.Err()
}

// transitionTenant is the only way an existing tenant moves. The status list
// and the optional version are guards evaluated by one UPDATE, so concurrent
// callers cannot both win.
func transitionTenant(
	ctx context.Context,
	db DB,
	id uuid.UUID,
	newStatus TenantStatus,
	allowed []TenantStatus,
	name *string,
	expectedVersion *int64,
) (Tenant, bool, error) {
	allowedStrings := make([]string, len(allowed))
	for i, s := range allowed {
		allowedStrings[i] = string(s)
	}

	row := db.QueryRow(ctx, `
		UPDATE tenants
		SET status = $2,
		    name = COALESCE($3::text, name),
		    version = version + 1,
		    updated_at = now()
		WHERE id = $1
		  AND status = ANY($4::text[])
		  AND ($5::bigint IS NULL OR version = $5::bigint)
		RETURNING id, slug, name, status, version, created_at, updated_at`,
		id, string(newStatus), name, allowedStrings, expectedVersion)

	t, err := scanTenant(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return Tenant{}, false, nil
	}
	if err != nil {
		return Tenant{}, false, err
	}
	return t, true, nil
}

func insertTask(ctx context.Context, db DB, t *Task) error {
	return db.QueryRow(ctx, `
		INSERT INTO tasks (id, tenant_id, type, status)
		VALUES ($1, $2, $3, $4)
		RETURNING created_at, updated_at`,
		t.ID, t.TenantID, string(t.Type), string(t.Status),
	).Scan(&t.CreatedAt, &t.UpdatedAt)
}

func scanTask(row rowScanner) (Task, error) {
	var t Task
	var taskType, status string
	err := row.Scan(&t.ID, &t.TenantID, &taskType, &status, &t.Error, &t.CreatedAt, &t.UpdatedAt)
	t.Type = TaskType(taskType)
	t.Status = TaskStatus(status)
	return t, err
}

func getTask(ctx context.Context, db DB, id uuid.UUID) (Task, error) {
	row := db.QueryRow(ctx, `
		SELECT id, tenant_id, type, status, COALESCE(error, ''), created_at, updated_at
		FROM tasks
		WHERE id = $1`, id)
	return scanTask(row)
}

func listTasks(ctx context.Context, db DB, tenantID *uuid.UUID, status string, limit, offset int) ([]Task, error) {
	rows, err := db.Query(ctx, `
		SELECT id, tenant_id, type, status, COALESCE(error, ''), created_at, updated_at
		FROM tasks
		WHERE ($1::uuid IS NULL OR tenant_id = $1::uuid)
		  AND ($2 = '' OR status = $2::text)
		ORDER BY created_at DESC, id DESC
		LIMIT $3 OFFSET $4`, tenantID, status, limit, offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	items := make([]Task, 0)
	for rows.Next() {
		t, err := scanTask(rows)
		if err != nil {
			return nil, err
		}
		items = append(items, t)
	}
	return items, rows.Err()
}

type OutboxTask struct {
	ID       uuid.UUID
	TenantID uuid.UUID
	Type     TaskType
	Status   TaskStatus
}

func claimUnpublished(ctx context.Context, db DB, limit int) ([]OutboxTask, error) {
	rows, err := db.Query(ctx, `
		SELECT id, tenant_id, type, status
		FROM tasks
		WHERE published_at IS NULL
		ORDER BY id
		LIMIT $1
		FOR UPDATE SKIP LOCKED`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	items := make([]OutboxTask, 0)
	for rows.Next() {
		var t OutboxTask
		var taskType, status string
		if err := rows.Scan(&t.ID, &t.TenantID, &taskType, &status); err != nil {
			return nil, err
		}
		t.Type = TaskType(taskType)
		t.Status = TaskStatus(status)
		items = append(items, t)
	}
	return items, rows.Err()
}

func markPublished(ctx context.Context, db DB, id uuid.UUID) error {
	_, err := db.Exec(ctx, `UPDATE tasks SET published_at = now() WHERE id = $1`, id)
	return err
}

type appliedTask struct {
	TenantID uuid.UUID
	Type     TaskType
}

// applyTaskUpdate encodes the task state machine and the idempotency key in one
// statement. Zero rows means the task does not exist, or the update is a
// duplicate/stale one.
func applyTaskUpdate(
	ctx context.Context,
	db DB,
	messageID, taskID uuid.UUID,
	status TaskStatus,
	errText string,
) (appliedTask, bool, error) {
	var out appliedTask
	var taskType string

	err := db.QueryRow(ctx, `
		UPDATE tasks
		SET status = $2,
		    error = NULLIF($3::text, ''),
		    last_message_id = $4,
		    updated_at = now()
		WHERE id = $1
		  AND (last_message_id IS NULL OR last_message_id <> $4)
		  AND (
		    ($2 = 'in_progress' AND status = 'accepted')
		    OR ($2 IN ('done','failed') AND status IN ('accepted','in_progress'))
		  )
		RETURNING tenant_id, type`,
		taskID, string(status), errText, messageID,
	).Scan(&out.TenantID, &taskType)

	if errors.Is(err, pgx.ErrNoRows) {
		return appliedTask{}, false, nil
	}
	if err != nil {
		return appliedTask{}, false, err
	}
	out.Type = TaskType(taskType)
	return out, true, nil
}
