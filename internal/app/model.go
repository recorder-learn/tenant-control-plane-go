package app

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"
)

type TenantStatus string

const (
	TenantProvisioning TenantStatus = "provisioning"
	TenantActive       TenantStatus = "active"
	TenantUpdating     TenantStatus = "updating"
	TenantDestroying   TenantStatus = "destroying"
	TenantDestroyed    TenantStatus = "destroyed"
	TenantFailed       TenantStatus = "failed"
)

type TaskType string

const (
	TaskDeploy  TaskType = "deploy"
	TaskUpdate  TaskType = "update"
	TaskDestroy TaskType = "destroy"
)

type TaskStatus string

const (
	TaskAccepted   TaskStatus = "accepted"
	TaskInProgress TaskStatus = "in_progress"
	TaskDone       TaskStatus = "done"
	TaskFailed     TaskStatus = "failed"
)

func (s TaskStatus) Terminal() bool {
	return s == TaskDone || s == TaskFailed
}

// tenantStatusAfter is the tenant outcome of a terminal task.
func tenantStatusAfter(taskType TaskType, outcome TaskStatus) TenantStatus {
	if outcome == TaskFailed {
		return TenantFailed
	}
	return rules[taskType].onDone
}

type Tenant struct {
	ID        uuid.UUID    `json:"id"`
	Slug      string       `json:"slug"`
	Name      string       `json:"name"`
	Status    TenantStatus `json:"status"`
	Version   int64        `json:"version"`
	CreatedAt Timestamp    `json:"created_at"`
	UpdatedAt Timestamp    `json:"updated_at"`
}

type Task struct {
	ID        uuid.UUID  `json:"id"`
	TenantID  uuid.UUID  `json:"tenant_id"`
	Type      TaskType   `json:"type"`
	Status    TaskStatus `json:"status"`
	Error     string     `json:"error,omitempty"`
	CreatedAt Timestamp  `json:"created_at"`
	UpdatedAt Timestamp  `json:"updated_at"`
}

// TaskEvent is the outbound message. The task IS the event, so only the fields
// the worker needs are carried.
type TaskEvent struct {
	ID       uuid.UUID  `json:"id"`
	Type     TaskType   `json:"type"`
	TenantID uuid.UUID  `json:"tenant_id"`
	Status   TaskStatus `json:"status"`
}

// TaskUpdateMessage is the inbound message from the worker. MessageID is the
// idempotency key and is stored on the task as last_message_id.
type TaskUpdateMessage struct {
	MessageID uuid.UUID  `json:"message_id"`
	TaskID    uuid.UUID  `json:"task_id"`
	Status    TaskStatus `json:"status"`
	Error     string     `json:"error,omitempty"`
}

func (u TaskUpdateMessage) Validate() error {
	if u.MessageID == uuid.Nil {
		return errors.New("message_id is required")
	}
	if u.TaskID == uuid.Nil {
		return errors.New("task_id is required")
	}
	switch u.Status {
	case TaskInProgress, TaskDone, TaskFailed:
		return nil
	default:
		return fmt.Errorf("invalid status %q", u.Status)
	}
}

type taskRule struct {
	allowedFrom []TenantStatus
	inFlight    TenantStatus
	onDone      TenantStatus
}

var rules = map[TaskType]taskRule{
	TaskDeploy: {
		allowedFrom: nil,
		inFlight:    TenantProvisioning,
		onDone:      TenantActive,
	},
	TaskUpdate: {
		allowedFrom: []TenantStatus{TenantActive},
		inFlight:    TenantUpdating,
		onDone:      TenantActive,
	},
	TaskDestroy: {
		allowedFrom: []TenantStatus{TenantActive, TenantFailed},
		inFlight:    TenantDestroying,
		onDone:      TenantDestroyed,
	},
}

func allowedFrom(t TaskType) []TenantStatus {
	return rules[t].allowedFrom
}

func inFlight(t TaskType) TenantStatus {
	return rules[t].inFlight
}

func canStart(t TaskType, status TenantStatus) bool {
	return slices.Contains(rules[t].allowedFrom, status)
}

func validTenantStatus(s TenantStatus) bool {
	switch s {
	case TenantProvisioning, TenantActive, TenantUpdating,
		TenantDestroying, TenantDestroyed, TenantFailed:
		return true
	}
	return false
}

func validTaskStatus(s TaskStatus) bool {
	switch s {
	case TaskAccepted, TaskInProgress, TaskDone, TaskFailed:
		return true
	}
	return false
}

var slugPattern = regexp.MustCompile(`^[a-z][-a-z0-9]{1,26}[a-z0-9]$`)

func validateSlug(slug string) error {
	if !slugPattern.MatchString(slug) {
		return validationError("slug must be 3-28 characters, start with a letter, end with a letter or digit, and use only lowercase letters, digits and hyphens")
	}
	return nil
}

func validateName(name string) error {
	if strings.TrimSpace(name) == "" {
		return validationError("name must not be empty")
	}
	return nil
}

// AppError is an error the HTTP layer can return to a client. Code is part of
// the published contract.
type AppError struct {
	Status  int
	Code    string
	Message string
}

func (e *AppError) Error() string {
	return e.Message
}

func newAppError(status int, code, format string, args ...any) *AppError {
	return &AppError{Status: status, Code: code, Message: fmt.Sprintf(format, args...)}
}

func tenantNotFound(id uuid.UUID) *AppError {
	return newAppError(404, "tenant_not_found", "tenant %s not found", id)
}

func taskNotFound(id uuid.UUID) *AppError {
	return newAppError(404, "task_not_found", "task %s not found", id)
}

func tenantAlreadyExists(slug string) *AppError {
	return newAppError(409, "tenant_already_exists", "slug %q is already taken", slug)
}

func tenantUpdateNotAllowed(taskType TaskType, status TenantStatus) *AppError {
	allowed := make([]string, len(rules[taskType].allowedFrom))
	for i, s := range rules[taskType].allowedFrom {
		allowed[i] = string(s)
	}
	return newAppError(409, "tenant_update_not_allowed",
		"cannot %s tenant in status %s; requires %s",
		taskType, status, strings.Join(allowed, " or "))
}

func tenantUpdateNotAllowedConcurrent() *AppError {
	return newAppError(409, "tenant_update_not_allowed", "tenant changed concurrently; retry")
}

func tenantVersionConflict(current, requested int64) *AppError {
	return newAppError(409, "tenant_version_conflict",
		"version %d is stale; current version is %d", requested, current)
}

func validationError(format string, args ...any) *AppError {
	return newAppError(400, "validation_error", format, args...)
}

// Timestamp renders ISO-8601 with milliseconds and a Z suffix, as the brief
// requires. Go's default time.Time JSON uses variable precision and +00:00.
type Timestamp time.Time

func (t Timestamp) MarshalJSON() ([]byte, error) {
	return []byte(`"` + time.Time(t).UTC().Format("2006-01-02T15:04:05.000Z") + `"`), nil
}

func (t *Timestamp) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	parsed, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return err
	}
	*t = Timestamp(parsed)
	return nil
}

func (t *Timestamp) Scan(v any) error {
	switch x := v.(type) {
	case time.Time:
		*t = Timestamp(x)
		return nil
	case nil:
		*t = Timestamp{}
		return nil
	default:
		return fmt.Errorf("cannot scan %T into Timestamp", v)
	}
}
