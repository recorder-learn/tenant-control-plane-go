package app

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestSlugValidation(t *testing.T) {
	valid := []string{
		"abc",
		"a-b",
		"acme",
		"it-0123456789abcdef",
		strings.Repeat("a", 28),
	}
	for _, slug := range valid {
		if err := validateSlug(slug); err != nil {
			t.Errorf("slug %q should be valid: %v", slug, err)
		}
	}

	invalid := []string{
		"",
		"ab",
		"1abc",
		"Abc",
		"abc-",
		"abc_",
		"-abc",
		strings.Repeat("a", 29),
	}
	for _, slug := range invalid {
		if err := validateSlug(slug); err == nil {
			t.Errorf("slug %q should be invalid", slug)
		}
	}
}

func TestTenantRules(t *testing.T) {
	tests := []struct {
		taskType TaskType
		status   TenantStatus
		want     bool
	}{
		{TaskUpdate, TenantActive, true},
		{TaskUpdate, TenantUpdating, false},
		{TaskUpdate, TenantFailed, false},
		{TaskDestroy, TenantActive, true},
		{TaskDestroy, TenantFailed, true},
		{TaskDestroy, TenantDestroyed, false},
		{TaskDeploy, TenantActive, false},
	}
	for _, tt := range tests {
		if got := canStart(tt.taskType, tt.status); got != tt.want {
			t.Errorf("canStart(%s, %s) = %v, want %v", tt.taskType, tt.status, got, tt.want)
		}
	}

	outcomes := []struct {
		taskType TaskType
		outcome  TaskStatus
		want     TenantStatus
	}{
		{TaskDeploy, TaskDone, TenantActive},
		{TaskDeploy, TaskFailed, TenantFailed},
		{TaskUpdate, TaskDone, TenantActive},
		{TaskUpdate, TaskFailed, TenantFailed},
		{TaskDestroy, TaskDone, TenantDestroyed},
		{TaskDestroy, TaskFailed, TenantFailed},
	}
	for _, tt := range outcomes {
		if got := tenantStatusAfter(tt.taskType, tt.outcome); got != tt.want {
			t.Errorf("tenantStatusAfter(%s, %s) = %s, want %s",
				tt.taskType, tt.outcome, got, tt.want)
		}
	}
}

func TestTaskUpdateValidation(t *testing.T) {
	valid := TaskUpdateMessage{
		MessageID: uuid.New(),
		TaskID:    uuid.New(),
		Status:    TaskDone,
	}
	if err := valid.Validate(); err != nil {
		t.Fatalf("valid update rejected: %v", err)
	}

	bad := []TaskUpdateMessage{
		{TaskID: uuid.New(), Status: TaskDone},
		{MessageID: uuid.New(), Status: TaskDone},
		{MessageID: uuid.New(), TaskID: uuid.New(), Status: TaskAccepted},
		{MessageID: uuid.New(), TaskID: uuid.New(), Status: "unknown"},
	}
	for _, update := range bad {
		if err := update.Validate(); err == nil {
			t.Errorf("update %+v should be invalid", update)
		}
	}
}

func TestTimestampJSON(t *testing.T) {
	ts := Timestamp(time.Date(2026, 1, 2, 3, 4, 5, 678000000, time.UTC))
	body, err := json.Marshal(ts)
	if err != nil {
		t.Fatal(err)
	}
	want := `"2026-01-02T03:04:05.678Z"`
	if string(body) != want {
		t.Fatalf("got %s, want %s", body, want)
	}

	var back Timestamp
	if err := json.Unmarshal(body, &back); err != nil {
		t.Fatalf("decode timestamp: %v", err)
	}
	if !time.Time(back).Equal(time.Time(ts)) {
		t.Fatalf("round trip: got %v, want %v", time.Time(back), time.Time(ts))
	}
}
