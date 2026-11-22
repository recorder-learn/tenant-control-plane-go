package app

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
)

var apiBase = os.Getenv("API_URL")

func requireStack(t *testing.T) {
	t.Helper()
	if apiBase == "" {
		t.Skip("set API_URL; run docker compose --profile tools run --rm --build check")
	}
}

func requireBroker(t *testing.T) {
	t.Helper()
	requireStack(t)
	if os.Getenv("AMQP_URL") == "" {
		t.Skip("set AMQP_URL to publish hand-made updates")
	}
}

func newSlug() string {
	return "it-" + strings.ReplaceAll(uuid.New().String(), "-", "")[:16]
}

func do(t *testing.T, method, path string, body, out any) int {
	t.Helper()

	var reader io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		reader = bytes.NewReader(raw)
	}

	req, err := http.NewRequest(method, apiBase+path, reader)
	if err != nil {
		t.Fatal(err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	client := &http.Client{Timeout: 15 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()

	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if out != nil && len(raw) > 0 {
		if err := json.Unmarshal(raw, out); err != nil {
			t.Fatalf("decode %s: %v; body=%s", path, err, raw)
		}
	}
	return resp.StatusCode
}

func createTenant(t *testing.T, slug string) (Tenant, Task) {
	t.Helper()
	var out tenantTaskResponse
	code := do(t, http.MethodPost, "/v1/tenants",
		map[string]string{"slug": slug, "name": "Test"}, &out)
	if code != http.StatusAccepted {
		t.Fatalf("create %s: status %d", slug, code)
	}
	return out.Tenant, out.Task
}

func fetchTenant(t *testing.T, id uuid.UUID) Tenant {
	t.Helper()
	var tenant Tenant
	code := do(t, http.MethodGet, "/v1/tenants/"+id.String(), nil, &tenant)
	if code != http.StatusOK {
		t.Fatalf("get tenant %s: status %d", id, code)
	}
	return tenant
}

func waitStatus(t *testing.T, id uuid.UUID, want TenantStatus) Tenant {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		tenant := fetchTenant(t, id)
		if tenant.Status == want {
			return tenant
		}
		if time.Now().After(deadline) {
			t.Fatalf("tenant %s: want %s, last %s", id, want, tenant.Status)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func tasksOf(t *testing.T, tenantID uuid.UUID) []Task {
	t.Helper()
	var out taskListResponse
	code := do(t, http.MethodGet, "/v1/tasks?tenant_id="+tenantID.String()+"&limit=100", nil, &out)
	if code != http.StatusOK {
		t.Fatalf("list tasks: status %d", code)
	}
	return out.Items
}

func publishBytes(t *testing.T, body []byte) {
	t.Helper()
	conn, err := amqp.Dial(os.Getenv("AMQP_URL"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()

	ch, err := conn.Channel()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ch.Close() }()

	if err := ch.Confirm(false); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	dc, err := ch.PublishWithDeferredConfirmWithContext(ctx, "", UpdatesQueue, false, false, amqp.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp.Persistent,
		MessageId:    uuid.New().String(),
		Body:         body,
	})
	if err != nil {
		t.Fatal(err)
	}
	ack, err := dc.WaitContext(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !ack {
		t.Fatal("broker rejected the update")
	}
}

func publishUpdate(t *testing.T, update TaskUpdateMessage) {
	t.Helper()
	body, err := json.Marshal(update)
	if err != nil {
		t.Fatal(err)
	}
	publishBytes(t, body)
}

func TestLifecycle(t *testing.T) {
	requireStack(t)

	slug := newSlug()
	tenant, task := createTenant(t, slug)
	if tenant.Status != TenantProvisioning || tenant.Version != 1 {
		t.Fatalf("new tenant: status %s version %d", tenant.Status, tenant.Version)
	}
	if task.Type != TaskDeploy || task.Status != TaskAccepted {
		t.Fatalf("new task: type %s status %s", task.Type, task.Status)
	}

	tenant = waitStatus(t, tenant.ID, TenantActive)
	if tenant.Version != 2 {
		t.Fatalf("after deploy: version %d, want 2", tenant.Version)
	}

	var patched tenantTaskResponse
	code := do(t, http.MethodPatch, "/v1/tenants/"+tenant.ID.String(),
		map[string]any{"name": "Renamed", "version": tenant.Version}, &patched)
	if code != http.StatusAccepted {
		t.Fatalf("patch: status %d", code)
	}
	if patched.Tenant.Status != TenantUpdating || patched.Tenant.Version != 3 {
		t.Fatalf("after patch: status %s version %d", patched.Tenant.Status, patched.Tenant.Version)
	}

	tenant = waitStatus(t, tenant.ID, TenantActive)
	if tenant.Version != 4 || tenant.Name != "Renamed" {
		t.Fatalf("after update: version %d name %q", tenant.Version, tenant.Name)
	}

	var deleted tenantTaskResponse
	code = do(t, http.MethodDelete, "/v1/tenants/"+tenant.ID.String(), nil, &deleted)
	if code != http.StatusAccepted {
		t.Fatalf("delete: status %d", code)
	}
	if deleted.Tenant.Status != TenantDestroying {
		t.Fatalf("after delete: status %s", deleted.Tenant.Status)
	}

	tenant = waitStatus(t, tenant.ID, TenantDestroyed)
	if tenant.Version != 6 {
		t.Fatalf("after destroy: version %d, want 6", tenant.Version)
	}

	var errBody errorResponse
	code = do(t, http.MethodPost, "/v1/tenants",
		map[string]string{"slug": slug, "name": "Again"}, &errBody)
	if code != http.StatusConflict || errBody.Error.Code != "tenant_already_exists" {
		t.Fatalf("recreate: status %d code %q", code, errBody.Error.Code)
	}

	tasks := tasksOf(t, tenant.ID)
	if len(tasks) != 3 {
		t.Fatalf("got %d tasks, want 3", len(tasks))
	}
}

func TestConcurrentCreateOneWinner(t *testing.T) {
	requireStack(t)

	slug := newSlug()
	const racers = 20

	var wg sync.WaitGroup
	results := make(chan int, racers)
	for i := 0; i < racers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			var out tenantTaskResponse
			results <- do(t, http.MethodPost, "/v1/tenants",
				map[string]string{"slug": slug, "name": fmt.Sprintf("Racer %d", i)}, &out)
		}(i)
	}
	wg.Wait()
	close(results)

	accepted, conflicts := 0, 0
	for code := range results {
		switch code {
		case http.StatusAccepted:
			accepted++
		case http.StatusConflict:
			conflicts++
		}
	}
	if accepted != 1 || conflicts != racers-1 {
		t.Fatalf("accepted=%d conflicts=%d", accepted, conflicts)
	}
}

func TestConcurrentPatchOneWinner(t *testing.T) {
	requireStack(t)

	tenant, _ := createTenant(t, newSlug())
	tenant = waitStatus(t, tenant.ID, TenantActive)
	version := tenant.Version

	const racers = 20
	var wg sync.WaitGroup
	results := make(chan int, racers)
	for i := 0; i < racers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			var out tenantTaskResponse
			results <- do(t, http.MethodPatch, "/v1/tenants/"+tenant.ID.String(),
				map[string]any{"name": fmt.Sprintf("Name %d", i), "version": version}, &out)
		}(i)
	}
	wg.Wait()
	close(results)

	accepted, conflicts := 0, 0
	for code := range results {
		switch code {
		case http.StatusAccepted:
			accepted++
		case http.StatusConflict:
			conflicts++
		}
	}
	if accepted != 1 || conflicts != racers-1 {
		t.Fatalf("accepted=%d conflicts=%d", accepted, conflicts)
	}

	updates := 0
	for _, task := range tasksOf(t, tenant.ID) {
		if task.Type == TaskUpdate {
			updates++
		}
	}
	if updates != 1 {
		t.Fatalf("got %d update tasks, want 1", updates)
	}

	waitStatus(t, tenant.ID, TenantActive)
}

func TestDuplicateAndStaleUpdates(t *testing.T) {
	requireBroker(t)

	tenant, task := createTenant(t, newSlug())

	done := TaskUpdateMessage{
		MessageID: uuid.New(),
		TaskID:    task.ID,
		Status:    TaskDone,
	}
	publishUpdate(t, done)
	publishUpdate(t, done) // same message id: duplicate

	tenant = waitStatus(t, tenant.ID, TenantActive)
	if tenant.Version != 2 {
		t.Fatalf("after done: version %d, want 2", tenant.Version)
	}

	publishUpdate(t, TaskUpdateMessage{
		MessageID: uuid.New(),
		TaskID:    task.ID,
		Status:    TaskInProgress,
	})
	time.Sleep(700 * time.Millisecond)

	tenant = fetchTenant(t, tenant.ID)
	if tenant.Status != TenantActive || tenant.Version != 2 {
		t.Fatalf("late in_progress changed tenant: status %s version %d",
			tenant.Status, tenant.Version)
	}

	var stored Task
	code := do(t, http.MethodGet, "/v1/tasks/"+task.ID.String(), nil, &stored)
	if code != http.StatusOK || stored.Status != TaskDone {
		t.Fatalf("task regressed: status %d task %s", code, stored.Status)
	}
}

func TestPoisonDoesNotStopConsumer(t *testing.T) {
	requireBroker(t)

	tenant, task := createTenant(t, newSlug())

	publishBytes(t, []byte("this is not json"))
	publishUpdate(t, TaskUpdateMessage{
		MessageID: uuid.New(),
		TaskID:    task.ID,
		Status:    TaskDone,
	})

	tenant = waitStatus(t, tenant.ID, TenantActive)
	if tenant.Version != 2 {
		t.Fatalf("version %d, want 2", tenant.Version)
	}
}

func TestRejectedPatchCreatesNoTask(t *testing.T) {
	requireStack(t)

	tenant, _ := createTenant(t, newSlug())
	tenant = waitStatus(t, tenant.ID, TenantActive)
	before := len(tasksOf(t, tenant.ID))

	var errBody errorResponse
	code := do(t, http.MethodPatch, "/v1/tenants/"+tenant.ID.String(),
		map[string]any{"name": "Nope", "version": 999}, &errBody)
	if code != http.StatusConflict || errBody.Error.Code != "tenant_version_conflict" {
		t.Fatalf("patch: status %d code %q", code, errBody.Error.Code)
	}

	after := len(tasksOf(t, tenant.ID))
	if after != before {
		t.Fatalf("rejected patch created a task: before %d after %d", before, after)
	}
}
