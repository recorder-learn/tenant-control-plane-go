package app

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/google/uuid"
)

type API struct {
	svc *Service
	log *slog.Logger
}

func NewAPI(svc *Service, log *slog.Logger) *API {
	return &API{svc: svc, log: log}
}

func (a *API) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/tenants", a.handleTenants)
	mux.HandleFunc("/v1/tenants/{id}", a.handleTenant)
	mux.HandleFunc("/v1/tasks", a.handleTasks)
	mux.HandleFunc("/v1/tasks/{id}", a.handleTask)
	mux.HandleFunc("/healthz", a.handleHealth)
	mux.HandleFunc("/", a.handleNotFound)
	return mux
}

func (a *API) handleTenants(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		var req struct {
			Slug string `json:"slug"`
			Name string `json:"name"`
		}
		if !a.decode(w, r, &req) {
			return
		}
		tenant, task, err := a.svc.CreateTenant(r.Context(), req.Slug, req.Name)
		if err != nil {
			a.writeError(w, r, err)
			return
		}
		w.Header().Set("Location", "/v1/tenants/"+tenant.ID.String())
		a.writeJSON(w, http.StatusAccepted, tenantTaskResponse{Tenant: tenant, Task: task})

	case http.MethodGet:
		limit, offset, err := pageParams(r)
		if err != nil {
			a.writeError(w, r, err)
			return
		}
		items, err := a.svc.ListTenants(r.Context(), r.URL.Query().Get("status"), limit, offset)
		if err != nil {
			a.writeError(w, r, err)
			return
		}
		a.writeJSON(w, http.StatusOK, tenantListResponse{Items: items})

	default:
		a.methodNotAllowed(w, r)
	}
}

func (a *API) handleTenant(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		a.writeError(w, r, validationError("invalid tenant id"))
		return
	}

	switch r.Method {
	case http.MethodGet:
		tenant, err := a.svc.GetTenant(r.Context(), id)
		if err != nil {
			a.writeError(w, r, err)
			return
		}
		a.writeJSON(w, http.StatusOK, tenant)

	case http.MethodPatch:
		var req struct {
			Name    string `json:"name"`
			Version int64  `json:"version"`
		}
		if !a.decode(w, r, &req) {
			return
		}
		tenant, task, err := a.svc.UpdateTenant(r.Context(), id, req.Name, req.Version)
		if err != nil {
			a.writeError(w, r, err)
			return
		}
		a.writeJSON(w, http.StatusAccepted, tenantTaskResponse{Tenant: tenant, Task: task})

	case http.MethodDelete:
		tenant, task, err := a.svc.DeleteTenant(r.Context(), id)
		if err != nil {
			a.writeError(w, r, err)
			return
		}
		a.writeJSON(w, http.StatusAccepted, tenantTaskResponse{Tenant: tenant, Task: task})

	default:
		a.methodNotAllowed(w, r)
	}
}

func (a *API) handleTasks(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		a.methodNotAllowed(w, r)
		return
	}

	var tenantID *uuid.UUID
	if v := r.URL.Query().Get("tenant_id"); v != "" {
		id, err := uuid.Parse(v)
		if err != nil {
			a.writeError(w, r, validationError("invalid tenant_id"))
			return
		}
		tenantID = &id
	}

	limit, offset, err := pageParams(r)
	if err != nil {
		a.writeError(w, r, err)
		return
	}
	items, err := a.svc.ListTasks(r.Context(), tenantID, r.URL.Query().Get("status"), limit, offset)
	if err != nil {
		a.writeError(w, r, err)
		return
	}
	a.writeJSON(w, http.StatusOK, taskListResponse{Items: items})
}

func (a *API) handleTask(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		a.methodNotAllowed(w, r)
		return
	}
	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		a.writeError(w, r, validationError("invalid task id"))
		return
	}
	task, err := a.svc.GetTask(r.Context(), id)
	if err != nil {
		a.writeError(w, r, err)
		return
	}
	a.writeJSON(w, http.StatusOK, task)
}

func (a *API) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		a.methodNotAllowed(w, r)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	if err := a.svc.Ping(ctx); err != nil {
		a.writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "unavailable"})
		return
	}
	a.writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (a *API) handleNotFound(w http.ResponseWriter, r *http.Request) {
	a.writeError(w, r, newAppError(404, "not_found", "route not found"))
}

func (a *API) methodNotAllowed(w http.ResponseWriter, r *http.Request) {
	a.writeError(w, r, newAppError(405, "method_not_allowed", "method not allowed"))
}

func (a *API) decode(w http.ResponseWriter, r *http.Request, v any) bool {
	if err := json.NewDecoder(r.Body).Decode(v); err != nil {
		a.writeError(w, r, validationError("invalid JSON body"))
		return false
	}
	return true
}

func pageParams(r *http.Request) (int, int, error) {
	limit, offset := 50, 0
	query := r.URL.Query()

	if v := query.Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > 100 {
			return 0, 0, validationError("limit must be between 1 and 100")
		}
		limit = n
	}
	if v := query.Get("offset"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 0 {
			return 0, 0, validationError("offset must be a non-negative integer")
		}
		offset = n
	}
	return limit, offset, nil
}

type tenantTaskResponse struct {
	Tenant Tenant `json:"tenant"`
	Task   Task   `json:"task"`
}

type tenantListResponse struct {
	Items []Tenant `json:"items"`
}

type taskListResponse struct {
	Items []Task `json:"items"`
}

type errorResponse struct {
	Error errorDetail `json:"error"`
}

type errorDetail struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func (a *API) writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func (a *API) writeError(w http.ResponseWriter, r *http.Request, err error) {
	var appErr *AppError
	if errors.As(err, &appErr) {
		a.writeJSON(w, appErr.Status, errorResponse{
			Error: errorDetail{Code: appErr.Code, Message: appErr.Message},
		})
		return
	}
	a.log.Error("request failed", "method", r.Method, "path", r.URL.Path, "error", err)
	a.writeJSON(w, http.StatusInternalServerError, errorResponse{
		Error: errorDetail{Code: "internal_error", Message: "internal server error"},
	})
}
