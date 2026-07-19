package httpapi

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/charliec05/fault-tolerant-ci-orchestrator/internal/orchestrator"
)

type Handler struct {
	coordinator *orchestrator.Coordinator
}

func New(coordinator *orchestrator.Coordinator) http.Handler {
	return &Handler{coordinator: coordinator}
}

func (h *Handler) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	writer.Header().Set("Content-Type", "application/json")
	path := strings.Trim(request.URL.Path, "/")
	parts := strings.Split(path, "/")
	switch {
	case request.Method == http.MethodGet && path == "healthz":
		writeJSON(writer, http.StatusOK, map[string]string{"status": "ok"})
	case request.Method == http.MethodGet && path == "metrics":
		h.metrics(writer)
	case request.Method == http.MethodPost && path == "v1/jobs":
		h.submit(writer, request)
	case request.Method == http.MethodGet && len(parts) == 3 && parts[0] == "v1" && parts[1] == "jobs":
		h.getJob(writer, parts[2])
	case request.Method == http.MethodPost && len(parts) == 4 && parts[0] == "v1" && parts[1] == "jobs" && parts[3] == "cancel":
		h.cancel(writer, parts[2])
	case request.Method == http.MethodPost && path == "v1/leases":
		h.lease(writer, request)
	case request.Method == http.MethodPost && len(parts) == 4 && parts[0] == "v1" && parts[1] == "tasks" && parts[3] == "heartbeat":
		h.heartbeat(writer, request, parts[2])
	case request.Method == http.MethodPost && len(parts) == 4 && parts[0] == "v1" && parts[1] == "tasks" && parts[3] == "complete":
		h.complete(writer, request, parts[2])
	default:
		writeJSON(writer, http.StatusNotFound, map[string]string{"error": "route not found"})
	}
}

func (h *Handler) submit(writer http.ResponseWriter, request *http.Request) {
	var body struct {
		Tasks []struct {
			Name        string   `json:"name"`
			Command     []string `json:"command"`
			TimeoutMS   int      `json:"timeout_ms"`
			MaxAttempts int      `json:"max_attempts"`
		} `json:"tasks"`
	}
	if err := decode(request, &body); err != nil {
		writeError(writer, err)
		return
	}
	specs := make([]orchestrator.TaskSpec, 0, len(body.Tasks))
	for _, task := range body.Tasks {
		specs = append(specs, orchestrator.TaskSpec{
			Name:        task.Name,
			Command:     task.Command,
			Timeout:     time.Duration(task.TimeoutMS) * time.Millisecond,
			MaxAttempts: task.MaxAttempts,
		})
	}
	job, err := h.coordinator.Submit(specs)
	if err != nil {
		writeError(writer, err)
		return
	}
	writeJSON(writer, http.StatusCreated, job)
}

func (h *Handler) getJob(writer http.ResponseWriter, id string) {
	job, tasks, err := h.coordinator.GetJob(id)
	if err != nil {
		writeError(writer, err)
		return
	}
	writeJSON(writer, http.StatusOK, map[string]any{"job": job, "tasks": tasks})
}

func (h *Handler) cancel(writer http.ResponseWriter, id string) {
	job, err := h.coordinator.CancelJob(id)
	if err != nil {
		writeError(writer, err)
		return
	}
	writeJSON(writer, http.StatusOK, job)
}

func (h *Handler) lease(writer http.ResponseWriter, request *http.Request) {
	var body struct {
		WorkerID string `json:"worker_id"`
		Limit    int    `json:"limit"`
	}
	if err := decode(request, &body); err != nil {
		writeError(writer, err)
		return
	}
	tasks, err := h.coordinator.Lease(body.WorkerID, body.Limit)
	if err != nil {
		writeError(writer, err)
		return
	}
	writeJSON(writer, http.StatusOK, map[string]any{"tasks": tasks})
}

func (h *Handler) heartbeat(writer http.ResponseWriter, request *http.Request, taskID string) {
	var body struct {
		WorkerID   string `json:"worker_id"`
		LeaseToken string `json:"lease_token"`
	}
	if err := decode(request, &body); err != nil {
		writeError(writer, err)
		return
	}
	task, err := h.coordinator.Heartbeat(body.WorkerID, taskID, body.LeaseToken)
	if err != nil {
		writeError(writer, err)
		return
	}
	writeJSON(writer, http.StatusOK, task)
}

func (h *Handler) complete(writer http.ResponseWriter, request *http.Request, taskID string) {
	var body struct {
		WorkerID   string `json:"worker_id"`
		LeaseToken string `json:"lease_token"`
		Success    bool   `json:"success"`
		ExitCode   int    `json:"exit_code"`
		Output     string `json:"output"`
	}
	if err := decode(request, &body); err != nil {
		writeError(writer, err)
		return
	}
	task, err := h.coordinator.Complete(body.WorkerID, taskID, body.LeaseToken, orchestrator.Completion{
		Success:  body.Success,
		ExitCode: body.ExitCode,
		Output:   body.Output,
	})
	if err != nil {
		writeError(writer, err)
		return
	}
	writeJSON(writer, http.StatusOK, task)
}

func (h *Handler) metrics(writer http.ResponseWriter) {
	stats := h.coordinator.Stats()
	writer.Header().Set("Content-Type", "text/plain; version=0.0.4")
	for _, key := range orchestrator.SortedStatKeys(stats) {
		fmt.Fprintf(writer, "ci_orchestrator_%s %d\n", key, stats[key])
	}
}

func decode(request *http.Request, value any) error {
	decoder := json.NewDecoder(io.LimitReader(request.Body, 1<<20))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(value); err != nil {
		return fmt.Errorf("decode request: %w", orchestrator.ErrInvalidInput)
	}
	return nil
}

func writeError(writer http.ResponseWriter, err error) {
	status := http.StatusInternalServerError
	switch {
	case errors.Is(err, orchestrator.ErrNotFound):
		status = http.StatusNotFound
	case errors.Is(err, orchestrator.ErrInvalidInput):
		status = http.StatusBadRequest
	case errors.Is(err, orchestrator.ErrStaleLease), errors.Is(err, orchestrator.ErrWorkerMismatch), errors.Is(err, orchestrator.ErrInvalidState):
		status = http.StatusConflict
	}
	writeJSON(writer, status, map[string]string{"error": err.Error()})
}

func writeJSON(writer http.ResponseWriter, status int, value any) {
	writer.WriteHeader(status)
	_ = json.NewEncoder(writer).Encode(value)
}
