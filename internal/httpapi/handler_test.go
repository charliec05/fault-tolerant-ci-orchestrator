package httpapi

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/charliec05/fault-tolerant-ci-orchestrator/internal/orchestrator"
)

func TestHTTPLeaseAndComplete(t *testing.T) {
	coordinator, _ := orchestrator.New("", time.Minute, nil)
	handler := New(coordinator)
	submit := perform(handler, http.MethodPost, "/v1/jobs", `{"tasks":[{"name":"unit","command":["go","test","./..."],"timeout_ms":60000,"max_attempts":2}]}`)
	if submit.Code != http.StatusCreated {
		t.Fatalf("submit status=%d body=%s", submit.Code, submit.Body.String())
	}
	lease := perform(handler, http.MethodPost, "/v1/leases", `{"worker_id":"worker-1","limit":1}`)
	if lease.Code != http.StatusOK {
		t.Fatalf("lease status=%d body=%s", lease.Code, lease.Body.String())
	}
	var leased struct {
		Tasks []orchestrator.Task `json:"tasks"`
	}
	_ = json.Unmarshal(lease.Body.Bytes(), &leased)
	if len(leased.Tasks) != 1 {
		t.Fatalf("leased tasks=%d", len(leased.Tasks))
	}
	task := leased.Tasks[0]
	body, _ := json.Marshal(map[string]any{
		"worker_id": "worker-1", "lease_token": task.LeaseToken,
		"success": true, "exit_code": 0, "output": "ok",
	})
	complete := perform(handler, http.MethodPost, "/v1/tasks/"+task.ID+"/complete", string(body))
	if complete.Code != http.StatusOK {
		t.Fatalf("complete status=%d body=%s", complete.Code, complete.Body.String())
	}
}

func perform(handler http.Handler, method, path, body string) *httptest.ResponseRecorder {
	request := httptest.NewRequest(method, path, bytes.NewBufferString(body))
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}
