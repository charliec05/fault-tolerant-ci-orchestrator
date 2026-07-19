package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"github.com/charliec05/fault-tolerant-ci-orchestrator/internal/orchestrator"
)

type client struct {
	baseURL string
	http    *http.Client
}

func main() {
	coordinatorURL := flag.String("coordinator", "http://127.0.0.1:8090", "coordinator base URL")
	workerID := flag.String("worker-id", "worker-local", "stable worker identifier")
	workspace := flag.String("workspace", ".", "directory in which test commands run")
	pollInterval := flag.Duration("poll-interval", 500*time.Millisecond, "delay when no work is available")
	heartbeatInterval := flag.Duration("heartbeat-interval", 2*time.Second, "lease heartbeat interval")
	flag.Parse()

	root, err := filepath.Abs(*workspace)
	if err != nil {
		log.Fatal(err)
	}
	api := &client{baseURL: *coordinatorURL, http: &http.Client{Timeout: 15 * time.Second}}
	for {
		tasks, err := api.lease(*workerID)
		if err != nil {
			log.Printf("lease: %v", err)
			time.Sleep(*pollInterval)
			continue
		}
		if len(tasks) == 0 {
			time.Sleep(*pollInterval)
			continue
		}
		for _, task := range tasks {
			completion := execute(api, *workerID, root, task, *heartbeatInterval)
			if err := api.complete(*workerID, task, completion); err != nil {
				log.Printf("complete %s: %v", task.ID, err)
			}
		}
	}
}

func execute(api *client, workerID, workspace string, task orchestrator.Task, heartbeatInterval time.Duration) orchestrator.Completion {
	ctx, cancel := context.WithTimeout(context.Background(), task.Timeout)
	defer cancel()
	command := exec.CommandContext(ctx, task.Command[0], task.Command[1:]...)
	command.Dir = workspace
	command.Env = append(os.Environ(), "CI=true")
	var output bytes.Buffer
	command.Stdout = &output
	command.Stderr = &output

	done := make(chan struct{})
	go func() {
		ticker := time.NewTicker(heartbeatInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if err := api.heartbeat(workerID, task); err != nil {
					log.Printf("heartbeat %s: %v", task.ID, err)
				}
			case <-done:
				return
			}
		}
	}()
	err := command.Run()
	close(done)
	completion := orchestrator.Completion{Success: err == nil, Output: output.String()}
	if err != nil {
		completion.ExitCode = 1
		if exitError, ok := err.(*exec.ExitError); ok {
			completion.ExitCode = exitError.ExitCode()
		}
	}
	if ctx.Err() == context.DeadlineExceeded {
		completion.Output += "\nworker timeout exceeded"
	}
	return completion
}

func (c *client) lease(workerID string) ([]orchestrator.Task, error) {
	var response struct {
		Tasks []orchestrator.Task `json:"tasks"`
	}
	err := c.post("/v1/leases", map[string]any{"worker_id": workerID, "limit": 1}, &response)
	return response.Tasks, err
}

func (c *client) heartbeat(workerID string, task orchestrator.Task) error {
	return c.post("/v1/tasks/"+task.ID+"/heartbeat", map[string]any{
		"worker_id": workerID, "lease_token": task.LeaseToken,
	}, nil)
}

func (c *client) complete(workerID string, task orchestrator.Task, completion orchestrator.Completion) error {
	return c.post("/v1/tasks/"+task.ID+"/complete", map[string]any{
		"worker_id": workerID, "lease_token": task.LeaseToken, "success": completion.Success,
		"exit_code": completion.ExitCode, "output": completion.Output,
	}, nil)
}

func (c *client) post(path string, requestBody any, responseBody any) error {
	encoded, _ := json.Marshal(requestBody)
	request, _ := http.NewRequest(http.MethodPost, c.baseURL+path, bytes.NewReader(encoded))
	request.Header.Set("Content-Type", "application/json")
	response, err := c.http.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
		return fmt.Errorf("coordinator returned %s: %s", response.Status, body)
	}
	if responseBody != nil {
		return json.NewDecoder(response.Body).Decode(responseBody)
	}
	return nil
}
