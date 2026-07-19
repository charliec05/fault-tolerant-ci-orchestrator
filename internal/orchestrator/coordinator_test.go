package orchestrator

import (
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func taskSpec(name string) TaskSpec {
	return TaskSpec{Name: name, Command: []string{"go", "test", "./..."}, Timeout: time.Minute, MaxAttempts: 3}
}

func TestExpiredLeaseIsRecoveredAndStaleCompletionRejected(t *testing.T) {
	now := time.Date(2026, 7, 19, 12, 0, 0, 0, time.UTC)
	coordinator, _ := New("", 10*time.Second, func() time.Time { return now })
	job, err := coordinator.Submit([]TaskSpec{taskSpec("unit")})
	if err != nil {
		t.Fatal(err)
	}
	first, _ := coordinator.Lease("worker-a", 1)
	if len(first) != 1 || first[0].Attempt != 1 {
		t.Fatalf("unexpected first lease: %#v", first)
	}
	now = now.Add(11 * time.Second)
	second, _ := coordinator.Lease("worker-b", 1)
	if len(second) != 1 || second[0].ID != first[0].ID || second[0].Attempt != 2 {
		t.Fatalf("unexpected recovered lease: %#v", second)
	}
	if _, err := coordinator.Complete("worker-a", first[0].ID, first[0].LeaseToken, Completion{Success: true}); !errors.Is(err, ErrStaleLease) {
		t.Fatalf("stale completion error = %v", err)
	}
	if _, err := coordinator.Complete("worker-b", second[0].ID, second[0].LeaseToken, Completion{Success: true}); err != nil {
		t.Fatal(err)
	}
	updated, _, _ := coordinator.GetJob(job.ID)
	if updated.Status != JobSucceeded {
		t.Fatalf("job status = %s", updated.Status)
	}
}

func TestHeartbeatExtendsLease(t *testing.T) {
	now := time.Date(2026, 7, 19, 12, 0, 0, 0, time.UTC)
	coordinator, _ := New("", 10*time.Second, func() time.Time { return now })
	_, _ = coordinator.Submit([]TaskSpec{taskSpec("integration")})
	leased, _ := coordinator.Lease("worker-a", 1)
	now = now.Add(8 * time.Second)
	heartbeat, err := coordinator.Heartbeat("worker-a", leased[0].ID, leased[0].LeaseToken)
	if err != nil {
		t.Fatal(err)
	}
	now = now.Add(5 * time.Second)
	if count, _ := coordinator.RequeueExpired(); count != 0 {
		t.Fatalf("requeued %d tasks after heartbeat", count)
	}
	if !heartbeat.LeaseExpiresAt.After(now) {
		t.Fatalf("heartbeat did not extend lease")
	}
}

func TestFailedTaskRetriesThenFailsJob(t *testing.T) {
	coordinator, _ := New("", time.Second, nil)
	spec := taskSpec("flaky")
	spec.MaxAttempts = 2
	job, _ := coordinator.Submit([]TaskSpec{spec})
	for attempt := 1; attempt <= 2; attempt++ {
		leased, _ := coordinator.Lease("worker", 1)
		if len(leased) != 1 {
			t.Fatalf("attempt %d did not lease", attempt)
		}
		task, err := coordinator.Complete("worker", leased[0].ID, leased[0].LeaseToken, Completion{Success: false, ExitCode: 1})
		if err != nil {
			t.Fatal(err)
		}
		if attempt == 1 && task.Status != TaskQueued {
			t.Fatalf("first failure status = %s", task.Status)
		}
	}
	updated, _, _ := coordinator.GetJob(job.ID)
	if updated.Status != JobFailed {
		t.Fatalf("job status = %s", updated.Status)
	}
}

func TestConcurrentLeasesAreUnique(t *testing.T) {
	coordinator, _ := New("", time.Minute, nil)
	specs := make([]TaskSpec, 100)
	for index := range specs {
		specs[index] = taskSpec(fmt.Sprintf("test-%d", index))
	}
	_, _ = coordinator.Submit(specs)

	seen := make(map[string]struct{})
	var seenMu sync.Mutex
	var wait sync.WaitGroup
	for worker := 0; worker < 20; worker++ {
		wait.Add(1)
		go func(worker int) {
			defer wait.Done()
			for {
				leased, err := coordinator.Lease(fmt.Sprintf("worker-%d", worker), 1)
				if err != nil {
					t.Error(err)
					return
				}
				if len(leased) == 0 {
					return
				}
				seenMu.Lock()
				if _, exists := seen[leased[0].ID]; exists {
					t.Errorf("task %s leased twice", leased[0].ID)
				}
				seen[leased[0].ID] = struct{}{}
				seenMu.Unlock()
			}
		}(worker)
	}
	wait.Wait()
	if len(seen) != 100 {
		t.Fatalf("leased %d tasks, want 100", len(seen))
	}
}

func TestCoordinatorStateSurvivesRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "coordinator.json")
	coordinator, err := New(path, time.Minute, nil)
	if err != nil {
		t.Fatal(err)
	}
	job, _ := coordinator.Submit([]TaskSpec{taskSpec("persisted")})
	leased, _ := coordinator.Lease("worker", 1)
	_, _ = coordinator.Complete("worker", leased[0].ID, leased[0].LeaseToken, Completion{Success: true})

	reloaded, err := New(path, time.Minute, nil)
	if err != nil {
		t.Fatal(err)
	}
	updated, tasks, err := reloaded.GetJob(job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if updated.Status != JobSucceeded || len(tasks) != 1 || tasks[0].Status != TaskSucceeded {
		t.Fatalf("unexpected reloaded state: job=%s tasks=%#v", updated.Status, tasks)
	}
}
