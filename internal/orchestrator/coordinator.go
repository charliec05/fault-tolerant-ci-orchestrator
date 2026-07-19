package orchestrator

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

var (
	ErrNotFound       = errors.New("not found")
	ErrInvalidInput   = errors.New("invalid input")
	ErrStaleLease     = errors.New("stale lease")
	ErrInvalidState   = errors.New("invalid state transition")
	ErrWorkerMismatch = errors.New("lease belongs to another worker")
)

type TaskStatus string

const (
	TaskQueued    TaskStatus = "queued"
	TaskLeased    TaskStatus = "leased"
	TaskSucceeded TaskStatus = "succeeded"
	TaskFailed    TaskStatus = "failed"
	TaskCancelled TaskStatus = "cancelled"
)

type JobStatus string

const (
	JobQueued    JobStatus = "queued"
	JobRunning   JobStatus = "running"
	JobSucceeded JobStatus = "succeeded"
	JobFailed    JobStatus = "failed"
	JobCancelled JobStatus = "cancelled"
)

type TaskSpec struct {
	Name        string        `json:"name"`
	Command     []string      `json:"command"`
	Timeout     time.Duration `json:"timeout"`
	MaxAttempts int           `json:"max_attempts"`
}

type Task struct {
	ID             string        `json:"id"`
	JobID          string        `json:"job_id"`
	Name           string        `json:"name"`
	Command        []string      `json:"command"`
	Timeout        time.Duration `json:"timeout"`
	MaxAttempts    int           `json:"max_attempts"`
	Attempt        int           `json:"attempt"`
	Status         TaskStatus    `json:"status"`
	LeaseOwner     string        `json:"lease_owner,omitempty"`
	LeaseToken     string        `json:"lease_token,omitempty"`
	LeaseExpiresAt time.Time     `json:"lease_expires_at,omitempty"`
	ExitCode       int           `json:"exit_code,omitempty"`
	Output         string        `json:"output,omitempty"`
	CreatedAt      time.Time     `json:"created_at"`
	StartedAt      time.Time     `json:"started_at,omitempty"`
	FinishedAt     time.Time     `json:"finished_at,omitempty"`
}

type Job struct {
	ID         string    `json:"id"`
	Status     JobStatus `json:"status"`
	TaskIDs    []string  `json:"task_ids"`
	CreatedAt  time.Time `json:"created_at"`
	StartedAt  time.Time `json:"started_at,omitempty"`
	FinishedAt time.Time `json:"finished_at,omitempty"`
}

type Completion struct {
	Success  bool   `json:"success"`
	ExitCode int    `json:"exit_code"`
	Output   string `json:"output"`
}

type state struct {
	Jobs      map[string]Job  `json:"jobs"`
	Tasks     map[string]Task `json:"tasks"`
	TaskOrder []string        `json:"task_order"`
	NextID    uint64          `json:"next_id"`
}

type Coordinator struct {
	mu            sync.Mutex
	state         state
	leaseDuration time.Duration
	stateFile     string
	now           func() time.Time
}

func New(stateFile string, leaseDuration time.Duration, now func() time.Time) (*Coordinator, error) {
	if leaseDuration <= 0 {
		return nil, fmt.Errorf("lease duration must be positive: %w", ErrInvalidInput)
	}
	if now == nil {
		now = time.Now
	}
	coordinator := &Coordinator{
		state: state{
			Jobs:  make(map[string]Job),
			Tasks: make(map[string]Task),
		},
		leaseDuration: leaseDuration,
		stateFile:     stateFile,
		now:           now,
	}
	if stateFile == "" {
		return coordinator, nil
	}
	data, err := os.ReadFile(stateFile)
	if errors.Is(err, os.ErrNotExist) {
		return coordinator, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read coordinator state: %w", err)
	}
	if err := json.Unmarshal(data, &coordinator.state); err != nil {
		return nil, fmt.Errorf("decode coordinator state: %w", err)
	}
	if coordinator.state.Jobs == nil {
		coordinator.state.Jobs = make(map[string]Job)
	}
	if coordinator.state.Tasks == nil {
		coordinator.state.Tasks = make(map[string]Task)
	}
	return coordinator, coordinator.checkLocked()
}

func (c *Coordinator) Submit(specs []TaskSpec) (Job, error) {
	if len(specs) == 0 {
		return Job{}, ErrInvalidInput
	}
	for _, spec := range specs {
		if spec.Name == "" || len(spec.Command) == 0 || spec.Timeout <= 0 || spec.MaxAttempts <= 0 {
			return Job{}, ErrInvalidInput
		}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.now().UTC()
	job := Job{ID: c.nextIDLocked("job"), Status: JobQueued, CreatedAt: now}
	for _, spec := range specs {
		task := Task{
			ID:          c.nextIDLocked("task"),
			JobID:       job.ID,
			Name:        spec.Name,
			Command:     append([]string(nil), spec.Command...),
			Timeout:     spec.Timeout,
			MaxAttempts: spec.MaxAttempts,
			Status:      TaskQueued,
			CreatedAt:   now,
		}
		c.state.Tasks[task.ID] = task
		c.state.TaskOrder = append(c.state.TaskOrder, task.ID)
		job.TaskIDs = append(job.TaskIDs, task.ID)
	}
	c.state.Jobs[job.ID] = job
	return job, c.persistLocked()
}

func (c *Coordinator) Lease(worker string, limit int) ([]Task, error) {
	if worker == "" || limit <= 0 {
		return nil, ErrInvalidInput
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.now().UTC()
	recovered := c.requeueExpiredLocked(now)
	leased := make([]Task, 0, limit)
	for _, id := range c.state.TaskOrder {
		if len(leased) == limit {
			break
		}
		task := c.state.Tasks[id]
		if task.Status != TaskQueued {
			continue
		}
		task.Attempt++
		task.Status = TaskLeased
		task.LeaseOwner = worker
		task.LeaseToken = c.nextIDLocked("lease")
		task.LeaseExpiresAt = now.Add(c.leaseDuration)
		if task.StartedAt.IsZero() {
			task.StartedAt = now
		}
		c.state.Tasks[id] = task
		job := c.state.Jobs[task.JobID]
		if job.Status == JobQueued {
			job.Status = JobRunning
			job.StartedAt = now
			c.state.Jobs[job.ID] = job
		}
		leased = append(leased, task)
	}
	if len(leased) > 0 || recovered > 0 {
		if err := c.persistLocked(); err != nil {
			return nil, err
		}
	}
	return leased, nil
}

func (c *Coordinator) Heartbeat(worker, taskID, leaseToken string) (Task, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	task, err := c.requireLeaseLocked(worker, taskID, leaseToken)
	if err != nil {
		return Task{}, err
	}
	task.LeaseExpiresAt = c.now().UTC().Add(c.leaseDuration)
	c.state.Tasks[task.ID] = task
	return task, c.persistLocked()
}

func (c *Coordinator) Complete(worker, taskID, leaseToken string, completion Completion) (Task, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	task, ok := c.state.Tasks[taskID]
	if !ok {
		return Task{}, ErrNotFound
	}
	if (task.Status == TaskSucceeded || task.Status == TaskFailed) && task.LeaseToken == leaseToken {
		return task, nil
	}
	if task.Status != TaskLeased || task.LeaseToken != leaseToken {
		return Task{}, ErrStaleLease
	}
	if task.LeaseOwner != worker {
		return Task{}, ErrWorkerMismatch
	}
	now := c.now().UTC()
	task.ExitCode = completion.ExitCode
	task.Output = truncate(completion.Output, 64*1024)
	task.LeaseExpiresAt = time.Time{}
	if completion.Success {
		task.Status = TaskSucceeded
		task.FinishedAt = now
	} else if task.Attempt < task.MaxAttempts {
		task.Status = TaskQueued
		task.LeaseOwner = ""
	} else {
		task.Status = TaskFailed
		task.FinishedAt = now
	}
	c.state.Tasks[task.ID] = task
	c.updateJobLocked(task.JobID, now)
	return task, c.persistLocked()
}

func (c *Coordinator) CancelJob(jobID string) (Job, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	job, ok := c.state.Jobs[jobID]
	if !ok {
		return Job{}, ErrNotFound
	}
	if job.Status == JobSucceeded || job.Status == JobFailed || job.Status == JobCancelled {
		return job, nil
	}
	now := c.now().UTC()
	job.Status = JobCancelled
	job.FinishedAt = now
	for _, taskID := range job.TaskIDs {
		task := c.state.Tasks[taskID]
		if task.Status == TaskQueued || task.Status == TaskLeased {
			task.Status = TaskCancelled
			task.FinishedAt = now
			task.LeaseExpiresAt = time.Time{}
			c.state.Tasks[taskID] = task
		}
	}
	c.state.Jobs[job.ID] = job
	return job, c.persistLocked()
}

func (c *Coordinator) GetJob(id string) (Job, []Task, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.now().UTC()
	if c.requeueExpiredLocked(now) > 0 {
		if err := c.persistLocked(); err != nil {
			return Job{}, nil, err
		}
	}
	job, ok := c.state.Jobs[id]
	if !ok {
		return Job{}, nil, ErrNotFound
	}
	tasks := make([]Task, 0, len(job.TaskIDs))
	for _, taskID := range job.TaskIDs {
		tasks = append(tasks, c.state.Tasks[taskID])
	}
	return job, tasks, nil
}

func (c *Coordinator) RequeueExpired() (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	count := c.requeueExpiredLocked(c.now().UTC())
	if count == 0 {
		return 0, nil
	}
	return count, c.persistLocked()
}

func (c *Coordinator) requeueExpiredLocked(now time.Time) int {
	count := 0
	jobIDs := make(map[string]struct{})
	for id, task := range c.state.Tasks {
		if task.Status != TaskLeased || task.LeaseExpiresAt.After(now) {
			continue
		}
		if task.Attempt < task.MaxAttempts {
			task.Status = TaskQueued
			task.LeaseOwner = ""
			task.LeaseToken = ""
			task.LeaseExpiresAt = time.Time{}
		} else {
			task.Status = TaskFailed
			task.FinishedAt = now
			task.Output = truncate(task.Output+"\nlease expired", 64*1024)
		}
		c.state.Tasks[id] = task
		jobIDs[task.JobID] = struct{}{}
		count++
	}
	for jobID := range jobIDs {
		c.updateJobLocked(jobID, now)
	}
	return count
}

func (c *Coordinator) requireLeaseLocked(worker, taskID, leaseToken string) (Task, error) {
	task, ok := c.state.Tasks[taskID]
	if !ok {
		return Task{}, ErrNotFound
	}
	if task.Status != TaskLeased || task.LeaseToken != leaseToken {
		return Task{}, ErrStaleLease
	}
	if task.LeaseOwner != worker {
		return Task{}, ErrWorkerMismatch
	}
	return task, nil
}

func (c *Coordinator) updateJobLocked(jobID string, now time.Time) {
	job := c.state.Jobs[jobID]
	if job.Status == JobCancelled {
		return
	}
	allTerminal := true
	hasFailed := false
	for _, taskID := range job.TaskIDs {
		switch c.state.Tasks[taskID].Status {
		case TaskFailed:
			hasFailed = true
		case TaskSucceeded, TaskCancelled:
		default:
			allTerminal = false
		}
	}
	if allTerminal && hasFailed {
		job.Status = JobFailed
		job.FinishedAt = now
	} else if allTerminal {
		job.Status = JobSucceeded
		job.FinishedAt = now
	} else if !job.StartedAt.IsZero() {
		job.Status = JobRunning
	}
	c.state.Jobs[jobID] = job
}

func (c *Coordinator) Stats() map[string]int {
	c.mu.Lock()
	defer c.mu.Unlock()
	stats := make(map[string]int)
	for _, job := range c.state.Jobs {
		stats["jobs_"+string(job.Status)]++
	}
	for _, task := range c.state.Tasks {
		stats["tasks_"+string(task.Status)]++
		stats["task_attempts_total"] += task.Attempt
	}
	return stats
}

func (c *Coordinator) nextIDLocked(prefix string) string {
	c.state.NextID++
	return fmt.Sprintf("%s-%08d", prefix, c.state.NextID)
}

func (c *Coordinator) checkLocked() error {
	for id, job := range c.state.Jobs {
		for _, taskID := range job.TaskIDs {
			task, ok := c.state.Tasks[taskID]
			if !ok || task.JobID != id {
				return fmt.Errorf("job %s references invalid task %s", id, taskID)
			}
		}
	}
	return nil
}

func (c *Coordinator) persistLocked() error {
	if err := c.checkLocked(); err != nil {
		return err
	}
	if c.stateFile == "" {
		return nil
	}
	directory := filepath.Dir(c.stateFile)
	if err := os.MkdirAll(directory, 0o755); err != nil {
		return err
	}
	temporary, err := os.CreateTemp(directory, ".orchestrator-state-*")
	if err != nil {
		return err
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)
	encoder := json.NewEncoder(temporary)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(c.state); err != nil {
		temporary.Close()
		return err
	}
	if err := temporary.Sync(); err != nil {
		temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	return os.Rename(temporaryPath, c.stateFile)
}

func truncate(value string, limit int) string {
	if len(value) <= limit {
		return value
	}
	return value[:limit]
}

func SortedStatKeys(stats map[string]int) []string {
	keys := make([]string, 0, len(stats))
	for key := range stats {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
