package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/charliec05/fault-tolerant-ci-orchestrator/internal/orchestrator"
)

type result struct {
	Tasks              int     `json:"tasks"`
	Workers            int     `json:"workers"`
	InjectedWorkerLoss int64   `json:"injected_worker_losses"`
	TaskAttempts       int     `json:"task_attempts"`
	FinalStatus        string  `json:"final_status"`
	ElapsedMS          float64 `json:"elapsed_ms"`
	SerialEstimateMS   float64 `json:"serial_estimate_ms"`
	ParallelSpeedup    float64 `json:"parallel_speedup"`
}

func main() {
	taskCount := flag.Int("tasks", 1000, "number of synthetic test shards")
	workerCount := flag.Int("workers", 32, "number of workers")
	workDuration := flag.Duration("work-duration", time.Millisecond, "synthetic execution time per shard")
	leaseDuration := flag.Duration("lease-duration", 50*time.Millisecond, "task lease duration")
	faultEvery := flag.Int("fault-every", 25, "drop the first lease for every Nth task; zero disables faults")
	flag.Parse()
	if *taskCount <= 0 || *workerCount <= 0 || *workDuration <= 0 || *leaseDuration <= 0 {
		fmt.Fprintln(os.Stderr, "tasks, workers, work-duration, and lease-duration must be positive")
		os.Exit(2)
	}

	coordinator, err := orchestrator.New("", *leaseDuration, nil)
	if err != nil {
		panic(err)
	}
	specs := make([]orchestrator.TaskSpec, 0, *taskCount)
	for index := 0; index < *taskCount; index++ {
		specs = append(specs, orchestrator.TaskSpec{
			Name: fmt.Sprintf("test-%05d", index), Command: []string{"true"},
			Timeout: time.Second, MaxAttempts: 3,
		})
	}
	job, err := coordinator.Submit(specs)
	if err != nil {
		panic(err)
	}

	start := time.Now()
	var workerLosses atomic.Int64
	var wait sync.WaitGroup
	for worker := 0; worker < *workerCount; worker++ {
		wait.Add(1)
		go func(worker int) {
			defer wait.Done()
			workerID := fmt.Sprintf("worker-%02d", worker)
			for {
				tasks, err := coordinator.Lease(workerID, 1)
				if err != nil {
					panic(err)
				}
				if len(tasks) == 0 {
					current, _, err := coordinator.GetJob(job.ID)
					if err != nil || current.Status == orchestrator.JobSucceeded || current.Status == orchestrator.JobFailed {
						return
					}
					time.Sleep(time.Millisecond)
					continue
				}
				task := tasks[0]
				var index int
				_, _ = fmt.Sscanf(task.Name, "test-%d", &index)
				if *faultEvery > 0 && index%*faultEvery == 0 && task.Attempt == 1 {
					workerLosses.Add(1)
					time.Sleep(*leaseDuration + time.Millisecond)
					continue
				}
				time.Sleep(*workDuration)
				_, err = coordinator.Complete(workerID, task.ID, task.LeaseToken, orchestrator.Completion{Success: true})
				if err != nil {
					panic(err)
				}
			}
		}(worker)
	}
	wait.Wait()
	elapsed := time.Since(start)
	finalJob, _, _ := coordinator.GetJob(job.ID)
	stats := coordinator.Stats()
	serialEstimate := time.Duration(*taskCount) * *workDuration
	output := result{
		Tasks:              *taskCount,
		Workers:            *workerCount,
		InjectedWorkerLoss: workerLosses.Load(),
		TaskAttempts:       stats["task_attempts_total"],
		FinalStatus:        string(finalJob.Status),
		ElapsedMS:          float64(elapsed.Microseconds()) / 1000,
		SerialEstimateMS:   float64(serialEstimate.Microseconds()) / 1000,
		ParallelSpeedup:    float64(serialEstimate) / float64(elapsed),
	}
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	_ = encoder.Encode(output)
}
