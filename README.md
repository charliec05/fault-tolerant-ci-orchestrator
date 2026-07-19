# Fault-Tolerant CI Test Orchestrator

A coordinator/worker execution platform for sharded CI workloads. The project
focuses on lease ownership, worker-loss recovery, stale-result rejection,
bounded retries, durable state, and operational metrics.

## Architecture

```mermaid
flowchart LR
    CI[CI client] -->|submit job| Coordinator
    WorkerA -->|lease + heartbeat| Coordinator
    WorkerB -->|lease + heartbeat| Coordinator
    Coordinator --> State[Atomic state snapshot]
    WorkerA -->|logs + result| Coordinator
    WorkerB -->|logs + result| Coordinator
    Coordinator --> Metrics[/metrics]
```

Workers lease tasks with opaque tokens. Heartbeats extend ownership. When a
worker disappears, the coordinator requeues its expired lease or terminally
fails it after the retry budget. Results from an old owner are rejected, which
prevents a recovered task from being completed twice.

## Implemented behavior

- Multi-task jobs with explicit lifecycle state
- Lease-based scheduling and worker heartbeats
- Automatic recovery after lease expiration
- Retry budgets, execution timeout metadata, and job cancellation
- Stale lease and cross-worker completion rejection
- Idempotent duplicate completion for the active lease token
- Atomic JSON persistence and restart recovery
- Prometheus-compatible coordinator metrics
- Worker command execution without a shell, with timeout and log capture
- Kubernetes coordinator/worker manifests

## Validate

```bash
make test
make simulate
```

The simulator runs 1,000 synthetic test shards across 32 workers and
deliberately drops the first lease for every 25th task. It reports total
attempts, injected worker losses, final job status, elapsed time, and parallel
speedup. The checked-in [baseline](benchmarks/baseline-darwin-arm64.json) and
[worker-loss](benchmarks/worker-loss-darwin-arm64.json) runs are generated from
the simulator rather than invented for the README. The fault-injection run
recovered all 40 dropped leases and completed all 1,000 shards successfully.

## Run locally

Terminal 1:

```bash
make coordinator
```

Terminal 2:

```bash
go run ./cmd/worker -worker-id worker-1 -workspace /path/to/repository
```

Submit a job:

```bash
curl -sS -X POST localhost:8090/v1/jobs \
  -d '{"tasks":[{"name":"unit","command":["go","test","./..."],"timeout_ms":60000,"max_attempts":3}]}'
```

## Safety boundary

Workers execute submitted argv directly and never pass it through a shell.
Commands are still trusted CI input and should run in an isolated container or
Kubernetes pod. The included worker image uses a non-root account; a production
deployment should additionally apply seccomp, network policy, and per-task
ephemeral workspaces.

## License

MIT
