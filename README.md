# Dispatch

Dispatch is a small Redis-backed job execution system for running Python work on remote worker
servers.

It is intentionally not Kubernetes, not a scheduler, and not a workflow engine. The intended use is
to let an existing orchestrator, such as Dagster, submit expensive units of work over HTTP while
Dispatch handles queueing, worker pickup, execution, result capture, and stuck-job recovery.

## Architecture

- `coordinator`: Elixir HTTP API that stores job state in Redis
- `worker`: Elixir poller that claims jobs from the coordinator and runs Python
- `redis`: shared queue and job state store
- `python/scripts`: executable Python job entrypoints
- `python/dagster_dispatch`: optional Dagster client/resource helper

Workers use a pull model. You can add capacity by running more worker services pointed at the same
coordinator.

## What It Does

- `POST /jobs` queues a job
- `POST /internal/poll` lets a worker atomically claim a job
- workers execute `python/scripts/<job_type>.py`
- `POST /internal/result` stores `success` or `failed`
- `GET /jobs/:id` returns the current job status
- stuck `running` jobs are recovered back to the queue

## Non-Goals

- no scheduling
- no autoscaling
- no UI
- no distributed locking service
- no authentication in the MVP
- no secret management
- no custom Dagster executor or run launcher

Do not expose the coordinator publicly without adding authentication and network controls.

## API

Submit a job:

```http
POST /jobs
content-type: application/json

{
  "job_type": "fetch_prices",
  "params": {
    "symbol": "AAPL"
  }
}
```

Response:

```json
{
  "job_id": "uuid"
}
```

Get job status:

```http
GET /jobs/:id
```

Response:

```json
{
  "job_id": "uuid",
  "status": "queued | running | success | failed",
  "result": "...",
  "error": "...",
  "worker_name": "worker-1"
}
```

Worker endpoints:

- `POST /internal/poll`
- `POST /internal/result`

Those are intended for workers only.

## Job Lifecycle

Redis keys:

- `jobs:queue`: queued job ids
- `jobs:processing`: claimed job ids currently running
- `job:{id}`: job hash containing status, payload, result, error, timestamps, and worker name

State flow:

```text
queued -> running -> success | failed
```

Reliability behavior:

- jobs are claimed with `RPOPLPUSH jobs:queue jobs:processing`
- completed jobs are removed from `jobs:processing` with `LREM`
- job hashes include `inserted_at`, `started_at`, and `finished_at`
- running and completed jobs include `worker_name` when claimed by a modern worker
- the coordinator periodically requeues stuck `running` jobs after `DISPATCH_JOB_STUCK_AFTER_SECONDS`
- stale worker results are rejected if a job was already recovered and restarted

`RPOPLPUSH` keeps the queue-to-processing move atomic without blocking the coordinator HTTP
request. If a worker process dies after receiving a job, recovery can still find the job id in
`jobs:processing` and requeue it.

## Configuration

Coordinator:

```env
APP_ROLE=coordinator
REDIS_URL=redis://localhost:6379/0
PORT=4000
DISPATCH_JOB_STUCK_AFTER_SECONDS=1800
```

Worker:

```env
APP_ROLE=worker
COORDINATOR_URL=http://localhost:4000
WORKER_NAME=worker-1
WORKER_CONCURRENCY=5
WORKER_POLL_INTERVAL_MS=1000
PYTHON_ROOT=/app/python
PYTHON_BIN=/opt/dispatch-python/bin/python
```

Generic callable runner:

```env
DISPATCH_CALLABLE_ALLOWLIST_FILE=/etc/dispatch/callables.json
DISPATCH_CALLABLE_ALLOWLIST_JSON={"example_job":"my_project.jobs:run_example_job"}
```

Allowlist loading order:

1. `DISPATCH_CALLABLE_ALLOWLIST_FILE`
2. `DISPATCH_CALLABLE_ALLOWLIST_JSON`
3. clear failure if neither is set

Use the JSON env form for quick tests. Prefer the file form for real deployments.

## Local Quickstart

Start Redis on `localhost:6379`.

Install Elixir dependencies:

```powershell
Set-Location .\elixir_app
mix deps.get
```

Start the coordinator:

```powershell
$env:APP_ROLE = "coordinator"
$env:REDIS_URL = "redis://localhost:6379/0"
$env:PORT = "4000"
mix run --no-halt
```

Start a worker in another shell:

```powershell
Set-Location .\elixir_app
$env:APP_ROLE = "worker"
$env:COORDINATOR_URL = "http://localhost:4000"
$env:WORKER_NAME = "worker-a"
$env:WORKER_CONCURRENCY = "2"
mix run --no-halt
```

Submit the bundled demo job:

```powershell
curl.exe -X POST http://localhost:4000/jobs `
  -H "content-type: application/json" `
  -d "{\"job_type\":\"fetch_prices\",\"params\":{\"symbol\":\"AAPL\"}}"
```

Check status:

```powershell
curl.exe http://localhost:4000/jobs/<job_id>
```

Failure test:

```powershell
curl.exe -X POST http://localhost:4000/jobs `
  -H "content-type: application/json" `
  -d "{\"job_type\":\"fetch_prices\",\"params\":{\"symbol\":\"FAIL\"}}"
```

## Python Job Scripts

Any script in `python/scripts` can be called by its filename:

```text
python/scripts/fetch_prices.py -> job_type: fetch_prices
```

The worker runs:

```text
python scripts/<job_type>.py <params-json>
```

Expected behavior:

- write the job result to stdout
- write errors to stderr
- exit `0` for success
- exit non-zero for failure

## Generic Python Callable Jobs

For reusable deployments, prefer the generic callable runner instead of adding a Dispatch script for
every job.

Request:

```json
{
  "job_type": "python_callable",
  "params": {
    "callable": "example_job",
    "kwargs": {
      "partition_date": "2026-04-24"
    }
  }
}
```

Allowlist file:

```json
{
  "example_job": "my_project.jobs:run_example_job"
}
```

Dispatch does not accept arbitrary module paths over HTTP. The HTTP request can only reference an
alias already configured on the worker.

The callable must:

- accept JSON-compatible keyword arguments
- return a JSON-serializable `dict`
- raise an exception for failure

The worker stores the callable return value as the job `result`.

## Project-Specific Worker Images

For real workloads, do not pass package names or callable maps through runtime env. Build a small
derived worker image that owns the code and capability list.

Example:

```dockerfile
FROM ghcr.io/your-org/dispatch-core:latest

RUN /opt/dispatch-python/bin/python -m pip install .

COPY deploy/dispatch-worker/dispatch_callables.json /etc/dispatch/callables.json

ENV DISPATCH_CALLABLE_ALLOWLIST_FILE=/etc/dispatch/callables.json
```

This split keeps responsibilities clear:

- Dispatch core image owns the coordinator, worker runtime, and generic runner
- project image owns installed project code and allowlisted callable aliases
- deployment env owns secrets and environment-specific settings
- job params carry only business inputs, not secrets

Example worker compose using a derived image:

```yaml
services:
  worker:
    image: ghcr.io/your-org/my-project-dispatch-worker:latest
    environment:
      APP_ROLE: worker
      COORDINATOR_URL: http://coordinator:4000
      WORKER_NAME: worker-1
      WORKER_CONCURRENCY: 5
      PYTHON_ROOT: /app/python
      PYTHON_BIN: /opt/dispatch-python/bin/python
      API_KEY: ${API_KEY}
      AWS_REGION: ${AWS_REGION}
```

Secrets such as API keys and cloud credentials should be provided through your deployment platform,
cloud IAM, or secret manager. Do not send secrets through `params`; job payloads are stored in Redis.

## Publishing A Core Image

This repo includes a GitHub Actions workflow that publishes the generic Dispatch core worker image
to GitHub Container Registry:

```text
.github/workflows/publish-core-image.yml
```

Published tags:

```text
ghcr.io/abdulrahman-97/dispatch-core-worker:main
ghcr.io/abdulrahman-97/dispatch-core-worker:sha-<git-sha>
```

Use the immutable `sha-<git-sha>` tag when building derived worker images.

Local equivalent:

```powershell
$DISPATCH_SHA = git rev-parse HEAD

docker build `
  -t ghcr.io/abdulrahman-97/dispatch-core-worker:sha-$DISPATCH_SHA `
  .

docker push ghcr.io/abdulrahman-97/dispatch-core-worker:sha-$DISPATCH_SHA
```

## Docker Compose

This repo includes two Compose files:

- `dokploy-coordinator.compose.yml`: coordinator plus Redis
- `dokploy-worker.compose.yml`: generic worker service

They are Dokploy-friendly, but the same idea works with any Docker Compose host.

Coordinator:

```env
REDIS_URL=redis://redis:6379/0
COORDINATOR_BIND_IP=0.0.0.0
```

Worker:

```env
COORDINATOR_URL=http://<coordinator-host>:4000
WORKER_NAME=worker-1
WORKER_CONCURRENCY=5
```

The worker does not need Redis access. It only needs HTTP reachability to the coordinator.

## Dokploy API Helper

`python/tools/dokploy_deploy.py` can create/update two Dokploy compose services:

- coordinator service in one Dokploy project/environment/server
- worker service in another Dokploy project/environment/server

Use `.env.example` as the template:

```powershell
Copy-Item .env.example .env
python .\python\tools\dokploy_deploy.py
```

The helper is optional. You can also create the Compose services manually in Dokploy.

## Dagster Integration

Dagster should treat Dispatch as a normal external service.

This repo includes a small optional helper:

- `python/dagster_dispatch/client.py`
- `python/dagster_dispatch/resource.py`
- `python/examples/dagster_dispatch_asset.py`

Typical pattern:

1. Add `python/dagster_dispatch` to your Dagster code location.
2. Set `DISPATCH_COORDINATOR_URL` in the Dagster deployment.
3. Register `DispatchCoordinatorResource`.
4. Submit Dispatch jobs from selected assets or ops.

Minimal resource setup:

```python
import dagster as dg

from dagster_dispatch import DispatchCoordinatorResource

defs = dg.Definitions(
    resources={
        "dispatch": DispatchCoordinatorResource(
            base_url=dg.EnvVar("DISPATCH_COORDINATOR_URL"),
        )
    }
)
```

Example asset:

```python
@dg.asset
def remote_fetch_prices(
    context: dg.AssetExecutionContext,
    dispatch: DispatchCoordinatorResource,
):
    return dispatch.run_job_and_wait(
        context,
        "fetch_prices",
        {"symbol": "AAPL"},
    )
```

Dispatch is not a Dagster executor. Dagster still owns orchestration, dependencies, schedules,
partitions, asset materialization, and retries at the pipeline level.

## Scaling Workers

To increase capacity:

- raise `WORKER_CONCURRENCY` on a worker service
- run another worker service on the same server
- deploy worker services on additional servers

Total rough concurrency is:

```text
sum(WORKER_CONCURRENCY across all worker services)
```

Choose concurrency based on the workload. CPU-heavy Python jobs usually need lower concurrency than
I/O-heavy jobs.

## Testing

Elixir tests:

```powershell
Set-Location .\elixir_app
$env:APP_ROLE = "none"
mix test
```

Python checks:

```powershell
python -m unittest discover -s python\tests -p "test_*.py"
python -m py_compile python\scripts\python_callable.py python\scripts\fetch_prices.py python\tools\dokploy_deploy.py
```

## Current Limitations

- no authentication
- no built-in TLS termination
- no built-in secret backend
- no result pagination or large-result storage
- no first-class worker health metrics
- no retry policy beyond stuck-job recovery

For production-shaped jobs, return durable output pointers and metadata, not large datasets, in the
job result.
