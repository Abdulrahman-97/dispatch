# Minimal Distributed Job Execution MVP

This repo contains a small Redis-backed job execution system:

- `elixir_app/`: coordinator API plus worker runtime
- `python/scripts/`: Python jobs executed by worker nodes
- `python/dagster_dispatch/`: thin Dagster-side client and resource for calling the coordinator

The public API is unchanged. The reliability changes are internal:

- job claims move through `jobs:processing` before execution
- jobs carry `inserted_at`, `started_at`, and `finished_at`
- the coordinator requeues stuck `running` jobs automatically

## What It Does

- `POST /jobs` queues a job in Redis
- `POST /internal/poll` lets a worker atomically claim the next job
- the worker executes `python/scripts/<job_type>.py`
- `POST /internal/result` stores the final `success` or `failed` state
- `GET /jobs/:id` returns the current status

## Requirements

- Elixir 1.19+
- Erlang/OTP 28+
- Python 3.11+
- Redis reachable by the coordinator at `REDIS_URL`

## Config

- `APP_ROLE`: `coordinator` or `worker`
- `REDIS_URL`: Redis connection string for the coordinator, default `redis://localhost:6379/0`
- `PORT`: coordinator port, default `4000`
- `COORDINATOR_URL`: worker target, default `http://localhost:4000`
- `WORKER_CONCURRENCY`: poller count per worker node, default `5`
- `WORKER_POLL_INTERVAL_MS`: sleep when the queue is empty, default `1000`
- `WORKER_NAME`: optional log label for a worker node
- `PYTHON_ROOT`: worker path to the Python directory, default `../python`
- `PYTHON_BIN`: Python executable. If unset, the worker tries `python`, then `python3`

## Local Run

1. Start Redis on `localhost:6379`.

2. Install Elixir dependencies:

```powershell
Set-Location .\elixir_app
mix deps.get
```

3. Start the coordinator:

```powershell
$env:APP_ROLE = "coordinator"
$env:REDIS_URL = "redis://localhost:6379/0"
$env:PORT = "4000"
mix run --no-halt
```

4. Start worker node A in a second shell:

```powershell
Set-Location .\elixir_app
$env:APP_ROLE = "worker"
$env:COORDINATOR_URL = "http://localhost:4000"
$env:WORKER_NAME = "worker-a"
$env:WORKER_CONCURRENCY = "1"
mix run --no-halt
```

5. Start worker node B in a third shell:

```powershell
Set-Location .\elixir_app
$env:APP_ROLE = "worker"
$env:COORDINATOR_URL = "http://localhost:4000"
$env:WORKER_NAME = "worker-b"
$env:WORKER_CONCURRENCY = "1"
mix run --no-halt
```

6. Submit a job:

```powershell
curl.exe -X POST http://localhost:4000/jobs `
  -H "content-type: application/json" `
  -d "{\"job_type\":\"fetch_prices\",\"params\":{\"symbol\":\"AAPL\"}}"
```

7. Check the job status:

```powershell
curl.exe http://localhost:4000/jobs/<uuid>
```

## Failure Flow

Submit this job to verify the `failed` state:

```powershell
curl.exe -X POST http://localhost:4000/jobs `
  -H "content-type: application/json" `
  -d "{\"job_type\":\"fetch_prices\",\"params\":{\"symbol\":\"FAIL\"}}"
```

The Python script exits non-zero and writes to stderr, and the coordinator stores:

- `status: failed`
- `error: intentional failure for testing`

## Recovery Behavior

- A worker claim moves the job from `jobs:queue` to `jobs:processing` with Redis `BRPOPLPUSH`
- When a worker starts a job, the coordinator updates the job hash from `queued` to `running` and sets `started_at`
- When a worker finishes, the coordinator only accepts a transition from `running` to `success` or `failed`
- Completion stores `finished_at` and removes the job from `jobs:processing`
- The coordinator runs a recovery scan every 30 seconds
- Any job still marked `running` for more than 180 seconds is treated as stuck, moved back to `jobs:queue`, and reset to `queued`
- Stale worker results are rejected if the job has already been requeued and started again

## Dokploy Deployment

The simplest two-server setup is:

- Dagster server: coordinator + Redis
- Worker server: worker container only

This repo includes one image and two Dokploy-friendly compose files:

- `dokploy-coordinator.compose.yml`
- `dokploy-worker.compose.yml`
- `python/tools/dokploy_deploy.py`
- `.env.example`

### 1. Deploy the coordinator on the Dagster server

Create a Dokploy Compose service on the Dagster server using `dokploy-coordinator.compose.yml`.

This stack runs:

- `redis`
- `coordinator`

Recommended environment values:

- `REDIS_URL=redis://redis:6379/0`
- `COORDINATOR_BIND_IP=10.77.0.2`

Expose the coordinator either:

- through a Dokploy domain such as `https://dispatch.example.com`, or
- directly on the server IP and port, such as `http://10.0.0.12:4000`

Use that same URL everywhere else as the coordinator URL. With private networking, the intended setup is:

- bind the coordinator on `10.77.0.2:4000`
- point workers and Dagster at `http://10.77.0.2:4000`

### 2. Deploy the worker on the second server

Create a second Dokploy Compose service on the worker server using `dokploy-worker.compose.yml`.

Required environment values:

- `COORDINATOR_URL=https://dispatch.example.com`
  or `http://10.0.0.12:4000`

Useful worker settings:

- `WORKER_NAME=worker-server-1`
- `WORKER_CONCURRENCY=5`
- `WORKER_POLL_INTERVAL_MS=1000`

The worker no longer needs direct Redis access. It only needs reachability to the coordinator URL.

### 3. Scale workers

You have two simple options:

- increase `WORKER_CONCURRENCY` on the worker service
- deploy another worker service on the same or another server

For a first real test, one worker service with `WORKER_CONCURRENCY=2` is enough.

### Dokploy API deployment

If you want to drive the rollout through the Dokploy API instead of the UI:

1. Copy `.env.example` to `.env`
2. Fill in:
   - `DOKPLOY_BASE_URL`
   - `DOKPLOY_API_KEY`
   - `DOKPLOY_COORDINATOR_PROJECT_NAME`
   - `DOKPLOY_COORDINATOR_ENVIRONMENT_NAME`
   - `DOKPLOY_COORDINATOR_ENVIRONMENT_ID`
   - `DOKPLOY_COORDINATOR_SERVER_ID`
   - `DOKPLOY_WORKER_PROJECT_NAME`
   - `DOKPLOY_WORKER_ENVIRONMENT_NAME`
   - `DOKPLOY_WORKER_ENVIRONMENT_ID`
   - `DOKPLOY_WORKER_SERVER_ID`
   - `DISPATCH_COORDINATOR_URL`
   - `COORDINATOR_BIND_IP`
   - `DOKPLOY_GIT_URL`
   - `DOKPLOY_GIT_BRANCH`
3. Run:

```powershell
python .\python\tools\dokploy_deploy.py
```

The script:

- creates or updates the coordinator compose service in the existing Dagster project
- creates the worker project and production environment if they do not exist yet
- creates or updates the worker compose service in that separate worker project
- saves the environment variables for each compose service
- triggers deployment for both services

For private-network deployment, set:

- `DISPATCH_COORDINATOR_URL=http://<dagster-private-ip>:4000`
- `COORDINATOR_BIND_IP=<dagster-private-ip>`

The worker service will use that private address to poll the coordinator.

It uses Dokploy's current API flow around `compose.create`, `compose.update`, `compose.saveEnvironment`, and `compose.deploy`:

- https://docs.dokploy.com/docs/api/compose
- https://docs.dokploy.com/docs/api

## Dagster Wiring

Use the coordinator as a normal external service from Dagster. Do not build a custom executor or run launcher for this MVP.

The repo includes:

- `python/dagster_dispatch/client.py`
- `python/dagster_dispatch/resource.py`
- `python/examples/dagster_dispatch_asset.py`
- `prompts/data-engineering-agent-dispatch-resource.md`

The intended pattern is:

1. Copy `python/dagster_dispatch/` into your Dagster code location.
2. Set `DISPATCH_COORDINATOR_URL` in the Dagster deployment.
3. Register `DispatchCoordinatorResource` with `dg.EnvVar("DISPATCH_COORDINATOR_URL")`.
4. Call `dispatch.run_job_and_wait(...)` from the asset or op that should offload work.

Minimal Dagster resource setup:

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
    job = dispatch.run_job_and_wait(
        context,
        "fetch_prices",
        {"symbol": "AAPL"},
    )
    return job
```

If you want your data engineering agent to implement the integration in the real Dagster repo, use:

- `prompts/data-engineering-agent-dispatch-resource.md`

## Real Test Flow

1. Deploy `dokploy-coordinator.compose.yml` to the Dagster server.
2. Confirm the coordinator is reachable with `POST /jobs` and `GET /jobs/:id`.
3. Deploy `dokploy-worker.compose.yml` to the worker server with `COORDINATOR_URL` pointing at the coordinator.
4. In the Dagster deployment, set `DISPATCH_COORDINATOR_URL` to that same coordinator URL.
5. Add the `dagster_dispatch` helper to the Dagster codebase and materialize an asset that calls `dispatch.run_job_and_wait(...)`.
6. Verify the worker logs show the job pickup and `GET /jobs/:id` returns `success`.

## Notes

- The queue is stored in Redis list `jobs:queue`
- In-flight jobs are stored in Redis list `jobs:processing`
- Job state is stored in Redis hashes named `job:<id>`
- Job hashes include `inserted_at`, `started_at`, and `finished_at`
- The only valid state flow is `queued -> running -> success | failed`
- There are no retries, locks, schedules, or UI layers in this MVP
