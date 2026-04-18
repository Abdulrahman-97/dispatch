Implement the Dagster integration for the existing Dispatch coordinator.

Context:

- The dispatcher already exists and is deployed separately.
- Coordinator API:
  - `POST /jobs`
  - `GET /jobs/:id`
- Request to submit a job:
  - `{"job_type":"fetch_prices","params":{"symbol":"AAPL"}}`
- Status response:
  - `{"job_id":"...","status":"queued|running|success|failed","result":"...","error":"..."}`
- The coordinator URL must come from `DISPATCH_COORDINATOR_URL`.
- This is not a Dagster executor or run launcher. Keep it as a thin service integration.

What to implement:

1. Add a small Python client around the coordinator API.
2. Add a Dagster `ConfigurableResource` that:
   - reads `DISPATCH_COORDINATOR_URL` via `dg.EnvVar`
   - can submit a job
   - can poll until completion
   - raises `dg.Failure` if the remote job fails or times out
3. Add one concrete example asset or op that offloads work through this resource.
4. Keep dependencies minimal. Prefer the standard library or whatever HTTP client the repo already uses.
5. Make timeout and poll interval configurable.

Behavior requirements:

- Submit:
  - `POST /jobs`
- Poll:
  - `GET /jobs/:id` until `success` or `failed`
- On `success`:
  - return the parsed result if it is JSON
  - otherwise return the raw string
- On `failed`:
  - raise `dg.Failure` with the remote error text
- On timeout:
  - raise `dg.Failure`

Constraints:

- Do not introduce a custom executor.
- Do not change the Dispatch API.
- Keep the implementation explicit and easy to debug.
- Use environment variables for deployment-specific configuration.

Suggested deliverables:

- `dispatch_client.py`
- `dispatch_resource.py`
- one example asset/op using the resource
- any minimal docs or `.env.example` update needed
