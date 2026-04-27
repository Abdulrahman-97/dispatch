Implement the Dagster/stocks side of a project-specific Dispatch worker image.

Context:

- Dispatch is intended to stay generic and reusable.
- Dispatch now supports a generic `python_callable` job type.
- Dagster submits only an allowlisted alias plus JSON kwargs.
- Dispatch must not contain stocks business logic or stocks-specific hardcoded aliases.
- The project-specific worker image should declare what stocks callables it can run.

Target Dispatch request shape:

```json
{
  "job_type": "python_callable",
  "params": {
    "callable": "stocks_tickers_daily_landing",
    "kwargs": {
      "partition_date": "2026-04-24",
      "bucket": "stockbucketus",
      "timeout_seconds": 120
    }
  }
}
```

Callable already exposed by Dagster/stocks:

```text
stocks.defs.tickers_daily.tickers_daily_new:run_tickers_daily_landing_from_env
```

Desired architecture:

- Dispatch core image provides:
  - coordinator/worker runtime
  - `python/scripts/python_callable.py`
  - generic callable runner behavior
- Dagster/stocks derived worker image provides:
  - installed `stocks` Python package
  - callable allowlist file
  - no secrets baked into the image

Please implement this in the Dagster/stocks repo:

1. Add a derived worker image definition, for example:

```text
deploy/dispatch-worker/Dockerfile
deploy/dispatch-worker/dispatch_callables.json
```

2. The derived Dockerfile should be conceptually:

```dockerfile
FROM <dispatch-core-worker-image>

RUN /opt/dispatch-python/bin/python -m pip install .

COPY deploy/dispatch-worker/dispatch_callables.json /etc/dispatch/callables.json

ENV DISPATCH_CALLABLE_ALLOWLIST_FILE=/etc/dispatch/callables.json
```

Adjust the install command to match the actual Dagster/stocks repo layout. The goal is that the worker
can import:

```python
stocks.defs.tickers_daily.tickers_daily_new
```

3. Add `dispatch_callables.json`:

```json
{
  "stocks_tickers_daily_landing": "stocks.defs.tickers_daily.tickers_daily_new:run_tickers_daily_landing_from_env"
}
```

4. Do not bake secrets into the image.

Runtime env vars should remain deployment-owned:

```text
COORDINATOR_URL
WORKER_NAME
WORKER_CONCURRENCY
FMP_API_KEY or API_KEY
BUCKET_NAME or S3_BUCKET
AWS credentials or IAM role access
AWS_REGION / AWS_DEFAULT_REGION as needed
```

5. Add a README/runbook section explaining:

- the image owns capabilities:
  - installed stocks package
  - callable allowlist
- Dokploy env owns runtime config/secrets
- Dagster passes only business kwargs such as `partition_date`, `bucket`, and `timeout_seconds`

6. Add a lightweight test if practical:

- verify `dispatch_callables.json` contains `stocks_tickers_daily_landing`
- verify the target module/function can be imported in the repo test environment
- do not call FMP or S3 in this test

Important constraints:

- Do not copy business logic into Dispatch.
- Do not add job-specific Dispatch scripts.
- Do not pass secrets through Dispatch job params.
- Do not require `DISPATCH_CALLABLE_ALLOWLIST_JSON` for this production path; prefer the allowlist file baked into the derived image.

Dispatch-side support now exists:

- Dispatch supports `DISPATCH_CALLABLE_ALLOWLIST_FILE=/etc/dispatch/callables.json` with this load order:
  1. file if set
  2. JSON env if set
  3. clear failure if neither is set

Expected deliverables:

- derived worker Dockerfile
- `dispatch_callables.json`
- minimal docs/runbook update
- import/allowlist test
- exact image name/tag recommendation for Dokploy
