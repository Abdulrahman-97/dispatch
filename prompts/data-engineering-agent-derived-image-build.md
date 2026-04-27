Update the Dagster/stocks derived Dispatch worker image build to use the published Dispatch core
image from GHCR.

Context:

- Dispatch core now publishes a generic worker image from the Dispatch repo.
- GitHub Actions workflow:
  - `.github/workflows/publish-core-image.yml`
- Published image names:
  - `ghcr.io/abdulrahman-97/dispatch-core-worker:main`
  - `ghcr.io/abdulrahman-97/dispatch-core-worker:sha-<dispatch-git-sha>`
- The derived Dagster/stocks worker image should build from one of those tags.

Recommended production pattern:

- Use the immutable `sha-<dispatch-git-sha>` tag in the derived worker build.
- Avoid `main` except for quick development tests.

Current Dagster/stocks derived Dockerfile already has:

```dockerfile
ARG DISPATCH_CORE_WORKER_IMAGE=dispatch-core-worker:latest
FROM ${DISPATCH_CORE_WORKER_IMAGE}
```

Please update the Dagster/stocks build/runbook so the derived image is built with:

```bash
docker build \
  -f deploy/dispatch-worker/Dockerfile \
  --build-arg DISPATCH_CORE_WORKER_IMAGE=ghcr.io/abdulrahman-97/dispatch-core-worker:sha-<dispatch-git-sha> \
  -t ghcr.io/abdulrahman-97/dagster-stocks-dispatch-worker:<stocks-git-sha> \
  .
```

The derived image must still:

- install the local `stocks` package
- copy `deploy/dispatch-worker/dispatch_callables.json` to `/etc/dispatch/callables.json`
- set `DISPATCH_CALLABLE_ALLOWLIST_FILE=/etc/dispatch/callables.json`
- bake no secrets

After building, verify:

```bash
docker run --rm ghcr.io/abdulrahman-97/dagster-stocks-dispatch-worker:<stocks-git-sha> \
  /opt/dispatch-python/bin/python -c "import stocks; print('stocks import ok')"

docker run --rm ghcr.io/abdulrahman-97/dagster-stocks-dispatch-worker:<stocks-git-sha> \
  sh -c "cat /etc/dispatch/callables.json && test -n \"$DISPATCH_CALLABLE_ALLOWLIST_FILE\""
```

Dokploy should run the derived image, not the generic Dispatch image.

Worker runtime env remains deployment-owned:

```text
APP_ROLE=worker
COORDINATOR_URL=http://10.77.0.2:4000
WORKER_NAME=worker-server-1
WORKER_CONCURRENCY=2
PYTHON_ROOT=/app/python
PYTHON_BIN=/opt/dispatch-python/bin/python
FMP_API_KEY or API_KEY
BUCKET_NAME or S3_BUCKET
AWS credentials or IAM role access
AWS_REGION / AWS_DEFAULT_REGION if needed
```

Do not pass secrets in Dispatch job params.
