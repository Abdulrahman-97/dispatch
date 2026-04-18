from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any
from urllib import error, parse, request


ROOT_DIR = Path(__file__).resolve().parents[2]
ENV_FILE = ROOT_DIR / ".env"
COORDINATOR_COMPOSE_FILE = ROOT_DIR / "dokploy-coordinator.compose.yml"
WORKER_COMPOSE_FILE = ROOT_DIR / "dokploy-worker.compose.yml"


class DokployError(RuntimeError):
    """Raised when a Dokploy API operation fails."""


class DokployClient:
    def __init__(self, base_url: str, api_key: str, timeout_seconds: float = 30.0) -> None:
        self._base_url = base_url.rstrip("/")
        self._api_key = api_key
        self._timeout_seconds = timeout_seconds

    def get(self, endpoint: str, query: dict[str, str] | None = None) -> Any:
        return self._request("GET", endpoint, payload=None, query=query)

    def post(self, endpoint: str, payload: dict[str, Any]) -> Any:
        return self._request("POST", endpoint, payload=payload)

    def _request(
        self,
        method: str,
        endpoint: str,
        *,
        payload: dict[str, Any] | None = None,
        query: dict[str, str] | None = None,
    ) -> Any:
        url = f"{self._base_url}/api/{endpoint}"

        if query:
            url = f"{url}?{parse.urlencode(query)}"

        headers = {
            "accept": "application/json",
            "content-type": "application/json",
            "x-api-key": self._api_key,
        }

        body: bytes | None = None

        if payload is not None:
            body = json.dumps(payload).encode("utf-8")

        req = request.Request(url=url, method=method, headers=headers, data=body)

        try:
            with request.urlopen(req, timeout=self._timeout_seconds) as response:
                raw_body = response.read().decode("utf-8")
        except error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")
            raise DokployError(
                f"{endpoint} failed with status {exc.code}: {detail or exc.reason}"
            ) from exc
        except error.URLError as exc:
            raise DokployError(f"{endpoint} failed: {exc.reason}") from exc

        if not raw_body:
            return None

        try:
            return json.loads(raw_body)
        except json.JSONDecodeError as exc:
            raise DokployError(f"{endpoint} returned invalid JSON") from exc


def main() -> int:
    load_env_file(ENV_FILE)

    client = DokployClient(
        base_url=required_env("DOKPLOY_BASE_URL"),
        api_key=required_env("DOKPLOY_API_KEY"),
    )

    coordinator_environment_id = resolve_environment_id(
        client=client,
        project_name=required_env("DOKPLOY_COORDINATOR_PROJECT_NAME"),
        environment_name=os.environ.get("DOKPLOY_COORDINATOR_ENVIRONMENT_NAME", "production"),
        explicit_environment_id=os.environ.get("DOKPLOY_COORDINATOR_ENVIRONMENT_ID"),
        create_project_if_missing=False,
    )

    worker_environment_id = resolve_environment_id(
        client=client,
        project_name=required_env("DOKPLOY_WORKER_PROJECT_NAME"),
        environment_name=os.environ.get("DOKPLOY_WORKER_ENVIRONMENT_NAME", "production"),
        explicit_environment_id=os.environ.get("DOKPLOY_WORKER_ENVIRONMENT_ID"),
        create_project_if_missing=True,
    )

    coordinator_compose_id = upsert_compose(
        client=client,
        environment_id=coordinator_environment_id,
        server_id=required_env("DOKPLOY_COORDINATOR_SERVER_ID"),
        name=os.environ.get("DOKPLOY_COORDINATOR_COMPOSE_NAME", "dispatch-coordinator"),
        compose_file=COORDINATOR_COMPOSE_FILE,
        compose_path="dokploy-coordinator.compose.yml",
        env_text=build_coordinator_env(),
    )

    worker_compose_id = upsert_compose(
        client=client,
        environment_id=worker_environment_id,
        server_id=required_env("DOKPLOY_WORKER_SERVER_ID"),
        name=os.environ.get("DOKPLOY_WORKER_COMPOSE_NAME", "dispatch-worker"),
        compose_file=WORKER_COMPOSE_FILE,
        compose_path="dokploy-worker.compose.yml",
        env_text=build_worker_env(),
    )

    print(
        json.dumps(
            {
                "coordinator_environment_id": coordinator_environment_id,
                "coordinator_compose_id": coordinator_compose_id,
                "worker_environment_id": worker_environment_id,
                "worker_compose_id": worker_compose_id,
            },
            indent=2,
        )
    )
    return 0


def resolve_environment_id(
    *,
    client: DokployClient,
    project_name: str,
    environment_name: str,
    explicit_environment_id: str | None,
    create_project_if_missing: bool,
) -> str:
    if explicit_environment_id and explicit_environment_id != "replace_me":
        return explicit_environment_id

    project_id = find_project_id(client, project_name)

    if project_id is None:
        if not create_project_if_missing:
            raise DokployError(f"project not found: {project_name}")

        client.post("project.create", {"name": project_name})
        project_id = find_project_id(client, project_name)

    if project_id is None:
        raise DokployError(f"unable to resolve project id for {project_name}")

    environment_id = find_environment_id(client, project_id=project_id, environment_name=environment_name)

    if environment_id is None:
        client.post(
            "environment.create",
            {
                "name": environment_name,
                "projectId": project_id,
            },
        )
        environment_id = find_environment_id(
            client,
            project_id=project_id,
            environment_name=environment_name,
        )

    if environment_id is None:
        raise DokployError(
            f"unable to resolve environment id for {project_name}/{environment_name}"
        )

    return environment_id


def find_project_id(client: DokployClient, project_name: str) -> str | None:
    payload = client.get("project.all")

    if not isinstance(payload, list):
        return None

    for item in payload:
        if not isinstance(item, dict):
            continue

        if item.get("name") == project_name:
            project_id = item.get("projectId") or item.get("id")
            if isinstance(project_id, str) and project_id:
                return project_id

    return None


def find_environment_id(
    client: DokployClient,
    *,
    project_id: str,
    environment_name: str,
) -> str | None:
    payload = client.get("environment.byProjectId", query={"projectId": project_id})
    items = extract_items(payload)

    for item in items:
        if not isinstance(item, dict):
            continue

        if item.get("name") == environment_name:
            environment_id = item.get("environmentId") or item.get("id")
            if isinstance(environment_id, str) and environment_id:
                return environment_id

    return None


def upsert_compose(
    *,
    client: DokployClient,
    environment_id: str,
    server_id: str,
    name: str,
    compose_file: Path,
    compose_path: str,
    env_text: str,
) -> str:
    compose_text = compose_file.read_text(encoding="utf-8")
    compose_id = find_compose_id(client, environment_id=environment_id, name=name)

    if compose_id is None:
        client.post(
            "compose.create",
            {
                "name": name,
                "environmentId": environment_id,
                "serverId": server_id,
                "composeType": "docker-compose",
                "composeFile": compose_text,
            },
        )
        compose_id = find_compose_id(client, environment_id=environment_id, name=name)

    if compose_id is None:
        raise DokployError(f"unable to resolve compose id for {name}")

    client.post(
        "compose.update",
        {
            "composeId": compose_id,
            "name": name,
            "composeType": "docker-compose",
            "sourceType": os.environ.get("DOKPLOY_GIT_SOURCE_TYPE", "git"),
            "customGitUrl": required_env("DOKPLOY_GIT_URL"),
            "customGitBranch": os.environ.get("DOKPLOY_GIT_BRANCH", "main"),
            "composePath": compose_path,
            "composeFile": compose_text,
        },
    )
    client.post("compose.saveEnvironment", {"composeId": compose_id, "env": env_text})
    client.post("compose.deploy", {"composeId": compose_id})

    return compose_id


def find_compose_id(client: DokployClient, *, environment_id: str, name: str) -> str | None:
    payload = client.get(
        "compose.search",
        query={"environmentId": environment_id, "name": name, "limit": "100"},
    )
    items = extract_items(payload)

    for item in items:
        if not isinstance(item, dict):
            continue

        if item.get("name") == name and item.get("environmentId") == environment_id:
            compose_id = item.get("composeId") or item.get("id")
            if isinstance(compose_id, str) and compose_id:
                return compose_id

    return None


def extract_items(payload: Any) -> list[Any]:
    if isinstance(payload, list):
        return payload

    if isinstance(payload, dict):
        for key in ("items", "results", "data", "composes", "environments"):
            value = payload.get(key)
            if isinstance(value, list):
                return value

    return []


def build_coordinator_env() -> str:
    return "\n".join(
        [
            "REDIS_URL=redis://redis:6379/0",
            f"COORDINATOR_BIND_IP={required_env('COORDINATOR_BIND_IP')}",
            "",
        ]
    )


def build_worker_env() -> str:
    coordinator_url = required_env("DISPATCH_COORDINATOR_URL")
    worker_name = os.environ.get("DISPATCH_WORKER_NAME", "worker-server-1")
    worker_concurrency = os.environ.get("DISPATCH_WORKER_CONCURRENCY", "5")
    poll_interval = os.environ.get("DISPATCH_WORKER_POLL_INTERVAL_MS", "1000")

    return "\n".join(
        [
            f"COORDINATOR_URL={coordinator_url}",
            f"WORKER_NAME={worker_name}",
            f"WORKER_CONCURRENCY={worker_concurrency}",
            f"WORKER_POLL_INTERVAL_MS={poll_interval}",
            "",
        ]
    )


def required_env(name: str) -> str:
    value = os.environ.get(name)
    if value is None or value == "":
        raise DokployError(f"missing required environment variable: {name}")
    return value


def load_env_file(path: Path) -> None:
    if not path.exists():
        return

    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()

        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue

        key, raw_value = stripped.split("=", 1)
        key = key.strip()
        value = raw_value.strip().strip("'").strip('"')

        if key and key not in os.environ:
            os.environ[key] = value


if __name__ == "__main__":
    raise SystemExit(main())
