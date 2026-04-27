from __future__ import annotations

import importlib
import json
import os
import sys
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any


ALLOWLIST_FILE_ENV_VAR = "DISPATCH_CALLABLE_ALLOWLIST_FILE"
ALLOWLIST_JSON_ENV_VAR = "DISPATCH_CALLABLE_ALLOWLIST_JSON"


class CallableRunnerError(RuntimeError):
    """Raised for expected user-facing callable runner failures."""


def main() -> int:
    if len(sys.argv) != 2:
        sys.stderr.write("expected one JSON params argument\n")
        return 1

    try:
        params = parse_params(sys.argv[1])
        result = run_callable(params)
        sys.stdout.write(json.dumps(result, separators=(",", ":"), sort_keys=True))
        return 0
    except CallableRunnerError as exc:
        sys.stderr.write(f"{exc}\n")
        return 1
    except Exception as exc:
        sys.stderr.write(f"{type(exc).__name__}: {exc}\n")
        return 1


def parse_params(raw_params: str) -> dict[str, Any]:
    try:
        params = json.loads(raw_params)
    except json.JSONDecodeError as exc:
        raise CallableRunnerError("params must be valid JSON") from exc

    if not isinstance(params, dict):
        raise CallableRunnerError("params must be a JSON object")

    return params


def run_callable(
    params: Mapping[str, Any],
    *,
    allowlist: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    alias = params.get("callable")
    kwargs = params.get("kwargs", {})

    if not isinstance(alias, str) or not alias:
        raise CallableRunnerError("callable must be a non-empty allowlist alias")

    resolved_allowlist = allowlist if allowlist is not None else load_allowlist()
    target = resolved_allowlist.get(alias)
    if target is None:
        raise CallableRunnerError(f"callable alias is not allowed: {alias}")

    if not isinstance(kwargs, dict):
        raise CallableRunnerError("kwargs must be a JSON object")

    callable_fn = resolve_callable(target)
    result = callable_fn(**kwargs)

    if not isinstance(result, dict):
        raise CallableRunnerError("callable must return a JSON object")

    ensure_json_serializable(result)
    return result


def load_allowlist(
    *,
    file_env_var: str = ALLOWLIST_FILE_ENV_VAR,
    json_env_var: str = ALLOWLIST_JSON_ENV_VAR,
) -> dict[str, str]:
    file_path = os.getenv(file_env_var)
    if file_path is not None and file_path.strip() != "":
        return load_allowlist_from_file(Path(file_path), source=file_env_var)

    raw_value = os.getenv(json_env_var)

    if raw_value is None or raw_value.strip() == "":
        raise CallableRunnerError(
            f"{file_env_var} or {json_env_var} must be set to an allowlist"
        )

    return parse_allowlist_json(raw_value, source=json_env_var)


def load_allowlist_from_file(path: Path, *, source: str) -> dict[str, str]:
    if not path.exists():
        raise CallableRunnerError(f"{source} file does not exist: {path}")

    if not path.is_file():
        raise CallableRunnerError(f"{source} must point to a file: {path}")

    return parse_allowlist_json(path.read_text(encoding="utf-8"), source=str(path))


def parse_allowlist_json(raw_value: str, *, source: str) -> dict[str, str]:
    try:
        decoded = json.loads(raw_value)
    except json.JSONDecodeError as exc:
        raise CallableRunnerError(f"{source} must contain valid JSON") from exc

    if not isinstance(decoded, dict):
        raise CallableRunnerError(f"{source} must be a JSON object")

    allowlist: dict[str, str] = {}
    for alias, target in decoded.items():
        if not isinstance(alias, str) or not alias:
            raise CallableRunnerError(f"{source} contains an invalid alias")

        if not isinstance(target, str) or not target:
            raise CallableRunnerError(f"{source} contains an invalid target for {alias}")

        allowlist[alias] = target

    return allowlist


def resolve_callable(target: str) -> Callable[..., Any]:
    module_name, separator, function_name = target.partition(":")

    if separator != ":" or not module_name or not function_name:
        raise CallableRunnerError(f"invalid allowlist target: {target}")

    module = importlib.import_module(module_name)
    value = getattr(module, function_name, None)

    if not callable(value):
        raise CallableRunnerError(f"allowlist target is not callable: {target}")

    return value


def ensure_json_serializable(value: dict[str, Any]) -> None:
    try:
        json.dumps(value)
    except (TypeError, ValueError) as exc:
        raise CallableRunnerError("callable return value must be JSON serializable") from exc


if __name__ == "__main__":
    raise SystemExit(main())
