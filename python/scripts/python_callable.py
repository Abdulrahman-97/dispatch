from __future__ import annotations

import importlib
import json
import os
import sys
from collections.abc import Callable, Mapping
from typing import Any


ALLOWLIST_ENV_VAR = "DISPATCH_CALLABLE_ALLOWLIST_JSON"


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

    resolved_allowlist = allowlist if allowlist is not None else load_allowlist_from_env()
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


def load_allowlist_from_env(env_var: str = ALLOWLIST_ENV_VAR) -> dict[str, str]:
    raw_value = os.getenv(env_var)

    if raw_value is None or raw_value.strip() == "":
        raise CallableRunnerError(f"{env_var} must be set to a JSON object")

    try:
        decoded = json.loads(raw_value)
    except json.JSONDecodeError as exc:
        raise CallableRunnerError(f"{env_var} must contain valid JSON") from exc

    if not isinstance(decoded, dict):
        raise CallableRunnerError(f"{env_var} must be a JSON object")

    allowlist: dict[str, str] = {}
    for alias, target in decoded.items():
        if not isinstance(alias, str) or not alias:
            raise CallableRunnerError(f"{env_var} contains an invalid alias")

        if not isinstance(target, str) or not target:
            raise CallableRunnerError(f"{env_var} contains an invalid target for {alias}")

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
