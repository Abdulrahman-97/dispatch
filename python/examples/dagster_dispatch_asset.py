from __future__ import annotations

import json
from typing import Any

import dagster as dg

from dagster_dispatch import DispatchCoordinatorResource


def _parse_result(result_text: str | None) -> dict[str, Any]:
    if result_text is None:
        return {}

    stripped = result_text.strip()

    if not stripped.startswith("{"):
        return {"raw_result": result_text}

    try:
        decoded = json.loads(stripped)
    except json.JSONDecodeError:
        return {"raw_result": result_text}

    if isinstance(decoded, dict):
        return decoded

    return {"raw_result": result_text}


@dg.asset(group_name="dispatch")
def remote_fetch_prices(
    context: dg.AssetExecutionContext,
    dispatch: DispatchCoordinatorResource,
) -> dg.MaterializeResult:
    job = dispatch.run_job_and_wait(
        context,
        "fetch_prices",
        {"symbol": "AAPL"},
    )
    payload = _parse_result(job.get("result"))

    return dg.MaterializeResult(
        value=payload,
        metadata={
            "dispatch_job_id": job["job_id"],
            "dispatch_status": job["status"],
            "dispatch_error": job.get("error"),
        },
    )


defs = dg.Definitions(
    assets=[remote_fetch_prices],
    resources={
        "dispatch": DispatchCoordinatorResource(
            base_url=dg.EnvVar("DISPATCH_COORDINATOR_URL"),
        )
    },
)
