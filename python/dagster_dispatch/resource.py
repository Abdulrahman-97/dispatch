from __future__ import annotations

from typing import Any

import dagster as dg

from dagster_dispatch.client import DispatchClient, DispatchTimeoutError


class DispatchCoordinatorResource(dg.ConfigurableResource):
    base_url: str
    request_timeout_seconds: float = 30.0
    poll_interval_seconds: float = 2.0
    job_timeout_seconds: float = 900.0

    def client(self) -> DispatchClient:
        return DispatchClient(
            base_url=self.base_url,
            request_timeout_seconds=self.request_timeout_seconds,
            poll_interval_seconds=self.poll_interval_seconds,
        )

    def submit_job(self, job_type: str, params: dict[str, Any] | None = None) -> str:
        return self.client().submit_job(job_type, params)

    def get_job(self, job_id: str) -> dict[str, Any]:
        return self.client().get_job(job_id)

    def run_job_and_wait(
        self,
        context: dg.AssetExecutionContext | dg.OpExecutionContext,
        job_type: str,
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        client = self.client()
        job_id = client.submit_job(job_type, params)
        context.log.info(f"dispatch job submitted: {job_id}")

        try:
            job = client.wait_for_completion(job_id, timeout_seconds=self.job_timeout_seconds)
        except DispatchTimeoutError as exc:
            raise dg.Failure(
                description=f"dispatch job {job_id} timed out after {self.job_timeout_seconds} seconds"
            ) from exc

        status = job.get("status")

        if status == "failed":
            error_message = job.get("error") or "job failed"
            raise dg.Failure(description=f"dispatch job {job_id} failed: {error_message}")

        return job
