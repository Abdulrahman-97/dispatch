from __future__ import annotations

import json
import time
from dataclasses import dataclass
from typing import Any
from urllib import error, parse, request


JobPayload = dict[str, Any]


class DispatchError(RuntimeError):
    """Base error raised by the dispatch coordinator client."""


class DispatchJobFailed(DispatchError):
    """Raised when a remote job reaches the failed state."""


class DispatchTimeoutError(DispatchError):
    """Raised when a remote job does not finish before the timeout."""


@dataclass(frozen=True, slots=True)
class DispatchClient:
    base_url: str
    request_timeout_seconds: float = 30.0
    poll_interval_seconds: float = 2.0

    def submit_job(self, job_type: str, params: JobPayload | None = None) -> str:
        payload = {"job_type": job_type, "params": params or {}}
        response = self._request_json("POST", "/jobs", payload)
        job_id = response.get("job_id")

        if not isinstance(job_id, str) or not job_id:
            raise DispatchError("coordinator response missing job_id")

        return job_id

    def get_job(self, job_id: str) -> JobPayload:
        encoded_job_id = parse.quote(job_id, safe="")
        return self._request_json("GET", f"/jobs/{encoded_job_id}")

    def wait_for_completion(self, job_id: str, timeout_seconds: float) -> JobPayload:
        deadline = time.monotonic() + timeout_seconds

        while True:
            job = self.get_job(job_id)
            status = job.get("status")

            if status in {"success", "failed"}:
                return job

            if time.monotonic() >= deadline:
                raise DispatchTimeoutError(f"job {job_id} did not finish within {timeout_seconds} seconds")

            time.sleep(self.poll_interval_seconds)

    def run_job_and_wait(
        self,
        job_type: str,
        params: JobPayload | None = None,
        *,
        timeout_seconds: float,
    ) -> JobPayload:
        job_id = self.submit_job(job_type, params)
        job = self.wait_for_completion(job_id, timeout_seconds)
        status = job.get("status")

        if status == "failed":
            error_message = job.get("error") or "job failed"
            raise DispatchJobFailed(f"job {job_id} failed: {error_message}")

        return job

    def _request_json(
        self,
        method: str,
        path: str,
        payload: JobPayload | None = None,
    ) -> JobPayload:
        body: bytes | None = None
        headers = {"content-type": "application/json"}

        if payload is not None:
            body = json.dumps(payload).encode("utf-8")

        req = request.Request(
            url=f"{self.base_url.rstrip('/')}{path}",
            data=body,
            headers=headers,
            method=method,
        )

        try:
            with request.urlopen(req, timeout=self.request_timeout_seconds) as response:
                raw_body = response.read().decode("utf-8")
        except error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")
            raise DispatchError(
                f"dispatch request failed with status {exc.code}: {detail or exc.reason}"
            ) from exc
        except error.URLError as exc:
            raise DispatchError(f"dispatch request failed: {exc.reason}") from exc

        if not raw_body:
            raise DispatchError("coordinator returned an empty response body")

        try:
            decoded = json.loads(raw_body)
        except json.JSONDecodeError as exc:
            raise DispatchError("coordinator returned invalid JSON") from exc

        if not isinstance(decoded, dict):
            raise DispatchError("coordinator response must be a JSON object")

        return decoded
