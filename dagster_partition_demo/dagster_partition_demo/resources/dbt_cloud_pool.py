import json
import random
import re
import threading
import time
from hashlib import sha256
from typing import Any

import dagster as dg
import httpx
from dagster import AssetExecutionContext
from pydantic import Field, PrivateAttr

ACTIVE_RUN_STATUSES = [1, 2, 3]
SUCCESS_STATUS = 10
ERROR_STATUS = 20
CANCELLED_STATUS = 30
STATUS_LABELS = {
    1: "Queued",
    2: "Starting",
    3: "Running",
    10: "Success",
    20: "Error",
    30: "Cancelled",
}
class DbtCloudJobPool(dg.ConfigurableResource):
    """Manages a shared in-process pool of dbt Cloud jobs for partition fan-out."""

    account_id: str = Field(description="dbt Cloud account id.")
    api_token: str = Field(description="dbt Cloud API token.")
    project_id: int = Field(default=406315)
    environment_id: int | None = Field(
        default=None,
        description="Optional dbt Cloud environment id to scope pool jobs.",
    )
    job_prefix: str = Field(default="partition_runner")
    poll_interval_seconds: int = Field(default=15)
    acquire_timeout_seconds: int = Field(default=300)
    dbt_cloud_base_url: str = Field(default="https://tk626.us1.dbt.com")

    _pool: dict[int, str] = PrivateAttr(default_factory=dict)
    _lock: threading.Lock = PrivateAttr(default_factory=threading.Lock)
    _function_relation_cache: dict[str, str] = PrivateAttr(default_factory=dict)
    _model_relation_cache: dict[str, str] = PrivateAttr(default_factory=dict)

    def _request(self, method: str, path: str, **kwargs: Any) -> dict[str, Any]:
        headers = kwargs.pop("headers", {})
        headers.update({"Authorization": f"Token {self.api_token}"})
        url = f"{self.dbt_cloud_base_url.rstrip('/')}{path}"

        retries = 3
        delay_seconds = 1
        for attempt in range(1, retries + 1):
            try:
                response = httpx.request(
                    method=method,
                    url=url,
                    headers=headers,
                    timeout=30.0,
                    **kwargs,
                )
                response.raise_for_status()
                return response.json()
            except httpx.HTTPStatusError as exc:
                retryable = exc.response.status_code in {429, 500, 502, 503, 504}
                if attempt == retries or not retryable:
                    raise
            except httpx.RequestError:
                if attempt == retries:
                    raise

            time.sleep(delay_seconds)
            delay_seconds *= 2

        raise RuntimeError("Unreachable: request retry loop exhausted.")

    def _list_pool_jobs(self) -> list[dict[str, Any]]:
        response = self._request(
            "GET",
            f"/api/v2/accounts/{self.account_id}/jobs/",
            params={
                "project_id": self.project_id,
                "name__icontains": self.job_prefix,
                "limit": 100,
                "offset": 0,
            },
        )
        jobs = response.get("data", [])
        scoped_jobs = [job for job in jobs if str(job.get("name", "")).startswith(self.job_prefix)]
        if self.environment_id is not None:
            scoped_jobs = [
                job
                for job in scoped_jobs
                if int(job.get("environment_id", -1)) == int(self.environment_id)
            ]
        return scoped_jobs

    def _list_runs_for_job(self, job_id: int, statuses: list[int], limit: int = 20) -> list[dict[str, Any]]:
        response = self._request(
            "GET",
            f"/api/v2/accounts/{self.account_id}/runs/",
            params={
                "job_definition_id": job_id,
                "status": statuses,
                "limit": limit,
                "offset": 0,
            },
        )
        return response.get("data", [])

    def _get_run_detail(self, run_id: int) -> dict[str, Any]:
        response = self._request(
            "GET",
            f"/api/v2/accounts/{self.account_id}/runs/{run_id}/",
            params={"include_related": "trigger"},
        )
        return response.get("data", {})

    def _extract_partition_id_from_run(self, run_detail: dict[str, Any]) -> str | None:
        trigger = run_detail.get("trigger") or {}
        cause = str(trigger.get("cause") or "")
        match = re.search(r"partition_id=([A-Za-z0-9_-]+)", cause)
        if match:
            return match.group(1)

        steps = trigger.get("steps_override") or []
        for step in steps:
            step_str = str(step)
            step_match = re.search(r'"partition_id"\s*:\s*"([A-Za-z0-9_-]+)"', step_str)
            if step_match:
                return step_match.group(1)
        return None

    def get_successful_run_ids_for_partition(self, partition_id: str) -> list[int]:
        matched_run_ids: list[int] = []
        jobs = self._list_pool_jobs()
        for job in jobs:
            job_id = int(job["id"])
            runs = self._list_runs_for_job(job_id=job_id, statuses=[SUCCESS_STATUS], limit=20)
            for run in runs:
                run_id = int(run["id"])
                run_detail = self._get_run_detail(run_id)
                run_partition_id = self._extract_partition_id_from_run(run_detail)
                if run_partition_id == partition_id:
                    matched_run_ids.append(run_id)
        return sorted(set(matched_run_ids), reverse=True)

    def get_successful_run_ids_from_pool(self) -> list[int]:
        run_ids: list[int] = []
        jobs = self._list_pool_jobs()
        for job in jobs:
            job_id = int(job["id"])
            runs = self._list_runs_for_job(job_id=job_id, statuses=[SUCCESS_STATUS], limit=5)
            run_ids.extend(int(run["id"]) for run in runs if run.get("id") is not None)
        return sorted(set(run_ids), reverse=True)

    def get_function_relation_from_run(self, run_id: int, function_name: str) -> str | None:
        manifest = self._request(
            "GET",
            f"/api/v2/accounts/{self.account_id}/runs/{run_id}/artifacts/manifest.json",
        )
        nodes = manifest.get("nodes", {})
        function_nodes = manifest.get("functions", {})
        fn_lower = function_name.lower()
        # dbt manifests can expose functions in top-level `functions`
        # (newer versions) and/or in `nodes` (older shapes).
        for node in list(nodes.values()) + list(function_nodes.values()):
            if node.get("resource_type") != "function":
                continue
            if str(node.get("name", "")).lower() != fn_lower:
                continue
            relation_name = node.get("relation_name")
            if relation_name:
                return str(relation_name)
            database = node.get("database")
            schema = node.get("schema")
            if database and schema:
                return f"{database}.{schema}.{function_name}"
        return None

    def resolve_function_relation_from_cloud(self, function_name: str, partition_id: str) -> str:
        cache_key = f"{function_name}:{partition_id}"
        if cache_key in self._function_relation_cache:
            return self._function_relation_cache[cache_key]

        candidate_run_ids = self.get_successful_run_ids_for_partition(partition_id)
        if not candidate_run_ids:
            candidate_run_ids = self.get_successful_run_ids_from_pool()
        if not candidate_run_ids:
            raise dg.Failure(
                description=(
                    f"Unable to resolve dbt function relation for {function_name}: "
                    "no successful dbt Cloud runs found in pool jobs."
                )
            )

        for run_id in candidate_run_ids:
            relation = self.get_function_relation_from_run(run_id=run_id, function_name=function_name)
            if relation:
                self._function_relation_cache[cache_key] = relation
                return relation

        raise dg.Failure(
            description=(
                f"Function '{function_name}' not found in manifest artifacts "
                "for recent successful runs."
            )
        )

    def get_model_relation_from_run(self, run_id: int, model_name: str) -> str | None:
        manifest = self._request(
            "GET",
            f"/api/v2/accounts/{self.account_id}/runs/{run_id}/artifacts/manifest.json",
        )
        nodes = manifest.get("nodes", {})
        model_lower = model_name.lower()
        for node in nodes.values():
            if node.get("resource_type") != "model":
                continue
            if str(node.get("name", "")).lower() != model_lower:
                continue
            relation_name = node.get("relation_name")
            if relation_name:
                return str(relation_name)
            database = node.get("database")
            schema = node.get("schema")
            alias = node.get("alias") or model_name
            if database and schema:
                return f"{database}.{schema}.{alias}"
        return None

    def resolve_model_relation_from_cloud(self, model_name: str, partition_id: str) -> str:
        cache_key = f"{model_name}:{partition_id}"
        if cache_key in self._model_relation_cache:
            return self._model_relation_cache[cache_key]

        candidate_run_ids = self.get_successful_run_ids_for_partition(partition_id)
        if not candidate_run_ids:
            candidate_run_ids = self.get_successful_run_ids_from_pool()
        if not candidate_run_ids:
            raise dg.Failure(
                description=(
                    f"Unable to resolve dbt model relation for {model_name}: "
                    "no successful dbt Cloud runs found in pool jobs."
                )
            )

        for run_id in candidate_run_ids:
            relation = self.get_model_relation_from_run(run_id=run_id, model_name=model_name)
            if relation:
                self._model_relation_cache[cache_key] = relation
                return relation

        raise dg.Failure(
            description=(
                f"Model '{model_name}' not found in manifest artifacts for recent successful runs."
            )
        )

    def _job_has_active_run(self, job_id: int) -> bool:
        response = self._request(
            "GET",
            f"/api/v2/accounts/{self.account_id}/runs/",
            params={
                "job_definition_id": job_id,
                "status": ACTIVE_RUN_STATUSES,
                "limit": 1,
                "offset": 0,
            },
        )
        pagination = response.get("extra", {}).get("pagination", {})
        total_count = pagination.get("total_count")
        if total_count is not None:
            return total_count > 0
        return len(response.get("data", [])) > 0

    def discover_pool(self) -> dict[int, str]:
        jobs = self._list_pool_jobs()
        if not jobs:
            env_hint = (
                f"environment_id={self.environment_id}, " if self.environment_id is not None else ""
            )
            raise dg.Failure(
                description=(
                    "No dbt Cloud pool jobs found. "
                    f"Check job_prefix={self.job_prefix}, {env_hint}project_id={self.project_id}."
                )
            )
        pool: dict[int, str] = {}

        for job in jobs:
            job_id = int(job["id"])
            pool[job_id] = "busy" if self._job_has_active_run(job_id) else "idle"

        with self._lock:
            self._pool = pool

        return dict(pool)

    def acquire_job(self, selection_hint: str | None = None) -> int:
        with self._lock:
            idle_job_ids = [job_id for job_id, state in sorted(self._pool.items()) if state == "idle"]
            if idle_job_ids:
                job_count = len(idle_job_ids)
                if selection_hint:
                    digest = sha256(selection_hint.encode("utf-8")).digest()
                    base_idx = int.from_bytes(digest[:8], "big") % job_count
                else:
                    base_idx = 0
                jitter = random.randrange(job_count)
                # Hybrid strategy: stable hint-based spread plus random jitter to avoid herding.
                selected_job_id = idle_job_ids[(base_idx + jitter) % job_count]
                self._pool[selected_job_id] = "busy"
                return selected_job_id
        raise RuntimeError("No idle dbt Cloud pool jobs available.")

    def wait_and_acquire(
        self, timeout_seconds: int | None = None, selection_hint: str | None = None
    ) -> int:
        timeout = timeout_seconds if timeout_seconds is not None else self.acquire_timeout_seconds
        start = time.time()
        sleep_seconds = 2

        while time.time() - start < timeout:
            # Always refresh from API because workers run in separate processes.
            self.discover_pool()

            try:
                return self.acquire_job(selection_hint=selection_hint)
            except RuntimeError:
                time.sleep(sleep_seconds)
                sleep_seconds = min(sleep_seconds * 2, 15)

        raise RuntimeError(
            "Timed out waiting for an idle dbt Cloud pool job after "
            f"{timeout} seconds."
        )

    def release_job(self, job_id: int) -> None:
        with self._lock:
            if job_id in self._pool:
                self._pool[job_id] = "idle"

    def trigger_run(self, job_id: int, partition_id: str) -> int:
        vars_payload = json.dumps({"partition_id": partition_id})
        steps = [f"dbt build --select +address_hygiene_pending --vars '{vars_payload}'"]
        return self.trigger_run_custom(
            job_id=job_id,
            steps=steps,
            cause=f"Dagster partition processing: partition_id={partition_id}",
        )

    def trigger_run_custom(self, job_id: int, steps: list[str], cause: str = "Dagster trigger") -> int:
        response = self._request(
            "POST",
            f"/api/v2/accounts/{self.account_id}/jobs/{job_id}/run/",
            json={
                "cause": cause,
                "steps_override": steps,
            },
        )
        return int(response["data"]["id"])

    def poll_run(self, run_id: int, context: AssetExecutionContext | None = None) -> dict[str, Any]:
        while True:
            response = self._request(
                "GET",
                f"/api/v2/accounts/{self.account_id}/runs/{run_id}/",
                params={"include_related": "run_steps"},
            )
            data = response["data"]
            if data.get("is_complete"):
                return data

            if context:
                context.log.info(
                    "dbt Cloud run %s in progress (status=%s)",
                    run_id,
                    data.get("status_humanized", STATUS_LABELS.get(data.get("status"), "Unknown")),
                )
            time.sleep(self.poll_interval_seconds)

    def run_partition(self, partition_id: str, context: AssetExecutionContext | None = None) -> dict[str, Any]:
        deadline = time.time() + self.acquire_timeout_seconds
        trigger_retry_sleep = 2

        while True:
            remaining = deadline - time.time()
            if remaining <= 0:
                raise dg.Failure(
                    description=(
                        f"Timed out triggering dbt run for partition {partition_id} "
                        f"after {self.acquire_timeout_seconds} seconds."
                    )
                )

            job_id = self.wait_and_acquire(
                timeout_seconds=max(1, int(remaining)),
                selection_hint=partition_id,
            )
            try:
                run_id = self.trigger_run(job_id=job_id, partition_id=partition_id)
                result = self.poll_run(run_id=run_id, context=context)
                break
            except httpx.HTTPStatusError as exc:
                status_code = exc.response.status_code
                # If two workers race and queue on same job, retry another pool job.
                if status_code in {400, 409}:
                    self.discover_pool()
                    if context:
                        context.log.warning(
                            "Trigger contention for partition %s on job %s (status=%s). Retrying another job.",
                            partition_id,
                            job_id,
                            status_code,
                        )
                    time.sleep(trigger_retry_sleep)
                    trigger_retry_sleep = min(trigger_retry_sleep * 2, 10)
                    continue
                raise
            finally:
                self.release_job(job_id)

        status_code = int(result.get("status", -1))
        status_humanized = result.get("status_humanized", STATUS_LABELS.get(status_code, "Unknown"))
        output = {
            "id": int(result.get("id", run_id)),
            "job_id": job_id,
            "status": status_code,
            "status_humanized": status_humanized,
            "duration_humanized": result.get("duration_humanized"),
            "href": result.get("href"),
        }

        if status_code == SUCCESS_STATUS:
            return output

        if status_code in {ERROR_STATUS, CANCELLED_STATUS}:
            message = (
                f"dbt Cloud run {output['id']} failed for partition {partition_id}. "
                f"Status: {status_humanized}. URL: {output.get('href')}"
            )
            raise dg.Failure(description=message, metadata={"dbt_cloud_run_url": output.get("href", "")})

        raise dg.Failure(
            description=(
                f"dbt Cloud run {output['id']} ended in unexpected status "
                f"{status_code} ({status_humanized})."
            ),
            metadata={"dbt_cloud_run_url": output.get("href", "")},
        )
