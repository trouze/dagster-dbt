import json
import os
import re
import socket
import threading
import time
from typing import Any

import dagster as dg
import httpx
import snowflake.connector
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
LEASE_RETRYABLE_ERROR_SNIPPETS = [
    "duplicate",
    "already exists",
    "conflict",
    "concurrent",
    "lock",
]


class DbtCloudJobPool(dg.ConfigurableResource):
    """Manages a shared lease pool of dbt Cloud jobs for partition fan-out."""

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
    lease_ttl_seconds: int = Field(default=600)
    use_hybrid_lease_table: bool = Field(default=True)
    lease_table_name: str = Field(default="DBT_CLOUD_JOB_LEASES")
    lease_database: str | None = Field(default=None)
    lease_schema: str | None = Field(default=None)
    snowflake_account: str | None = Field(default=None)
    snowflake_user: str | None = Field(default=None)
    snowflake_warehouse: str | None = Field(default=None)
    snowflake_role: str | None = Field(default=None)
    snowflake_password: str | None = Field(default=None)
    snowflake_private_key_path: str | None = Field(default=None)
    snowflake_private_key_passphrase: str | None = Field(default=None)

    _pool: dict[int, str] = PrivateAttr(default_factory=dict)
    _lock: threading.Lock = PrivateAttr(default_factory=threading.Lock)
    _lease_table_initialized: bool = PrivateAttr(default=False)
    _function_relation_cache: dict[str, str] = PrivateAttr(default_factory=dict)
    _model_relation_cache: dict[str, str] = PrivateAttr(default_factory=dict)
    _lease_owner: str = PrivateAttr(
        default_factory=lambda: f"{socket.gethostname()}:{os.getpid()}:{threading.get_ident()}"
    )

    def _resolve_snowflake_config(self) -> dict[str, str]:
        cfg = {
            "account": self.snowflake_account or os.getenv("SNOWFLAKE_ACCOUNT"),
            "user": self.snowflake_user or os.getenv("SNOWFLAKE_USER"),
            "warehouse": self.snowflake_warehouse or os.getenv("SNOWFLAKE_WAREHOUSE"),
            "database": self.lease_database or os.getenv("SNOWFLAKE_DATABASE"),
            "schema": self.lease_schema or os.getenv("SNOWFLAKE_SCHEMA") or "TROUZE",
            "role": self.snowflake_role or os.getenv("SNOWFLAKE_ROLE") or os.getenv("ROLE"),
            "password": self.snowflake_password or os.getenv("SNOWFLAKE_PASSWORD"),
            "private_key_path": self.snowflake_private_key_path or os.getenv("SNOWFLAKE_KEY_PATH"),
            "private_key_passphrase": self.snowflake_private_key_passphrase
            or os.getenv("SNOWFLAKE_PASSPHRASE"),
        }
        required = ["account", "user", "warehouse", "database", "schema"]
        missing = [key for key in required if not cfg.get(key)]
        if missing:
            raise dg.Failure(
                description=(
                    "Missing Snowflake config for dbt job leasing: "
                    + ", ".join(sorted(missing))
                    + ". Set env vars or resource config."
                )
            )
        if not cfg["private_key_path"] and not cfg["password"]:
            raise dg.Failure(
                description=(
                    "Snowflake auth not configured for job leasing. "
                    "Set SNOWFLAKE_KEY_PATH (preferred) or SNOWFLAKE_PASSWORD."
                )
            )
        return cfg

    def _snowflake_connection(self):
        cfg = self._resolve_snowflake_config()
        conn_kwargs: dict[str, Any] = {
            "account": cfg["account"],
            "user": cfg["user"],
            "warehouse": cfg["warehouse"],
            "database": cfg["database"],
            "schema": cfg["schema"],
            "role": cfg["role"],
        }
        if cfg["private_key_path"]:
            conn_kwargs["private_key_file"] = cfg["private_key_path"]
            if cfg["private_key_passphrase"]:
                conn_kwargs["private_key_file_pwd"] = cfg["private_key_passphrase"]
        else:
            conn_kwargs["password"] = cfg["password"]
        return snowflake.connector.connect(**conn_kwargs)

    def _lease_table_fqn(self) -> str:
        cfg = self._resolve_snowflake_config()
        return f'"{cfg["database"]}"."{cfg["schema"]}"."{self.lease_table_name}"'

    def _ensure_lease_table(self) -> None:
        if self._lease_table_initialized:
            return

        cfg = self._resolve_snowflake_config()
        create_schema_sql = f'CREATE SCHEMA IF NOT EXISTS "{cfg["database"]}"."{cfg["schema"]}"'
        table_type = "HYBRID TABLE" if self.use_hybrid_lease_table else "TABLE"
        ddl = f"""
            CREATE {table_type} IF NOT EXISTS {self._lease_table_fqn()} (
                job_id NUMBER PRIMARY KEY,
                lease_owner STRING,
                lease_expires_at TIMESTAMP_NTZ,
                updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
            )
        """
        with self._snowflake_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(create_schema_sql)
                try:
                    cursor.execute(ddl)
                except Exception:
                    if not self.use_hybrid_lease_table:
                        raise
                    # Some accounts/regions do not support hybrid tables yet.
                    fallback_ddl = ddl.replace("HYBRID TABLE", "TABLE")
                    cursor.execute(fallback_ddl)
                    self.use_hybrid_lease_table = False
        self._lease_table_initialized = True

    def _find_function_relation_in_snowflake(self, function_name: str) -> str | None:
        cfg = self._resolve_snowflake_config()
        sql = f"""
            SELECT
                FUNCTION_CATALOG,
                FUNCTION_SCHEMA,
                FUNCTION_NAME
            FROM "{cfg["database"]}".INFORMATION_SCHEMA.FUNCTIONS
            WHERE UPPER(FUNCTION_NAME) = UPPER(%s)
            ORDER BY
                CASE
                    WHEN LOWER(FUNCTION_SCHEMA) LIKE '%%partition_demo%%' THEN 0
                    ELSE 1
                END,
                LAST_ALTERED DESC
            LIMIT 1
        """
        with self._snowflake_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql, (function_name,))
                row = cursor.fetchone()
                if not row:
                    return None
                return f"{row[0]}.{row[1]}.{row[2]}"

    def _job_has_active_lease(self, job_id: int) -> bool:
        self._ensure_lease_table()
        sql = f"""
            SELECT 1
            FROM {self._lease_table_fqn()}
            WHERE job_id = %s
              AND lease_owner IS NOT NULL
              AND lease_expires_at > CURRENT_TIMESTAMP()
            LIMIT 1
        """
        with self._snowflake_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql, (job_id,))
                return cursor.fetchone() is not None

    def _acquire_snowflake_lease(self, job_id: int) -> bool:
        self._ensure_lease_table()
        merge_sql = f"""
            MERGE INTO {self._lease_table_fqn()} AS t
            USING (
                SELECT
                    %s::NUMBER AS job_id,
                    %s::STRING AS lease_owner,
                    DATEADD('second', %s, CURRENT_TIMESTAMP()) AS lease_expires_at
            ) AS s
            ON t.job_id = s.job_id
            WHEN MATCHED
                AND (
                    t.lease_owner IS NULL
                    OR t.lease_expires_at < CURRENT_TIMESTAMP()
                    OR t.lease_owner = s.lease_owner
                )
                THEN UPDATE SET
                    lease_owner = s.lease_owner,
                    lease_expires_at = s.lease_expires_at,
                    updated_at = CURRENT_TIMESTAMP()
            WHEN NOT MATCHED
                THEN INSERT (job_id, lease_owner, lease_expires_at, updated_at)
                VALUES (s.job_id, s.lease_owner, s.lease_expires_at, CURRENT_TIMESTAMP())
        """
        check_sql = f"""
            SELECT lease_owner
            FROM {self._lease_table_fqn()}
            WHERE job_id = %s
            LIMIT 1
        """
        try:
            with self._snowflake_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(merge_sql, (job_id, self._lease_owner, self.lease_ttl_seconds))
                    cursor.execute(check_sql, (job_id,))
                    row = cursor.fetchone()
                    return bool(row and row[0] == self._lease_owner)
        except Exception as exc:
            message = str(exc).lower()
            if any(snippet in message for snippet in LEASE_RETRYABLE_ERROR_SNIPPETS):
                # Treat racing lease claims as "not acquired", not hard failures.
                return False
            raise

    def _release_snowflake_lease(self, job_id: int) -> None:
        self._ensure_lease_table()
        sql = f"""
            UPDATE {self._lease_table_fqn()}
            SET
                lease_owner = NULL,
                lease_expires_at = NULL,
                updated_at = CURRENT_TIMESTAMP()
            WHERE job_id = %s
              AND lease_owner = %s
        """
        with self._snowflake_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql, (job_id, self._lease_owner))

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

        fallback_relation = self._find_function_relation_in_snowflake(function_name=function_name)
        if fallback_relation:
            self._function_relation_cache[cache_key] = fallback_relation
            return fallback_relation

        raise dg.Failure(
            description=(
                f"Function '{function_name}' not found in manifest artifacts for recent successful runs. "
                "Also checked Snowflake INFORMATION_SCHEMA and found no matching function."
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
            has_active_run = self._job_has_active_run(job_id)
            has_remote_lease = self._job_has_active_lease(job_id)
            pool[job_id] = "busy" if has_active_run or has_remote_lease else "idle"

        with self._lock:
            self._pool = pool

        return dict(pool)

    def acquire_job(self) -> int:
        with self._lock:
            for job_id, state in self._pool.items():
                if state == "idle":
                    if self._acquire_snowflake_lease(job_id):
                        self._pool[job_id] = "busy"
                        return job_id
        raise RuntimeError("No idle dbt Cloud pool jobs available.")

    def wait_and_acquire(self, timeout_seconds: int | None = None) -> int:
        timeout = timeout_seconds if timeout_seconds is not None else self.acquire_timeout_seconds
        start = time.time()
        sleep_seconds = 2

        while time.time() - start < timeout:
            if not self._pool:
                self.discover_pool()

            try:
                return self.acquire_job()
            except RuntimeError:
                self.discover_pool()
                time.sleep(sleep_seconds)
                sleep_seconds = min(sleep_seconds * 2, 15)

        raise RuntimeError(
            "Timed out waiting for an idle dbt Cloud pool job after "
            f"{timeout} seconds."
        )

    def release_job(self, job_id: int) -> None:
        self._release_snowflake_lease(job_id)
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

            job_id = self.wait_and_acquire(timeout_seconds=max(1, int(remaining)))
            try:
                run_id = self.trigger_run(job_id=job_id, partition_id=partition_id)
                result = self.poll_run(run_id=run_id, context=context)
                break
            except httpx.HTTPStatusError as exc:
                status_code = exc.response.status_code
                # If a job is claimed between check/trigger windows, try another pool job.
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
                try:
                    self.release_job(job_id)
                except Exception:
                    # Never mask the real trigger/poll failure with lease cleanup errors.
                    if context:
                        context.log.warning("Failed to release lease for job %s", job_id)

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
