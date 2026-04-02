from datetime import datetime, timezone
from typing import Any
import json
import os
import time

import dagster as dg
import snowflake.connector
from pydantic import Field

from dagster_partition_demo.partitions import file_partitions
from dagster_partition_demo.resources.dbt_cloud_pool import DbtCloudJobPool


class SnowflakeResource(dg.ConfigurableResource):
    account: str = Field(description="Snowflake account identifier.")
    user: str = Field(description="Snowflake username.")
    warehouse: str = Field(description="Snowflake warehouse name.")
    database: str = Field(description="Snowflake database name.")
    schema_name: str = Field(default="TROUZE", description="Snowflake schema name.")
    hygiene_results_database: str | None = Field(
        default=None,
        description="Optional database override for hygiene_results writes.",
    )
    hygiene_results_schema: str | None = Field(
        default=None,
        description="Optional schema override for hygiene_results writes.",
    )
    hygiene_results_table: str = Field(
        default="hygiene_results",
        description="Target table name for hygiene result inserts.",
    )
    password: str | None = Field(default=None, description="Optional Snowflake password.")
    private_key_path: str | None = Field(
        default=None, description="Optional path to Snowflake RSA private key."
    )
    private_key_passphrase: str | None = Field(
        default=None, description="Optional passphrase for encrypted private key."
    )
    role: str | None = Field(default=None)

    def _connect(self):
        connect_kwargs: dict[str, Any] = dict(
            account=self.account,
            user=self.user,
            warehouse=self.warehouse,
            database=self.database,
            schema=self.schema_name,
            role=self.role or os.getenv("SNOWFLAKE_ROLE") or os.getenv("ROLE"),
        )
        if self.private_key_path:
            connect_kwargs["private_key_file"] = self.private_key_path
            if self.private_key_passphrase:
                connect_kwargs["private_key_file_pwd"] = self.private_key_passphrase
        elif self.password:
            connect_kwargs["password"] = self.password
        else:
            raise ValueError(
                "Snowflake auth not configured. Set private_key_path (preferred) "
                "or password on SnowflakeResource."
            )

        return snowflake.connector.connect(**connect_kwargs)

    def execute(self, sql: str, params: tuple[Any, ...] | None = None) -> list[dict[str, Any]]:
        with self._connect() as conn:
            with conn.cursor() as cursor:
                if params is None:
                    cursor.execute(sql)
                else:
                    cursor.execute(sql, params)
                if cursor.description is None:
                    return []
                columns = [col[0].lower() for col in cursor.description]
                return [dict(zip(columns, row)) for row in cursor.fetchall()]

    def insert_hygiene_results(self, rows: list[dict[str, Any]], target_relation: str | None = None) -> None:
        if not rows:
            return

        values = []
        for row in rows:
            values.append(
                (
                    row["customer_id"],
                    row["hygiene_status"],
                    row["corrected_name"],
                    row["corrected_address"],
                    row["last_hygiene_date"],
                )
            )

        resolved_target_relation = target_relation
        if not resolved_target_relation:
            target_database = self.hygiene_results_database or self.database
            target_schema = self.hygiene_results_schema or self.schema_name
            target_table = self.hygiene_results_table
            resolved_target_relation = f'"{target_database}"."{target_schema}"."{target_table}"'

        insert_sql = f"""
            insert into {resolved_target_relation}
            (
                customer_id,
                hygiene_status,
                corrected_name,
                corrected_address,
                last_hygiene_date,
                inserted_at
            )
            values (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP())
        """
        with self._connect() as conn:
            with conn.cursor() as cursor:
                cursor.executemany(insert_sql, values)


def simulate_hygiene_api(pending_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    results = []
    for row in pending_rows:
        customer_id = row.get("customer_id")
        corrected_name = row.get("customer_name") or row.get("name") or "Unknown Customer"
        address = row.get("address") or row.get("customer_address") or "Unknown Address"
        results.append(
            {
                "customer_id": customer_id,
                "hygiene_status": "VERIFIED",
                "corrected_name": corrected_name,
                "corrected_address": address,
                "last_hygiene_date": datetime.now(timezone.utc).date().isoformat(),
            }
        )
    return results


def dedupe_pending_rows(pending_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    # Keep one row per customer_id for idempotent/dedupe-safe API requests.
    deduped: dict[str, dict[str, Any]] = {}
    for row in pending_rows:
        customer_id = row.get("customer_id")
        if not customer_id:
            continue
        deduped[str(customer_id)] = row
    return list(deduped.values())


def call_hygiene_api_with_retry(
    pending_rows: list[dict[str, Any]],
    retries: int = 3,
    base_sleep_seconds: int = 2,
) -> list[dict[str, Any]]:
    for attempt in range(1, retries + 1):
        try:
            return simulate_hygiene_api(pending_rows)
        except Exception:
            if attempt == retries:
                raise
            time.sleep(base_sleep_seconds * attempt)
    return []


@dg.asset(
    partitions_def=file_partitions,
    op_tags={"dagster/concurrency_key": "dbt_cloud_pool"},
)
def partition_dbt_run(
    context,
    dbt_cloud_pool: DbtCloudJobPool,
) -> dg.MaterializeResult:
    partition_id = context.partition_key
    context.log.info(f"Triggering dbt Cloud run for partition {partition_id}.")
    result = dbt_cloud_pool.run_partition(partition_id=partition_id, context=context)

    return dg.MaterializeResult(
        metadata={
            "partition_id": partition_id,
            "dbt_cloud_run_id": result["id"],
            "status": result["status_humanized"],
            "dbt_cloud_run_url": result.get("href", ""),
            "duration": result.get("duration_humanized", ""),
            "dbt_cloud_job_id": result["job_id"],
        }
    )


@dg.asset(
    partitions_def=file_partitions,
    deps=[partition_dbt_run],
    op_tags={"dagster/concurrency_key": "hygiene_api"},
)
def partition_hygiene_processing(
    context,
    dbt_cloud_pool: DbtCloudJobPool,
    snowflake: SnowflakeResource,
) -> dg.MaterializeResult:
    partition_id = context.partition_key
    hygiene_function_relation = dbt_cloud_pool.resolve_function_relation_from_cloud(
        function_name="address_hygiene_pending",
        partition_id=partition_id,
    )
    hygiene_results_relation = dbt_cloud_pool.resolve_model_relation_from_cloud(
        model_name="hygiene_results",
        partition_id=partition_id,
    )
    pending = snowflake.execute(
        f"select * from table({hygiene_function_relation}(%s, %s))",
        params=(partition_id, 18),
    )
    if not pending:
        context.log.info("No pending hygiene records found for partition %s.", partition_id)
        return dg.MaterializeResult(metadata={"partition_id": partition_id, "records_processed": 0})

    deduped_pending = dedupe_pending_rows(pending)
    hygiene_results = call_hygiene_api_with_retry(deduped_pending)
    snowflake.insert_hygiene_results(hygiene_results, target_relation=hygiene_results_relation)
    context.log.info(
        "Inserted %s hygiene records for partition %s into %s (pending=%s, deduped=%s).",
        len(hygiene_results),
        partition_id,
        hygiene_results_relation,
        len(pending),
        len(deduped_pending),
    )

    return dg.MaterializeResult(
        metadata={
            "partition_id": partition_id,
            "records_processed": len(hygiene_results),
            "pending_records": len(pending),
            "deduped_records": len(deduped_pending),
            "hygiene_results_relation": hygiene_results_relation,
        }
    )


@dg.asset(deps=[partition_hygiene_processing])
def name_address_refresh(
    context,
    dbt_cloud_pool: DbtCloudJobPool,
) -> dg.MaterializeResult:
    job_id = dbt_cloud_pool.wait_and_acquire()
    try:
        run_id = dbt_cloud_pool.trigger_run_custom(
            job_id=job_id,
            steps=["dbt run --select name_address"],
            cause="Dagster post-hygiene refresh: name_address",
        )
        run = dbt_cloud_pool.poll_run(run_id=run_id, context=context)
    finally:
        dbt_cloud_pool.release_job(job_id)

    status = int(run.get("status", -1))
    if status != 10:
        raise dg.Failure(
            description=(
                f"name_address rebuild failed for dbt Cloud run {run_id}. "
                f"Status: {run.get('status_humanized')}. URL: {run.get('href')}"
            ),
            metadata={"dbt_cloud_run_url": run.get("href", "")},
        )

    return dg.MaterializeResult(
        metadata={
            "dbt_cloud_run_id": run_id,
            "dbt_cloud_run_url": run.get("href", ""),
            "status": run.get("status_humanized", "Success"),
        }
    )
