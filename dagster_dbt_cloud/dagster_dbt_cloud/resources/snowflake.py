import os
from datetime import datetime, timezone
from typing import Any

import dagster as dg
import snowflake.connector
from pydantic import Field


class SnowflakeResource(dg.ConfigurableResource):
    account: str = Field(description="Snowflake account identifier.")
    user: str = Field(description="Snowflake username.")
    warehouse: str = Field(description="Snowflake warehouse name.")
    database: str = Field(description="Snowflake database name.")
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

    def insert_hygiene_results(
        self, rows: list[dict[str, Any]], target_relation: str
    ) -> None:
        if not rows:
            return
        values = [
            (
                row["customer_id"],
                row["hygiene_status"],
                row["corrected_name"],
                row["corrected_address"],
                row["last_hygiene_date"],
            )
            for row in rows
        ]
        insert_sql = f"""
            insert into {target_relation}
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
    deduped: dict[str, dict[str, Any]] = {}
    for row in pending_rows:
        customer_id = row.get("customer_id")
        if not customer_id:
            continue
        deduped[str(customer_id)] = row
    return list(deduped.values())


def build_snowflake_resource() -> SnowflakeResource:
    return SnowflakeResource(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=os.environ["SNOWFLAKE_DATABASE"],
        private_key_path=os.environ.get("SNOWFLAKE_KEY_PATH"),
        private_key_passphrase=os.environ.get("SNOWFLAKE_PASSPHRASE"),
        password=os.environ.get("SNOWFLAKE_PASSWORD"),
        role=os.environ.get("ROLE"),
    )
