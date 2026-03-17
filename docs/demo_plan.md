# Demo: UDTF-based hygiene routing

End-to-end walkthrough of the hygiene overlay pattern. Shows the idempotent
dbt build, the UDTF routing decision, simulated hygiene API results, and the
merge back into `name_address`.

## Prerequisites

- dbt project is configured and connected to Snowflake
- Source table `RAW.trouze.inbound_file_orders_001` exists with sample data
- No prior runs (clean slate) — or run `dbt run -s hygiene_results name_address --full-refresh` to reset

## Step 1: Build the idempotent path + functions

Run in terminal:

```bash
dbtf build -s +address_hygiene_pending --vars '{"partition_id": "001"}'
```

This builds the full DAG:

```
source → file_orders_stage → file_orders_refined → name_address
                           → file_orders_rejects
                                                  → hygiene_results (empty stub)
         needs_hygiene (UDF) → address_hygiene_pending (UDTF)
```

**Talking point**: Everything here is idempotent. Safe to re-run anytime.
The `hygiene_results` table is created empty — it's a schema stub that
Dagster INSERTs into.

## Step 2: Query the UDTF — "what should we send to hygiene?"

Run in Snowflake:

```sql
-- This is what Dagster calls after the dbt build.
-- Scoped to file 001; parallel-safe across partitions.
SELECT * FROM TABLE(address_hygiene_pending('001'));
```

**Expected**: All records from file 001 appear — nothing has been
hygiene'd yet, so the UDTF flags everything.

**Talking point**: This is a warehouse function, not a dbt model.
Dagster (or any SQL client) calls it directly. No `dbt run` needed.
The `needs_hygiene` UDF is the predicate that decides the routing.

## Step 3: Show the UDF predicate directly (optional)

```sql
-- The scalar UDF that powers the UDTF's filter
SELECT needs_hygiene(NULL, 18);          -- TRUE  (never hygiene'd)
SELECT needs_hygiene('2024-01-15', 18);  -- TRUE  (stale, > 18 months)
SELECT needs_hygiene(CURRENT_DATE(), 18); -- FALSE (fresh)
```

## Step 4: Simulate hygiene API — INSERT results

In production, Dagster calls the external API and INSERTs results. We
simulate this with a direct INSERT from the UDTF output. The
`hygiene_results` table is append-only (log style) with an `inserted_at`
timestamp; `name_address` uses that timestamp for incremental filtering
and deduplicates to the latest result per customer.

```sql
-- Dagster writes raw API responses — plain INSERT, no MERGE needed.
-- Parallel-safe: concurrent partitions INSERT different rows.
-- inserted_at is the high-water mark name_address uses for incrementality.
INSERT INTO hygiene_results (
    customer_id,
    hygiene_status,
    corrected_name,
    corrected_address,
    last_hygiene_date,
    inserted_at
)
SELECT
    customer_id,
    'VALID'                                      AS hygiene_status,
    'Corrected Name ' || customer_id             AS corrected_name,
    '123 Clean St, Suite ' || customer_id        AS corrected_address,
    CURRENT_DATE()                               AS last_hygiene_date,
    CURRENT_TIMESTAMP()                          AS inserted_at
FROM TABLE(address_hygiene_pending('001'));
```

**Talking point**: Dagster just does a simple INSERT — no MERGE, no
complex SQL. The table is append-only (a log). `name_address` handles
deduplication via `QUALIFY ROW_NUMBER()` on `inserted_at` when it merges,
and uses `inserted_at` to incrementally scan only new results.

## Step 5: Verify hygiene_results

```sql
SELECT * FROM hygiene_results ORDER BY customer_id;
```

**Expected**: One row per customer from file 001, with mock corrected
data and today's date as `last_hygiene_date`.

## Step 6: Merge results into name_address

Run in terminal:

```bash
dbt run -s name_address
```

**Talking point**: This is the only "re-run" step. `name_address` INNER
JOINs refined records with `hygiene_results` (deduplicated to latest per
customer). The incremental merge on `customer_id` upserts into the
terminal table.

## Step 7: Verify name_address

```sql
SELECT * FROM name_address ORDER BY customer_id;
```

**Expected**: Refined customer data joined with the corrected hygiene
fields.

## Step 8: The money shot — UDTF now returns zero rows

```sql
SELECT * FROM TABLE(address_hygiene_pending('001'));
```

**Expected**: Empty result set. All customers from file 001 are now in
`name_address` with a fresh `last_hygiene_date`. The `needs_hygiene`
predicate returns FALSE for all of them.

**Talking point**: On the next file run, only genuinely new customer IDs
(~5-10%) will be flagged. The UDTF is the single interface for "what
needs hygiene?" — same function, different file_id, run in parallel.

## Step 9 (optional): Show parallel safety

Open a second Snowflake worksheet and run the same flow for file 002:

```sql
-- Worksheet 2 — different partition, same hygiene_results table
INSERT INTO hygiene_results (
    customer_id,
    hygiene_status,
    corrected_name,
    corrected_address,
    last_hygiene_date,
    inserted_at
)
SELECT
    customer_id,
    'VALID'                                      AS hygiene_status,
    'Corrected Name ' || customer_id             AS corrected_name,
    '123 Clean St, Suite ' || customer_id        AS corrected_address,
    CURRENT_DATE()                               AS last_hygiene_date,
    CURRENT_TIMESTAMP()                          AS inserted_at
FROM TABLE(address_hygiene_pending('002'));
```

Both worksheets INSERT into the same table simultaneously — no conflicts
because INSERTs are append-only and never lock each other.

## Step 10 (optional): Show periodic staleness refresh

```sql
-- Imagine it's 18 months later. Lower the threshold to pick up stale records.
SELECT * FROM TABLE(address_hygiene_pending(NULL, 1));
```

**Talking point**: Same UDTF, different parameters. `NULL` for file_id
means "all files". `1` month threshold means "anything older than 1
month." This is how the periodic refresh job would work — Dagster calls
the same function on a different schedule with different arguments.

## Key points to emphasize

1. **dbt owns all the SQL** — the UDTF, the UDF, the merge logic. Dagster
   just does I/O (API call + INSERT).

2. **No cycle in dbt** — the UDTF is a leaf node in the DAG. The feedback
   loop (hygiene results → name_address) goes through Dagster, not dbt refs.

3. **Parallel-safe** — different file partitions run simultaneously. INSERTs
   don't conflict. The UDTF is parametrized by file_id.

4. **One function, two modes** — per-file routing (normal) and cross-file
   staleness refresh (periodic). Same interface, different arguments.

5. **Bootstrap** — for initial load, INSERT existing hygiene data from the
   legacy system into `hygiene_results`, then `dbt run -s name_address`.
