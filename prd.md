# PRD: Ephemeral Trigger Jobs for Orchestrator-Driven dbt Cloud Workloads

## 1. Problem Statement and Context

Teams using external orchestrators (Dagster, Airflow, Prefect, etc.) to fan out partitioned dbt work currently must pre-create and maintain pools of static dbt Cloud jobs. This causes operational friction: environment drift, job contention, brittle job naming conventions, and extra API complexity to discover idle jobs.

dbt Cloud jobs are optimized for scheduled or manually managed runs, not high-frequency dynamic triggering with per-run overrides at orchestrator scale. As a result, users build custom lease tables and job-pool logic outside dbt Cloud.

Business impact:
- Slower time to production for orchestrator-led patterns.
- More support burden due to custom locking and environment mismatches.
- Lower trust in "dbt Cloud as control plane" for complex workloads.

## 2. Target Customer and Benefits

Primary users:
- Analytics engineers orchestrating partition-parallel workloads.
- Platform/data infrastructure teams integrating dbt Cloud with orchestrators.
- Enterprise teams with strict dev/stage/prod promotion workflows.

Benefits:
- Trigger dbt runs "infinitely" from an orchestrator without creating N static jobs.
- Eliminate custom job-leasing infrastructure for common fanout patterns.
- Improve environment safety with explicit run-level environment awareness.
- Reduce setup complexity for new orchestrator integrations.

## 3. Vision, Goals, and Success Criteria

Vision:
dbt Cloud should provide a first-class "ephemeral trigger job" abstraction that behaves like a reusable template. Orchestrators can trigger unlimited runs with step/vars/select overrides while preserving governance, permissions, observability, and environment boundaries.

Goals (MVP):
- Introduce a job type optimized for API-triggered, unscheduled, high-frequency execution.
- Support run overrides without mutating persisted job definitions.
- Provide concurrency controls and queueing semantics at the template level.
- Ensure strict environment scoping and promotion-friendly behavior.

Non-goals (MVP):
- Replacing existing scheduled jobs.
- Building a full external orchestrator product inside dbt Cloud.
- Cross-account orchestration.

Success criteria:
- 50% reduction in setup time for orchestrator partition fanout use cases.
- 80% reduction in support tickets related to pooled static job contention/drift.
- >=99.9% successful trigger acceptance for valid requests under documented limits.

## 4. User Scenarios and High-Level Requirements

### Scenario A (P0): Unlimited orchestrator fanout
As an orchestrator, I want to trigger many partitioned dbt runs against one ephemeral template job with per-run overrides.

Requirements:
- New `job_type: ephemeral_trigger` (name TBD).
- No schedule required.
- API trigger endpoint supports per-run overrides:
  - `steps_override`
  - `vars`
  - `select/exclude` (or equivalent)
  - metadata tags/labels for partition correlation
- Triggering a run must not persist overrides to the base job template.

### Scenario B (P0): Safe environment alignment
As a platform engineer, I want each trigger template bound to a single environment so dev/stage/prod cannot mix accidentally.

Requirements:
- Template has immutable `environment_id` binding (editable only by privileged role).
- Trigger calls reject requests that attempt cross-environment execution.
- API and UI clearly show environment context for every run.

### Scenario C (P1): Built-in concurrency semantics
As an operator, I want deterministic run concurrency behavior without external lease tables.

Requirements:
- Template-level concurrency policy:
  - `max_active_runs`
  - `queue_behavior` (`queue`, `reject`, `cancel_oldest`)
- Optional dedupe key:
  - If active run exists with same key, return existing run or queue according to policy.

### Scenario D (P1): Observable and debuggable at scale
As an engineer, I want run-level traceability from orchestrator partition to dbt artifacts.

Requirements:
- User-provided `correlation_id` and `partition_key` fields.
- Exposed in run list/filter/search and run detail API.
- Artifact retrieval remains standard (manifest, run_results, logs).

## 5. Proposed Product Design

### 5.1 Ephemeral Trigger Template Job

New job subtype with:
- Base defaults: project, environment, execution settings, permissions.
- No schedule/triggers required.
- Intended lifecycle: create once, trigger many.

### 5.2 API Shape (Illustrative)

Create template:
- `POST /jobs`
- Body includes `job_type: "ephemeral_trigger"`, `project_id`, `environment_id`, defaults.

Trigger run:
- `POST /jobs/{id}/run`
- Body includes:
  - `cause`
  - `steps_override`
  - `vars`
  - `correlation_id`
  - `dedupe_key` (optional)
  - `priority` (optional future)

Response:
- `run_id`, `status`, accepted/rejected reason, queue position if queued.

### 5.3 Concurrency and Queueing

Per-template config:
- `max_active_runs` default `1` for deterministic behavior.
- FIFO queue for overflow requests when `queue_behavior=queue`.
- Hard cap on queue depth with clear error codes.

### 5.4 Governance and Security

- Permissions model mirrors existing job execution roles.
- Full audit log of overrides per run.
- Policy controls:
  - allowed override fields
  - max SQL step count
  - forbidden commands (optional hardening)

## 6. Acceptance Criteria (Definition of Done)

Functional acceptance:
- User can create one ephemeral trigger template and launch >=100 sequential or parallel runs without editing the job.
- Per-run overrides execute and are visible in run metadata and logs.
- Environment mismatch attempts are rejected with explicit API error.
- Concurrency policies behave as configured under contention.

Reliability acceptance:
- Trigger API remains stable under burst tests at documented throughput.
- Queue semantics are deterministic and observable.
- Artifact access for ephemeral-triggered runs matches standard jobs.

UX acceptance:
- UI clearly distinguishes ephemeral templates from scheduled jobs.
- Run list filters include `job_type`, `correlation_id`, and `partition_key`.

## 7. Risks and Mitigations

Risk 1: Abuse of overrides could bypass governance.
- Mitigation: allowlist override fields + policy checks + audit trail.

Risk 2: Queue growth under fanout spikes.
- Mitigation: queue depth limits, backpressure errors, and monitoring alerts.

Risk 3: Confusion between standard and ephemeral jobs.
- Mitigation: clear UI labeling, docs, and migration guide.

## 8. Rollout Plan

Phase 1 (private beta):
- API-only support for selected orchestrator customers.
- Basic concurrency (`max_active_runs`, `queue`).

Phase 2 (public beta):
- UI creation and management.
- Enhanced metadata filters and observability.

Phase 3 (GA):
- SLA-backed limits and quotas.
- Dedupe behavior and policy controls hardened.

## 9. Open Questions

- Should `dedupe_key` be globally unique per template for active + queued runs?
- What are safe default limits (`max_active_runs`, queue depth) by account tier?
- Should overrides support full command replacement or restricted parameterized execution?
- Do we need template versioning for promotion workflows across environments?