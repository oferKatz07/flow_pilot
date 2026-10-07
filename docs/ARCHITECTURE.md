# FlowPilot Architecture

## Overview

FlowPilot is a C++20 distributed workflow orchestration platform designed around asynchronous I/O, coroutine-based processing, durable workflow persistence, dependency-aware scheduling, and extensible worker integration.

The system is designed to separate **workflow admission** from **workflow execution**.

The admission subsystem validates incoming workflows, enforces client policies and admission limits, persists the workflow definition, and prepares the runtime state required for execution.

The scheduler then operates on the prepared runtime state in Redis. During normal execution, the scheduler does not access SQLite. Redis contains the runtime state required to schedule and execute jobs, while SQLite provides durable persistence and the basis for recovery.

The workflow admission phase is complete. The current execution architecture uses an explicit READY -> QUEUED -> PENDING_EXECUTION -> RUNNING -> RETRY_DELAY/COMPLETED/FAILED/ABORTED lifecycle, workflow-level execution-slot reservation, priority-ordered Redis scheduling, scheduler ownership, runtime-owned scheduler/completion/retry components, configurable Redis I/O threads, delayed retry scheduling, and durable execution timestamps/statuses in SQLite. Current development is focused on completing execution semantics, recovery, and operational visibility.

---

# Architecture Goals

FlowPilot is designed around the following principles:

* High throughput using asynchronous I/O.
* Durable persistence of workflow definitions and request history.
* Idempotent workflow admission.
* Strict separation of transport, admission, scheduling, and execution.
* Fast rejection of invalid requests.
* Dependency-aware job scheduling.
* Redis-based runtime coordination.
* SQLite-based durable persistence and recovery.
* Asynchronous job execution.
* Extensible worker/executor integration.
* Runtime-managed scheduler, completion-handler, retry-handler, and Redis I/O lifecycles.
* Operational status surfaces for runtime components.
* Eventual support for distributed scheduler and worker deployments.

---

# Design Principles

### 1. Asynchronous by Default

Request processing and workflow execution should not block I/O threads.

Boost.Asio coroutines are used as the primary asynchronous programming model.

### 2. Durability Before Execution

A workflow must be successfully validated and durably persisted before it can enter execution.

Runtime execution data is prepared only after admission succeeds.

### 3. Admission and Execution Are Separate

The admission subsystem decides whether a workflow is allowed to execute.

The scheduler is responsible for executing admitted workflows according to their dependency graph and runtime policies.

### 4. Redis for Runtime Coordination, SQLite for Durable Persistence

Redis is used for transient but authoritative **runtime execution state**.

SQLite stores durable workflow definitions, requests, job definitions, dependency information, and execution history required for persistence and recovery.

The scheduler does not query SQLite during normal workflow execution.

### 5. Redis Is the Scheduler's Runtime Interface

The scheduler obtains all information required for normal execution from Redis.

This provides a clean boundary between workflow admission and workflow execution and avoids introducing synchronous database access into the scheduler's execution path.

### 6. Execution-Eligible Jobs Are the Scheduler Boundary

The Redis `execution_queue` sorted set is the scheduler-facing collection of execution-eligible jobs.

A job is inserted into `execution_queue` only after its dependencies are satisfied and the workflow has reserved an execution slot for it, transitioning the job to QUEUED. When a scheduler claims that queued job, ownership moves to the scheduler and the job transitions to PENDING_EXECUTION until a worker starts it.

This ordering ensures that a scheduler can never observe a job before the runtime data required to execute that job has been created.

### 7. Explicit Runtime State

Workflow execution state is represented explicitly in Redis.

The runtime state contains the information required to:

* identify active workflows,
* determine job readiness,
* track dependencies,
* obtain job payloads,
* track job execution state,
* manage workflow execution counters,
* transition jobs as execution progresses.

### 8. Immutable Workflow Definitions

After admission, the workflow DAG is immutable.

Execution operates on the runtime representation of that immutable definition.

This avoids runtime graph mutation and simplifies dependency scheduling.

### 9. Business Logic Remains Outside the HTTP Layer

HTTP handlers are responsible for transport concerns, request routing, and lightweight operational status responses.

Workflow admission and execution logic belong to dedicated services and runtime components.

### 10. Runtime Lifecycle Is Explicit

FlowPilot owns its execution components through a runtime manager rather than ad hoc local variables in `main()`.

The runtime manager is responsible for creating, stopping, and destroying Redis I/O threads, workflow schedulers, completion handlers, and the retry handler. This keeps shutdown ordering explicit and prepares the system for dynamic scaling, where schedulers and completion handlers can be added or removed while the process continues running.

### 11. Concurrency Requires Atomic State Transitions

FlowPilot may run multiple schedulers, completion handlers, worker threads, and Redis I/O threads in one process. Component lifecycle scaling and workflow-state correctness are separate concerns: adding execution components must not weaken Redis state-machine invariants.

State changes that update several related Redis fields or structures should therefore be expressed as one atomic transition, preferably in Lua. In particular, completion processing must atomically validate the current job/workflow state, update job status, maintain workflow counters and `reserved_execution_slots`, and promote work when appropriate. This prevents lost updates when two jobs from the same workflow complete concurrently.

A Redis connection is also a serialized request/reply channel. Multiple Asio threads do not by themselves make one shared connection safe for concurrent independent request/reply transactions. The normal Redis command path must therefore serialize complete transactions per connection or use a connection pool with per-connection serialization. Dedicated blocking scheduler/completion-wait connections remain separate from this command path.

---

# High-Level Architecture

```text
                         +----------------------+
                         |       Client         |
                         +----------+-----------+
                                    |
                                    v
                         +----------------------+
                         |    HTTP Server       |
                         |   Boost.Beast/Asio   |
                         +----------+-----------+
                                    |
                                    v
                         +----------------------+
                         | Workflow Admission    |
                         |       Service        |
                         +----------+-----------+
                                    |
                  +-----------------+------------------+
                  |                                    |
                  v                                    v
          +---------------+                    +----------------+
          |    Redis      |                    |    SQLite      |
          | Admission +   |                    |    Durable     |
          | Runtime Data  |                    |   Persistence  |
          +-------+-------+                    +----------------+
                  |
                  | queued_jobs
                  v
          +----------------------+
          |  FlowPilot Runtime   |
          | schedulers +         |
          | completion/retry     |
          | handlers             |
          +----------+-----------+
                     |
                     v
          +----------------------+
          |      Scheduler       |
          | Redis-only runtime   |
          |       access         |
          +----------+-----------+
                     |
                     v
          +----------------------+
          | Worker / Executor    |
          | Integration Layer    |
          +----------+-----------+
                     |
                     v
          +----------------------+
          | Local / Remote       |
          | Job Executors        |
          +----------------------+
```

The important architectural boundary is:

```text
              Admission
                  |
                  | Prepare runtime state
                  | + enqueue ready jobs
                  v
             Redis runtime
                  |
             queued_jobs
                  |
                  v
              Scheduler
                  |
                  v
             Job execution
```

---

# Core Components

## HTTP Server

The HTTP server is implemented using Boost.Beast and Boost.Asio coroutines.

Responsibilities:

* Accept incoming REST requests.
* Parse HTTP requests.
* Route requests to handlers.
* Create request context.
* Return HTTP responses.

The HTTP layer does not contain workflow business logic.

---

# Workflow Admission Service

The Workflow Admission Service is responsible for determining whether a workflow can enter execution.

Its responsibilities include:

* Request parsing and validation.
* JSON schema validation.
* Client validation.
* Policy enforcement.
* Request idempotency.
* Rate limiting.
* Concurrent workflow limits.
* Semantic workflow validation.
* Dependency validation.
* DAG validation.
* Durable workflow persistence.
* Runtime initialization.
* Initial ready-job creation.

The admission subsystem is the only component responsible for taking a submitted workflow definition and turning it into an executable workflow runtime.

---

# Admission-to-Execution Boundary

After a workflow passes admission and is durably persisted, the admission subsystem prepares its Redis runtime representation.

The runtime representation contains the information required to manage and execute the workflow without requiring the scheduler to access SQLite:

* Workflow runtime metadata.
* Job runtime state.
* Dependency and successor information.
* Job payloads.
* Runtime counters and execution metadata.

Jobs with no unresolved dependencies enter the **READY** state.

A READY job does not consume workflow execution capacity. When an execution slot is available, the workflow reserves the slot and transitions a selected READY job to **QUEUED**.

```text
reserved_execution_slots = QUEUED jobs + PENDING_EXECUTION jobs + RUNNING jobs
```

QUEUED jobs are execution-eligible and are inserted into the Redis `execution_queue` sorted set for scheduler claim.

The initialization path is:

```text
Persist workflow
       |
       v
Create Redis runtime state
       |
       v
Initialize dependency state
       |
       v
Dependencies satisfied
       |
       v
     READY
       |
       | workflow execution slot available
       v
     QUEUED
       |
       v
Insert into execution_queue
       |
       v
Scheduler claims ownership
       |
       v
 PENDING_EXECUTION
       |
       v
Scheduler dispatch
```

This boundary separates workflow-specific execution eligibility from scheduling. The workflow runtime grants execution capacity; the scheduler prioritizes and dispatches jobs that have already been granted that capacity.

# FlowPilot Runtime Manager

`FlowPilotRuntime` owns the process-local execution components:

* Redis I/O threads.
* Workflow scheduler instances.
* Completion handler instances.
* Retry handler instance.

The runtime is created by `main()` after configuration has been parsed. It starts the configured number of Redis I/O threads, schedulers, completion handlers, and one retry handler. The default is one of each component, but the executable accepts command-line overrides for the scalable component counts:

```text
--redis-io-threads <count>
--schedulers <count>
--completion-handlers <count>
```

The runtime also exposes add/remove operations for schedulers and completion handlers. This supports the design intent that FlowPilot can eventually scale local scheduler and completion-handler capacity according to current load.

Dynamic add/remove operations are accepted only while the runtime is started and not shutting down. Scheduler removal is graceful: the scheduler stops acquiring new Redis work, closes its local worker queue, and allows already handed-off local work to drain before destruction. Redis I/O thread count is configured at runtime startup; schedulers and completion handlers are the components currently designed for live add/remove. The retry handler is a singleton process-local component in the current implementation.

Shutdown is coordinated through the runtime. `request_stop()` on schedulers, completion handlers, and the retry handler is idempotent, so components can be stopped explicitly by runtime management code and later destroyed safely. A scheduler or completion handler can be removed while other instances continue running.

The runtime provides a process-local status snapshot used by the HTTP status API. The initial snapshot includes lifecycle flags and component counts; future snapshots can include scheduler UUIDs, Redis connection identifiers, thread IDs, queue depths, health information, and per-component statistics.

---

# Scheduler

The scheduler is the core component of the workflow execution subsystem.

Its primary responsibility is to select QUEUED jobs that have already been granted a workflow execution slot, claim them as PENDING_EXECUTION under scheduler ownership, prioritize them, and coordinate their dispatch into the worker/executor layer according to available worker capacity.

FlowPilot separates **dependency readiness**, **workflow execution eligibility**, and **actual execution**:

```text
PENDING
   |
   | dependencies satisfied
   v
 READY
   |
   | workflow reserves an execution slot
   v
 QUEUED
   |
   | scheduler claims ownership
   v
 PENDING_EXECUTION
   |
   | worker fetches job
   v
 RUNNING
   |
   +------> COMPLETED
   |
   +------> FAILED
   |
   +------> ABORTED
```

A QUEUED job has already consumed one of the workflow's execution slots, even though it may not yet be owned by a scheduler or running. PENDING_EXECUTION means a scheduler owns the job and is responsible for dispatching it to a worker, but execution timing has not started yet. A retry does not require a separate RETRYING state. Retry intent is represented by the job's retry counter together with its normal execution state. A failed attempt that is eligible for retry is moved to RETRY_DELAY, recorded in the delayed retry queue, and retried after `retry_delay_sec` plus the current jitter policy. No separate RETRYING state is used.

## Scheduler Responsibilities

The scheduler is responsible for:

* Consuming priority-ordered QUEUED jobs from the Redis scheduler-facing structure.
* Obtaining job and workflow runtime information from Redis.
* Treating QUEUED jobs as already admitted by the workflow for execution; the scheduler does not decide whether the workflow has a free execution slot.
* Recording scheduler ownership for jobs it has claimed and moving them to PENDING_EXECUTION.
* Maintaining a bounded local set of jobs waiting to be executed.
* Dispatching PENDING_EXECUTION jobs to available workers/executors.
* Transitioning a job to RUNNING only when a worker actually fetches it.
* Starting the execution-time budget when the worker fetches the job, rather than when the scheduler reserves it.
* Monitoring dispatcher and worker-thread health.
* Activating scheduler-local recovery when a worker thread becomes non-responsive.
* Handing worker completion/failure outcomes to the Redis completion stream.

Dependency advancement, retry/finalization decisions, workflow counters, slot release, and workflow terminal-state decisions belong to the completion/retry side of the execution architecture rather than to the scheduler dispatch loop.

The scheduler remains independent of the HTTP and workflow-admission layers.

## Scheduler Local Scheduling State

Redis remains the shared runtime authority, while each scheduler may maintain local structures optimized for efficient dispatch.

The scheduler uses:

* A bounded collection of jobs already reserved by the scheduler and waiting for worker capacity.
* A priority-ordered local container for efficient selection of the next job to execute.
* Scheduler identity stored in the job's `owned_by` field after a scheduler claims a QUEUED job and moves it to PENDING_EXECUTION. Workflow execution-slot reservation and scheduler ownership are separate concepts.
* Per-worker in-flight job ownership, heartbeat, and last-progress metadata for health monitoring.
* A scheduler-local recovery queue used to reprocess jobs whose worker thread failed before reporting completion.

Scheduler-local priority adjustments, such as future fairness/aging, do not modify the job's persisted priority in Redis or SQLite.

## Planned Health Monitor and Recovery

The scheduler will include a health monitor responsible for detecting non-responsive execution threads and triggering recovery for their in-flight work.

The health monitor tracks:

* Dispatcher progress, including whether the scheduler main loop is still polling Redis and dispatching work.
* Worker-thread heartbeats or progress timestamps.
* The job currently assigned to each worker thread.
* Scheduler-owned jobs that have not reached a terminal state.

If a worker thread becomes non-responsive, the health monitor should stop using that thread, terminate it when the platform supports safe termination, and hand its in-flight job to the scheduler recovery component. The recovery component is responsible for reconciling the job's Redis runtime state and returning it to an executable state when appropriate.

Worker-thread failure is handled locally by the scheduler that owns the worker. Scheduler-process failure is handled by distributed recovery. Both recovery paths use the same ownership model: jobs with `owned_by = <scheduler_id>` and non-terminal status are candidates for reclaim; jobs already in terminal states are not reclaimed.

Recovery must preserve the execution-slot invariant:

```text
reserved_execution_slots = QUEUED jobs + PENDING_EXECUTION jobs + RUNNING jobs
```

For a reclaimed job:

* If the job can be retried, recovery clears stale worker ownership metadata and routes it through the retry path, which returns the job to an executable state according to retry delay and workflow capacity.
* If the job cannot be retried, recovery finalizes it as FAILED or ABORTED through the same completion/finalization path used by normal execution.
* If the scheduler itself died, another scheduler or recovery coordinator reclaims the abandoned non-terminal jobs owned by the dead scheduler.

The completion handler remains the canonical place for releasing execution slots, updating workflow counters, advancing dependencies, and persisting final SQLite status. Recovery should therefore route recovered terminal outcomes through completion handling rather than duplicating completion logic.


# Completion Handler

Completion handlers process job completion events and advance workflow execution after jobs leave RUNNING.

Their responsibilities include:

* Reading completion events from Redis.
* Persisting terminal job status and timing data to SQLite.
* Releasing workflow execution slots.
* Advancing dependency state for successful jobs.
* Promoting newly eligible successors through READY and QUEUED when capacity allows.
* Driving workflow-level finalization when all jobs reach terminal states.

Completion handlers are runtime-managed components, like schedulers. Multiple completion handlers may run in the same process, and each can be stopped independently. Completion-handler shutdown uses the same idempotent `request_stop()` pattern used by schedulers.

The current completion transport uses a Redis Stream plus a notification channel. A worker appends a completion record and publishes a wake-up notification; a handler atomically takes the oldest stream entry using `XRANGE` + `XDEL` before processing it. This prevents two handlers from dequeuing the same stream entry, but it is currently **at-most-once after dequeue**: a handler/process failure after `XDEL` and before completion processing finishes can lose that event. The planned reliability improvement is a Redis Stream consumer group (`XREADGROUP`/pending entries/`XACK`) so completion records remain recoverable until processing succeeds.

The current completion implementation is intentionally still under validation. Some workflow counters are calculated from a previously fetched workflow snapshot and then written back. With multiple completion handlers, two jobs from the same workflow can therefore race and lose a counter update. Before multi-handler execution is treated as data-integrity safe, these completion-side read/modify/write sequences should be consolidated into idempotent atomic Redis transitions. Duplicate/redelivered completion events must also become safe no-ops after the first valid terminal transition.

---

# Scheduler Runtime Data Access

The scheduler uses **Redis exclusively for normal runtime execution**.

It does not query SQLite for:

* Job payloads.
* Dependency state.
* Workflow runtime counters.
* Job execution state.
* Ready jobs.
* Delayed retry jobs.
* Successor information.
* Other runtime scheduling information.

This is a deliberate architectural decision.

The scheduler must be able to operate as an asynchronous execution engine without introducing database latency or synchronous database operations into the scheduling path.

The runtime path is therefore:

```text
Scheduler
    |
    +--> queued_jobs
    |
    +--> workflow runtime
    |
    +--> job runtime
    |
    +--> dependency state
    |
    +--> job payload
    |
    +--> execution state
    |
    +--> workflow counters
    |
    v
  Redis
```

SQLite remains relevant for persistence and recovery, but is outside the scheduler's normal execution loop.

---

# Ready Jobs

`execution_queue` is the primary scheduler input structure and is implemented as a Redis sorted set ordered by job priority.

`execution_queue` contains **QUEUED** jobs: jobs whose dependencies are satisfied and for which the workflow has reserved an execution slot. Once a scheduler pops and claims a job from this queue, the job leaves the global queue and becomes **PENDING_EXECUTION** under that scheduler's ownership.

The runtime transitions are:

```text
PENDING
   |
   | dependencies satisfied
   v
 READY
   |
   | workflow reserves execution slot
   v
 QUEUED  ---> inserted into execution_queue
   |
   | scheduler claims ownership
   v
 PENDING_EXECUTION
   |
   | worker fetches job
   v
 RUNNING
```

The states have distinct meanings:

* **READY** — dependencies are satisfied; no execution slot is reserved.
* **QUEUED** — an execution slot is reserved; the job is eligible for scheduler claim.
* **PENDING_EXECUTION** — a scheduler owns the job and is waiting to hand it to a worker.
* **RUNNING** — a worker has fetched the job and execution has started.

Per-workflow concurrency is represented by:

```text
reserved_execution_slots = QUEUED jobs + PENDING_EXECUTION jobs + RUNNING jobs
```

The slot is reserved on READY → QUEUED and released when the execution attempt completes or otherwise leaves the slot-consuming lifecycle.

# Runtime State

Redis contains the runtime representation of active workflows.

The runtime state is optimized for the operations required by the scheduler rather than for durable relational storage.

Typical runtime information includes:

| Runtime Information      | Purpose                              |
| ------------------------ | ------------------------------------ |
| Workflow runtime         | Track active workflow execution      |
| Job runtime              | Track individual job state           |
| Job payload              | Provide execution input              |
| Dependency state         | Determine whether jobs are ready     |
| Successor information    | Identify jobs affected by completion |
| Ready jobs               | Scheduler input queue                |
| Delayed retry queue      | Park retryable jobs until eligible   |
| Execution counters       | Enforce workflow/runtime limits      |
| Workflow execution state | Determine workflow progress          |

The runtime model includes a global `execution_queue` sorted set, a delayed retry sorted set, and workflow/job runtime keys. Job runtime data includes status, dependency count, priority, retry information, timeout information, and scheduler ownership (`owned_by`). Successor relationships are stored separately so dependency advancement can be performed without querying SQLite.

The Redis representation is optimized for runtime scheduling and recovery operations.

---

# Dependency Scheduling

FlowPilot workflows are immutable DAGs validated during admission.

During execution, Redis maintains each job's remaining dependency count and successor relationships. When a job completes successfully, the remaining dependency count of each successor is updated.

```text
dependency satisfied
        |
        v
remaining_dependencies == 0
        |
        v
      READY
        |
        | workflow execution slot available
        v
      QUEUED
        |
        v
insert into execution_queue
        |
        v
 scheduler claims ownership
        |
        v
 PENDING_EXECUTION
```

Independent branches can therefore become READY concurrently, while the workflow's execution-slot limit controls how many are promoted to QUEUED.

# Job Execution Lifecycle

FlowPilot uses explicit execution states so that dependency waiting, scheduler queueing, and actual execution can be measured independently.

```text
PENDING
   |
   | remaining_dependencies == 0
   v
 READY
   |
   | workflow reserves execution slot
   v
 QUEUED
   |
   | scheduler claims ownership
   v
 PENDING_EXECUTION
   |
   | worker fetches job
   v
 RUNNING
   |
   +------> COMPLETED
   |
   +------> FAILED
   |
   +------> ABORTED
```

The current job states are:

* `PENDING` — waiting for dependencies.
* `READY` — dependencies are satisfied and the job is globally schedulable.
* `QUEUED` — granted one of the workflow's execution slots and eligible for scheduler claim.
* `PENDING_EXECUTION` — claimed by a scheduler and waiting for worker execution.
* `RUNNING` — fetched by a worker; execution timing starts here.
* `RETRY_DELAY` — failed attempt is parked until the retry deadline expires.
* `COMPLETED` — completed successfully.
* `FAILED` — execution failed with no further retry scheduled.
* `ABORTED` — execution was aborted by workflow/runtime failure.
* `CANCELED` — reserved for future client-requested workflow cancellation.

Retry is modeled orthogonally to the state machine using the retry counter. A retry-eligible job is scheduled in the Redis retry queue and returns to READY/QUEUED after the configured delay rather than entering a separate RETRYING state.

Redis keeps READY, QUEUED, PENDING_EXECUTION, and RUNNING distinct so runtime logic can separate dependency readiness, workflow slot reservation, scheduler ownership, and actual execution. SQLite persists the durable status transitions and timestamps that are part of the current persisted lifecycle, especially READY, QUEUED, RUNNING, and terminal outcomes.

# Retry Handler

The retry handler is a runtime-owned component that promotes retry-eligible jobs after their retry delay expires.

When a RUNNING job reports FAILED and has retry attempts remaining, the completion handler:

* increments the job's retry count,
* clears scheduler/worker ownership fields,
* moves the job to RETRY_DELAY,
* records a retry deadline in the Redis retry sorted set,
* releases the workflow execution slot consumed by the failed attempt,
* promotes another waiting READY job to QUEUED if workflow capacity is available.

The retry deadline is:

```text
retry_at_ms = now + retry_delay_sec + jitter(1ms..1000ms)
```

The retry handler waits for due retry entries, removes one eligible job from the retry set, and promotes it according to workflow capacity:

```text
RETRY_DELAY due
       |
       | workflow running and slot available
       v
     READY
       |
       | if capacity remains
       v
     QUEUED -> execution_queue
```

If the workflow has an open execution slot and the workflow waiting-ready queue is empty, the retried job is inserted directly into `execution_queue` and marked QUEUED. This handles the important case where every other workflow job has completed and the retried job is the only remaining work. If the workflow has no open slot, the retried job remains READY in the workflow waiting-ready queue.

The retry handler persists READY/QUEUED transitions to SQLite after Redis promotion succeeds. Redis remains the source of truth for active execution; SQLite keeps the durable audit/recovery status.


# Worker / Executor Integration

The scheduler should not contain the implementation details of individual job execution.

Instead, it dispatches jobs through a worker/executor abstraction.

The executor layer may eventually support:

* Local workers.
* Docker-based workers.
* Kubernetes-based workers.
* Remote workers running on client infrastructure.
* Go-based workers.
* Other worker implementations.

The workflow runtime determines whether a job is execution-eligible, the scheduler determines which eligible job is dispatched next, and the executor is responsible for **how the job executes**.

Conceptually:

```text
Scheduler
    |
    | execute(job)
    v
Executor Interface
    |
    +--> Local Executor
    +--> Docker Executor
    +--> Kubernetes Executor
    +--> Remote gRPC Executor
```

---

# SQLite

SQLite is the durable persistence layer.

It stores information required to maintain a durable record of submitted and admitted workflows.

Persisted information includes:

* Workflow requests.
* Workflow metadata.
* Job definitions.
* Dependency definitions.
* Admission state.
* Durable execution information required for recovery/auditing.

SQLite is not part of the scheduler's normal runtime execution path.

Instead, SQLite provides the durable foundation from which runtime state can be reconstructed after failure.

---

# Redis

Redis has two distinct architectural roles.

## Admission Coordination

Redis is used for:

* Request idempotency.
* Rate limiting.
* Concurrent workflow limits.
* Admission coordination.

Admission control uses atomic Redis operations where required.

## Workflow Runtime

Redis is also the runtime store used by the execution subsystem.

It contains:

* Active workflow runtime state.
* Job runtime state.
* Dependency runtime state.
* Job payloads.
* Ready jobs.
* Execution counters.
* Other transient execution state.

This distinction is important:

```text
Redis
 |
 +-- Admission coordination
 |
 +-- Workflow runtime
```

The runtime portion of Redis is authoritative for **active execution state**, while SQLite remains authoritative for **durable persisted state**.

---

# Runtime Status API

FlowPilot exposes a versioned runtime status endpoint:

```text
GET /api/v1/runtime/status
```

The initial response reports the process-local runtime state:

```json
{
  "status": "running",
  "started": true,
  "shutting_down": false,
  "redis_io_threads": 1,
  "scheduler_count": 1,
  "completion_handler_count": 1,
  "retry_handler_count": 1
}
```

This endpoint is intentionally small today. It is the foundation for operational visibility such as scheduler UUIDs, Redis connection identifiers, thread identifiers, per-component health, queue depths, retry backlog, and throughput counters.

---

# Failure and Recovery

Redis runtime state is intentionally treated as reconstructable execution state.

SQLite contains the durable workflow definition required to reconstruct execution after a failure.

Recovery is therefore conceptually:

```text
SQLite
  |
  | durable workflow definition
  v
Runtime reconstruction
  |
  v
Redis runtime state
  |
  v
Scheduler
```

Runtime reconstruction is part of the recovery architecture; distributed scheduler/node-failure recovery is planned for the distributed-orchestration phase.

---

# Asynchronous Architecture

## Network Layer

Boost.Asio coroutines are used throughout the HTTP stack.

Primary abstractions include:

```text
co_await
use_awaitable
awaitable<T>
```

## Redis

Redis operations are exposed through asynchronous APIs and integrated directly into the coroutine execution model.

Scheduler, completion-handler, and retry-handler main loops run as asynchronous Redis-driven coroutines on the Redis I/O context. Blocking local worker execution remains outside the shared Redis I/O threads.

`--redis-io-threads` controls how many threads run the Redis Asio `io_context`; it is execution concurrency, not a guarantee of independent Redis command connections. Connection-level request/reply serialization (or pooling) is required before the ordinary shared command path can safely exploit concurrent Redis I/O threads.

## SQLite

SQLite is synchronous by nature.

Blocking database operations are isolated behind asynchronous wrappers and dedicated execution threads.

SQLite access remains outside the scheduler's normal execution path.

---

# Concurrency

FlowPilot supports multiple levels of concurrency control.

Admission controls the number of workflows that may become active for a client.

Workflow runtime admission controls per-workflow execution concurrency by granting execution slots to READY jobs. The scheduler operates only on jobs that have already been granted such a slot.

The scheduler must therefore distinguish between:

* Workflow-level execution-slot capacity.
* `reserved_execution_slots`, which counts QUEUED, PENDING_EXECUTION, and RUNNING jobs.
* Jobs claimed by a scheduler but not yet running.
* Global worker/executor capacity.
* Client-level execution policy where applicable.

A job becoming QUEUED is not equivalent to it becoming RUNNING. QUEUED means the workflow has reserved capacity for the job and placed it in the global execution queue. PENDING_EXECUTION means a scheduler owns the job but no worker has started it yet. RUNNING means a worker has actually fetched it. Therefore `reserved_execution_slots = QUEUED jobs + PENDING_EXECUTION jobs + RUNNING jobs`, and the execution timeout starts only at RUNNING.

Each scheduler uses a bounded local worker queue and priority-ordered dispatch. Scheduler-local fairness mechanisms can be added without changing persisted job priority. Multiple scheduler instances may run in one process, and a scheduler can be stopped and removed while other schedulers continue consuming Redis runtime work.


Important runtime invariants for concurrent execution are:

```text
reserved_execution_slots = QUEUED jobs + PENDING_EXECUTION jobs + RUNNING jobs consuming workflow capacity

A completion event changes terminal state/counters at most once

A QUEUED job is present in the global execution queue
A PENDING_EXECUTION job is owned by a live scheduler that can progress/recover it
```

The last invariant is not yet fully recoverable across scheduler/process failure. Scheduler ownership (`owned_by`) provides the information required for later dead-scheduler reclamation, but the distributed recovery coordinator is still planned.

---

# Current Implementation Status

## Phase 1 — Workflow Admission ✅

The admission subsystem is feature-complete.

Implemented:

* HTTP API and request routing.
* JSON parsing and JSON Schema validation.
* Client validation and policy enforcement.
* Request idempotency.
* Redis admission control.
* Rate limiting and concurrent-workflow limits.
* Semantic workflow validation.
* Dependency and DAG validation.
* SQLite persistence and durable request auditing.
* Redis runtime initialization.
* Initial READY-job preparation.
* Redis `execution_queue` scheduler interface.
* Unit testing.

## Phase 2 — Workflow Execution 🚧

The execution model, persistence support, and process-local runtime lifecycle have been refined around scheduler dispatch, worker execution, completion handling, and clean shutdown.

Implemented/refined:

* Explicit `PENDING`, `READY`, `QUEUED`, `PENDING_EXECUTION`, `RUNNING`, `COMPLETED`, `FAILED`, and `ABORTED` job states.
* Redis runtime support for READY/QUEUED/PENDING_EXECUTION/RUNNING lifecycle transitions, with durable SQLite support for persisted lifecycle timestamps and terminal outcomes.
* Redis runtime representation for scheduler-visible job state.
* Priority-aware `execution_queue` sorted-set design.
* Workflow execution-slot reservation tracked through `reserved_execution_slots`.
* Scheduler ownership through `owned_by`, separate from workflow slot reservation.
* Separation of scheduler queueing time from actual job execution time.
* Delayed retry semantics based on retry count and retry deadline rather than a separate RETRYING state.
* Unit tests covering the updated persistence/state-transition behavior.
* Runtime manager for Redis I/O threads, schedulers, completion handlers, and the retry handler.
* Configurable Redis I/O, scheduler, and completion-handler counts.
* Dynamic add/remove operations for schedulers and completion handlers.
* Idempotent scheduler, completion-handler, and retry-handler stop requests.
* Detached scheduler, completion-handler, and retry-handler main loops on the Redis I/O context.
* Retry promotion that queues the retried job directly when workflow capacity is available and no other READY jobs are waiting.
* Clean shutdown process tests for default and multi-component configurations.
* Runtime status API for process-local component counts and lifecycle flags.

Current execution focus:

1. Make the ordinary Redis command path safe under multiple Redis I/O threads (transaction serialization or a connection pool).
2. Consolidate completion-side job/workflow/counter updates into idempotent atomic Redis transitions.
3. Replace destructive completion dequeue with consumer-group acknowledgement/recovery semantics.
4. Complete dependency advancement, successor promotion, and workflow finalization.
5. Add scheduler-local worker health monitoring, dead-owner recovery, and richer runtime status/metrics.
6. Add concurrency stress/invariant tests, including simultaneous completions for one workflow and scheduler/completion-handler churn under load.

## Concurrency and Integrity Test Strategy

The multi-component runtime should be validated primarily through invariants rather than only single-operation unit tests. High-value regression/stress scenarios include:

* Concurrent ordinary Redis commands with more than one Redis I/O thread.
* Two completion handlers processing different jobs from the same workflow simultaneously.
* Concurrent successful and terminal-failure completion for the same workflow.
* Duplicate/redelivered completion events; counters and slots must change only once.
* Scheduler removal after Redis dequeue but before/after local worker handoff.
* Scheduler and completion-handler add/remove churn while many workflows execute.
* End-of-run reconciliation of Redis counters, job states, execution-queue membership, and SQLite durable state.
* ThreadSanitizer runs for lifecycle and concurrency tests, with AddressSanitizer/UBSan as complementary configurations.

## Phase 3 — Distributed Orchestration

Planned:

* Multiple scheduler instances across processes or machines.
* Distributed scheduler coordination and dead-scheduler ownership recovery.
* Scheduler health monitor for dispatcher and worker-thread responsiveness.
* Scheduler-local recovery for non-responsive worker threads and their in-flight jobs.
* Redis runtime reconstruction after failure.
* Horizontal worker scaling.
* Remote worker execution.
* Kubernetes integration.
* Observability and operational tooling built on the runtime status/health surface.
* Optional fairness/priority-aging policies.


# Architectural Boundary

The most important boundary in the current architecture is:

```text
                    WORKFLOW ADMISSION
                           |
                           |
             Durable      |      Runtime
             persistence  |      initialization
                           |
                    +------v------+
                    |    Redis    |
                    | Runtime     |
                    +------+------+
                           |
          execution_queue (QUEUED jobs, ZSET)
                           |
                    +------v------+
                    |  Scheduler  |
                    +------+------+
                           |
              PENDING_EXECUTION ownership
                           |
                    Job execution
                           |
                    +------v------+
                    |   Workers   |
                    +-------------+
```

Admission answers:

> **"Is this workflow valid and allowed to execute, and what runtime state is required to start it?"**

The scheduler answers:

> **"Given the jobs that workflows have already made eligible for execution, which QUEUED jobs should be claimed as PENDING_EXECUTION, prioritized, and dispatched next?"**

This separation is central to FlowPilot's architecture.

The scheduler should not revalidate workflow definitions, perform admission decisions, or query SQLite for every execution operation.

Its responsibility is to efficiently and reliably drive the execution of workflows using the runtime state already prepared in Redis.
