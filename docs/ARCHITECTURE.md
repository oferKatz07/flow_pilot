# FlowPilot Architecture

## Overview

FlowPilot is a C++20 distributed workflow orchestration platform designed around asynchronous I/O, coroutine-based processing, durable workflow persistence, dependency-aware scheduling, and extensible worker integration.

The system is designed to separate **workflow admission** from **workflow execution**.

The admission subsystem validates incoming workflows, enforces client policies and admission limits, persists the workflow definition, and prepares the runtime state required for execution.

The scheduler then operates on the prepared runtime state in Redis. During normal execution, the scheduler does not access SQLite. Redis contains the runtime state required to schedule and execute jobs, while SQLite provides durable persistence and the basis for recovery.

The workflow admission phase is complete. The current execution architecture uses an explicit READY → QUEUED → RUNNING lifecycle, workflow-level execution-slot reservation, priority-ordered Redis scheduling, scheduler ownership, and durable execution timestamps/statuses in SQLite. Scheduler implementation is the current development phase.

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

A job is inserted into `execution_queue` only after its dependencies are satisfied and the workflow has reserved an execution slot for it, transitioning the job to QUEUED.

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

HTTP handlers are responsible for transport concerns and request routing.

Workflow admission and execution logic belong to dedicated services.

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
reserved_execution_slots = QUEUED jobs + RUNNING jobs
```

QUEUED jobs are execution-eligible and are inserted into the Redis `execution_queue` sorted set for scheduler dispatch.

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
Scheduler dispatch
```

This boundary separates workflow-specific execution eligibility from scheduling. The workflow runtime grants execution capacity; the scheduler prioritizes and dispatches jobs that have already been granted that capacity.

# Scheduler

The scheduler is the core component of the workflow execution subsystem.

Its primary responsibility is to select QUEUED jobs that have already been granted a workflow execution slot, prioritize them, and coordinate their dispatch into the worker/executor layer according to available worker capacity.

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
   | worker fetches job
   v
 RUNNING
   |
   +------> SUCCESS
   |
   +------> FAILED
   |
   +------> CANCELED
```

A QUEUED job has already consumed one of the workflow's execution slots, even though it may not yet be running. A retry does not require a separate RETRYING state. Retry intent is represented by the job's retry counter together with its normal execution state. A failed attempt that is eligible for retry is returned to READY when its retry delay has elapsed.

## Scheduler Responsibilities

The scheduler is responsible for:

* Consuming priority-ordered QUEUED jobs from the Redis scheduler-facing structure.
* Obtaining job and workflow runtime information from Redis.
* Treating QUEUED jobs as already admitted by the workflow for execution; the scheduler does not decide whether the workflow has a free execution slot.
* Recording scheduler ownership for jobs it has reserved.
* Maintaining a bounded local set of jobs waiting to be executed.
* Dispatching QUEUED jobs to available workers/executors.
* Transitioning a job to RUNNING only when a worker actually fetches it.
* Starting the execution-time budget when the worker fetches the job, rather than when the scheduler reserves it.
* Monitoring dispatcher and worker-thread health.
* Activating scheduler-local recovery when a worker thread becomes non-responsive.
* Processing completion/failure events.
* Advancing dependency state after successful completion.
* Making newly eligible successor jobs READY.
* Returning retry-eligible jobs to READY after their retry delay.
* Completing or failing workflows according to their aggregate job state.

The scheduler remains independent of the HTTP and workflow-admission layers.

## Scheduler Local Scheduling State

Redis remains the shared runtime authority, while each scheduler may maintain local structures optimized for efficient dispatch.

The scheduler uses:

* A bounded collection of jobs already reserved by the scheduler and waiting for worker capacity.
* A priority-ordered local container for efficient selection of the next job to execute.
* Scheduler identity stored in the job's `owned_by` field after a scheduler claims a QUEUED job. Workflow execution-slot reservation and scheduler ownership are separate concepts.
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
reserved_execution_slots = QUEUED jobs + RUNNING jobs
```

For a reclaimed job:

* If the job can be retried, recovery clears stale worker ownership metadata, updates retry bookkeeping, and returns the job to READY or QUEUED according to retry delay and workflow capacity.
* If the job cannot be retried, recovery finalizes it as FAILED or CANCELED through the same completion/finalization path used by normal execution.
* If the scheduler itself died, another scheduler or recovery coordinator reclaims the abandoned non-terminal jobs owned by the dead scheduler.

The completion handler remains the canonical place for releasing execution slots, updating workflow counters, advancing dependencies, and persisting final SQLite status. Recovery should therefore route recovered terminal outcomes through completion handling rather than duplicating completion logic.


# Scheduler Runtime Data Access

The scheduler uses **Redis exclusively for normal runtime execution**.

It does not query SQLite for:

* Job payloads.
* Dependency state.
* Workflow runtime counters.
* Job execution state.
* Ready jobs.
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

`execution_queue` contains **QUEUED** jobs: jobs whose dependencies are satisfied and for which the workflow has reserved an execution slot.

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
   | worker fetches job
   v
 RUNNING
```

The states have distinct meanings:

* **READY** — dependencies are satisfied; no execution slot is reserved.
* **QUEUED** — an execution slot is reserved; the job is eligible for scheduler/worker dispatch.
* **RUNNING** — a worker has fetched the job and execution has started.

Per-workflow concurrency is represented by:

```text
reserved_execution_slots = QUEUED jobs + RUNNING jobs
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
| Execution counters       | Enforce workflow/runtime limits      |
| Workflow execution state | Determine workflow progress          |

The runtime model includes a global `execution_queue` sorted set plus workflow/job runtime keys. Job runtime data includes status, dependency count, priority, retry information, timeout information, and scheduler ownership (`owned_by`). Successor relationships are stored separately so dependency advancement can be performed without querying SQLite.

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
   | worker fetches job
   v
 RUNNING
   |
   +------> SUCCESS
   |
   +------> FAILED
   |
   +------> CANCELED
```

The current job states are:

* `PENDING` — waiting for dependencies.
* `READY` — dependencies are satisfied and the job is globally schedulable.
* `QUEUED` — granted one of the workflow's execution slots and eligible for scheduler/worker dispatch.
* `RUNNING` — fetched by a worker; execution timing starts here.
* `SUCCESS` — completed successfully.
* `FAILED` — execution failed with no further retry scheduled.
* `CANCELED` — execution was canceled.

Retry is modeled orthogonally to the state machine using the retry counter. A retry-eligible job returns to READY after the configured delay rather than entering a separate RETRYING state.

SQLite persists the corresponding status transitions and timestamps. In particular, READY and QUEUED are kept distinct so future statistics can measure time spent dependency-ready, waiting for a workflow execution slot, queued for scheduler/worker dispatch, and actually running.


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

The scheduler should therefore be implemented as an asynchronous Redis-driven execution loop.

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
* `reserved_execution_slots`, which counts both QUEUED and RUNNING jobs.
* Jobs claimed by a scheduler but not yet running.
* Global worker/executor capacity.
* Client-level execution policy where applicable.

A job becoming QUEUED is not equivalent to it becoming RUNNING. QUEUED means the workflow has reserved capacity for the job; RUNNING means a worker has actually fetched it. Therefore `reserved_execution_slots = QUEUED jobs + RUNNING jobs`, and the execution timeout starts only at RUNNING.

The scheduler uses a bounded local queue and priority-ordered dispatch. Scheduler-local fairness mechanisms can be added without changing persisted job priority.

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

The execution model and persistence support have been refined in preparation for the scheduler implementation.

Implemented/refined:

* Explicit `PENDING`, `READY`, `QUEUED`, `RUNNING`, `SUCCESS`, `FAILED`, and `CANCELED` job states.
* Durable SQLite support for READY/QUEUED/RUNNING lifecycle transitions and execution timing.
* Redis runtime representation for scheduler-visible job state.
* Priority-aware `execution_queue` sorted-set design.
* Workflow execution-slot reservation tracked through `reserved_execution_slots`.
* Scheduler ownership through `owned_by`, separate from workflow slot reservation.
* Separation of scheduler queueing time from actual job execution time.
* Retry semantics based on retry count rather than a separate RETRYING state.
* Unit tests covering the updated persistence/state-transition behavior.

Current scheduler implementation focus:

1. Grant available workflow execution slots and transition eligible READY jobs to QUEUED.
2. Insert QUEUED jobs into the priority-ordered `execution_queue` sorted set.
3. Claim QUEUED jobs and maintain bounded local scheduler state.
4. Prioritize and dispatch QUEUED jobs to workers.
5. Transition QUEUED → RUNNING when a worker fetches the job.
6. Process completion and persist final status/timing.
7. Advance dependencies and promote eligible successors through READY → QUEUED.
8. Integrate delayed retry handling.
9. Complete/fail workflows from aggregate job state.
10. Add scheduler-local worker health monitoring and recovery.

## Phase 3 — Distributed Orchestration

Planned:

* Multiple scheduler instances.
* Distributed scheduler coordination and dead-scheduler ownership recovery.
* Scheduler health monitor for dispatcher and worker-thread responsiveness.
* Scheduler-local recovery for non-responsive worker threads and their in-flight jobs.
* Redis runtime reconstruction after failure.
* Horizontal worker scaling.
* Remote worker execution.
* Kubernetes integration.
* Observability and operational tooling.
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
                    Job execution
                           |
                    +------v------+
                    |   Workers   |
                    +-------------+
```

Admission answers:

> **"Is this workflow valid and allowed to execute, and what runtime state is required to start it?"**

The scheduler answers:

> **"Given the jobs that workflows have already made eligible for execution, which QUEUED jobs should be claimed, prioritized, and dispatched next?"**

This separation is central to FlowPilot's architecture.

The scheduler should not revalidate workflow definitions, perform admission decisions, or query SQLite for every execution operation.

Its responsibility is to efficiently and reliably drive the execution of workflows using the runtime state already prepared in Redis.
