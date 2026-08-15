# FlowPilot Architecture

## Overview

FlowPilot is a C++20 distributed workflow orchestration platform designed around asynchronous I/O, coroutine-based processing, durable workflow persistence, dependency-aware scheduling, and extensible worker integration.

The system is designed to separate **workflow admission** from **workflow execution**.

The admission subsystem validates incoming workflows, enforces client policies and admission limits, persists the workflow definition, and prepares the runtime state required for execution.

The scheduler then operates on the prepared runtime state in Redis. During normal execution, the scheduler does not access SQLite. Redis contains the runtime state required to schedule and execute jobs, while SQLite provides durable persistence and the basis for recovery.

The current implementation has completed the workflow admission phase and is moving into the workflow execution phase, beginning with the scheduler.

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

### 6. Ready Jobs Are the Admission-to-Scheduler Boundary

The `ready_jobs` Redis queue is the explicit interface between workflow admission/runtime initialization and the scheduler.

A workflow becomes executable only after its runtime state has been prepared and its initially ready jobs have been placed into `ready_jobs`.

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
                  | ready_jobs
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
             ready_jobs
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

After a workflow has successfully passed admission and has been durably persisted, the admission subsystem prepares the Redis runtime representation.

Runtime initialization includes the data required by the scheduler to execute the workflow without accessing SQLite.

The runtime representation may include:

* Workflow runtime metadata.
* Job runtime state.
* Dependency information.
* Successor information.
* Job payloads.
* Runtime counters.
* Execution-related metadata.

The initial jobs whose dependencies are already satisfied are identified as ready jobs.

Only after the required runtime data has been successfully created are the ready jobs placed into the Redis `ready_jobs` queue.

This ordering is intentional:

```text
Persist workflow
       |
       v
Create runtime data in Redis
       |
       v
Prepare dependency state
       |
       v
Create initial ready jobs
       |
       v
Push ready jobs to ready_jobs
       |
       v
Scheduler may execute
```

The `ready_jobs` queue therefore acts as a synchronization boundary: a job appearing there implies that the runtime state required by the scheduler has already been prepared.

---

# Scheduler

The scheduler is the core component of the workflow execution subsystem.

Its primary responsibility is to manage the execution of admitted jobs.

The scheduler consumes jobs from the Redis `ready_jobs` queue and coordinates their execution according to the workflow runtime state.

## Scheduler Responsibilities

The scheduler is responsible for:

* Consuming ready jobs from `ready_jobs`.
* Obtaining job runtime information from Redis.
* Enforcing execution concurrency limits.
* Dispatching jobs to workers/executors.
* Tracking job execution state.
* Handling job completion.
* Handling job failure.
* Applying retry policies.
* Updating workflow runtime state.
* Detecting newly satisfied dependencies.
* Making successor jobs ready.
* Completing workflows.
* Handling workflow execution failures and compensation where applicable.

The scheduler should remain independent of the HTTP and admission layers.

---

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
    +--> ready_jobs
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

`ready_jobs` is the primary scheduler input queue.

A job is placed into `ready_jobs` when all conditions required for its execution have been satisfied.

Initially, this is performed during workflow runtime initialization for jobs that have no unresolved dependencies.

Later, completed jobs can cause successor jobs to become ready. Those jobs are then added to `ready_jobs`.

Conceptually:

```text
Job A
  |
  v
Job B
  |
  v
Job C
```

Initially:

```text
ready_jobs = [A]
```

After A completes successfully:

```text
ready_jobs = [B]
```

After B completes successfully:

```text
ready_jobs = [C]
```

The scheduler therefore operates as a dependency-aware execution engine rather than as a simple FIFO worker queue.

---

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

The exact Redis key structure is an implementation detail and may evolve as the scheduler implementation progresses.

---

# Dependency Scheduling

FlowPilot represents workflows as DAGs.

During admission, the DAG is validated and guaranteed to be executable.

During execution, the scheduler maintains runtime dependency state.

When a job completes successfully, its successors are evaluated.

For each successor:

```text
dependency satisfied
        |
        v
remaining dependencies == 0
        |
        v
job becomes READY
        |
        v
push to ready_jobs
```

This allows independent branches of a workflow to execute concurrently.

---

# Job Execution Lifecycle

The scheduler manages the runtime lifecycle of jobs.

A simplified lifecycle is:

```text
READY
  |
  v
RUNNING
  |
  +---------> RETRY_PENDING
  |                |
  |                v
  |              READY
  |
  +---------> COMPLETED
  |
  +---------> FAILED
```

The exact state machine will be refined during scheduler implementation.

The important architectural principle is that job state transitions are represented in Redis runtime state.

---

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

The scheduler is therefore responsible for **when and whether a job executes**, while the executor is responsible for **how the job executes**.

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

Recovery is a later execution-phase capability and is not required for the initial scheduler implementation.

The initial scheduler can therefore focus on correct normal execution semantics before distributed recovery is introduced.

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

The scheduler controls job execution concurrency according to runtime and client policy.

The scheduler must therefore distinguish between:

* Workflow-level concurrency.
* Global job execution concurrency.
* Client-level job concurrency.
* Worker/executor capacity.

The exact scheduling policy will be implemented and refined during the scheduler phase.

---

# Current Implementation Status

## Phase 1 — Workflow Admission ✅

The admission subsystem is feature-complete.

Implemented:

* HTTP API.
* Request routing.
* JSON parsing.
* JSON Schema validation.
* Client validation.
* Client policy enforcement.
* Request idempotency.
* Redis admission control.
* Rate limiting.
* Concurrent workflow limits.
* Semantic workflow validation.
* Dependency validation.
* DAG validation.
* SQLite persistence.
* Durable request auditing.
* Runtime initialization.
* Initial ready-job preparation.
* Redis `ready_jobs` scheduler interface.
* Unit testing.

## Phase 2 — Workflow Execution 🚧

Current development phase.

Planned implementation order:

1. Scheduler runtime abstraction.
2. Redis runtime access.
3. `ready_jobs` consumption.
4. Job execution lifecycle.
5. Execution concurrency control.
6. Worker/executor abstraction.
7. Job completion processing.
8. Dependency advancement.
9. Successor readiness.
10. Workflow completion.
11. Retry handling.
12. Failure handling.
13. Compensation handling.
14. Execution monitoring.

## Phase 3 — Distributed Orchestration

Planned:

* Distributed scheduler coordination.
* Multiple scheduler instances.
* Recovery after process/node failure.
* Redis runtime reconstruction.
* Horizontal worker scaling.
* Remote worker execution.
* Kubernetes integration.
* Observability and operational tooling.

---

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
                      ready_jobs
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

> **"Given the active runtime state, which jobs can execute now, and how should they be dispatched and progressed?"**

This separation is central to FlowPilot's architecture.

The scheduler should not revalidate workflow definitions, perform admission decisions, or query SQLite for every execution operation.

Its responsibility is to efficiently and reliably drive the execution of workflows using the runtime state already prepared in Redis.
