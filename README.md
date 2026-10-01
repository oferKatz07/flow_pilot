# FlowPilot

A distributed workflow orchestration platform designed for reliable asynchronous task execution, dependency-aware scheduling, and extensible worker integration.

FlowPilot is a modern backend infrastructure project that explores production-grade workflow orchestration using asynchronous C++, coroutines, Redis, and SQLite.

The project focuses on the engineering challenges behind reliable distributed systems rather than workflow business logic. Its architecture emphasizes idempotent request admission, dependency-aware scheduling, durable request auditing and workflow persistence, and scalable asynchronous execution.

The admission subsystem is feature-complete. Current development is focused on the workflow execution engine: Redis-backed runtime state, explicit READY -> QUEUED -> PENDING_EXECUTION -> RUNNING job transitions, runtime-owned scheduler and completion-handler components, worker dispatch, completion handling, clean shutdown, and recovery.

FlowPilot is being developed as a portfolio-quality system architecture project demonstrating modern C++ backend design, concurrent programming, and infrastructure engineering.

---

# Engineering Objectives

* Build a reliable workflow orchestration platform using modern asynchronous C++.
* Design an admission pipeline that guarantees idempotency, rate limiting, and durable request tracking.
* Explore distributed systems concepts including concurrency control, dependency scheduling, and eventual scalability.
* Demonstrate clean software architecture through dependency injection, modular components, and comprehensive unit testing.
* Provide a production-inspired codebase suitable for experimentation with scheduling algorithms, worker pools, retries, and distributed execution.

The project emphasizes clean architecture, operational semantics, and realistic infrastructure design rather than CRUD-oriented backend development.

---

# Core Concepts

## Workflow

A workflow is the primary orchestration entity in the system.

A workflow contains:

* Jobs
* Dependencies
* Retry policies
* Compensation actions
* Execution metadata

Workflows are represented internally as directed acyclic graphs (DAGs).

## Job

A job is an executable task within a workflow.

Examples:

* Charge payment
* Reserve inventory
* Send notification
* Generate report

Jobs may depend on completion of other jobs.

## Request

Requests represent API operations submitted to FlowPilot.

Examples:

* Submit workflow
* Append workflow tasks
* Cancel workflow

Requests are uniquely identified by:

(client_id, request_id)

This enables:

* Idempotency
* Safe retries
* Duplicate request detection
* Multi-tenant isolation

---

# Architecture Overview

```text
                +----------------------+
                |    API Gateway       |
                |  Boost.Beast HTTP    |
                +----------+-----------+
                           |
                           v
                +----------------------+
                |  Workflow Service    |
                | Admission &          |
                | Validation Layer     |
                +----------+-----------+
                           |
        +------------------+------------------+
        |                                     |
        v                                     v
+-------------------+             +-------------------+
| Redis             |             | SQLite            |
| Admission Layer   |             | Durable Storage   |
+-------------------+             +-------------------+
                           |
                           v
                +----------------------+
                | Scheduler / Queue    |
                +----------+-----------+
                           |
                           v
                +----------------------+
                | Workers / Executors  |
                +----------------------+
```

---

# Design Principles

• Keep the HTTP layer free of business logic.
• Separate admission control from workflow execution.
• Redis provides fast coordination; SQLite provides durable persistence.
• Validate workflows before they enter the execution engine.
• Prefer explicit workflow state over implicit behavior.
• Build around asynchronous I/O and coroutine-based composition.
• Keep runtime lifecycle ownership explicit so schedulers, completion handlers, and Redis I/O threads can be started, stopped, and eventually scaled independently.


# Key Design Decisions

FlowPilot is being developed in phases.
The current implementation has a robust admission layer that guarantees only valid workflows are registered for execution, and the execution engine is now being built on top of that foundation.
The active execution work focuses on scheduler dispatch, worker lifecycle transitions, completion handling, retry behavior, and recovery.

## Atomic Workflow Admission

Workflow requests pass through a multi-stage admission pipeline. Fast, transient admission decisions (duplicate detection, rate limiting, concurrent workflow limits) are handled by Redis. Once admitted, requests are durably recorded in a database (SQLite) before workflow validation continues. The database is the authoritative audit trail and system of record, while Redis remains a transient coordination layer. The admission validation steps are:

- JSON parsing
- JSON schema validation
- Client Lookup
- Rate limiting and duplication detection (Redis)
- Persist request
- policy validation
- Semantic workflow validation
- Workflow dependency validation
- Workflow DAG cycle detection
- Persist workflow
- Register workflow for execution
- HTTP response

Execution begins only after the workflow is successfully persisted.

## Immutable Workflow Definitions

Workflow DAGs are immutable after admission to simplify execution semantics and avoid runtime graph mutation complexity.

## Asynchronous Execution

Workflow execution is fully asynchronous.

The API gateway never blocks waiting for workflow completion.

## Explicit Job Execution Lifecycle

FlowPilot separates dependency readiness, workflow execution-slot reservation, scheduler dispatch, and actual execution.

The current runtime job lifecycle is:

```text
PENDING -> READY -> QUEUED -> PENDING_EXECUTION -> RUNNING -> COMPLETED | FAILED | CANCELED
```

`READY` means dependencies are satisfied. `QUEUED` means the workflow has reserved an execution slot and the job is eligible for scheduler dispatch. `PENDING_EXECUTION` means a scheduler has claimed ownership of the job, but a worker has not started it yet. `RUNNING` begins only when a worker fetches the job and the execution timer starts.

Redis is the runtime authority for these transitions during execution, while SQLite persists durable status and timestamp history.

## Runtime Ownership and Thread Configuration

FlowPilot now owns runtime execution components through a `FlowPilotRuntime` manager. The runtime creates and shuts down Redis I/O threads, workflow schedulers, and completion handlers as one coordinated lifecycle while still allowing schedulers and completion handlers to be added or removed at runtime.

The default thread configuration is intentionally conservative:

* Redis I/O threads: `1`
* Workflow schedulers: `1`
* Completion handlers: `1`

The executable accepts these overrides:

```bash
./flow_pilot --redis-io-threads 2 --schedulers 2 --completion-handlers 2
```

`--redis-io-threads` controls the number of threads running the Redis Asio I/O context; it does not by itself create independent Redis command connections. Schedulers and completion handlers can be added/removed while the runtime is active; Redis I/O thread count is currently a startup configuration.

Each scheduler and completion handler can be stopped independently. Shutdown is idempotent, so `request_stop()` may be called by explicit runtime management code and again during destruction without double-stopping the same component.

## Runtime Status API

FlowPilot exposes an initial runtime status endpoint:

```text
GET /api/v1/runtime/status
```

Example response:

```json
{
  "status": "running",
  "started": true,
  "shutting_down": false,
  "redis_io_threads": 1,
  "scheduler_count": 1,
  "completion_handler_count": 1
}
```

This API currently reports lifecycle state and component counts. It is intended to grow into the operational status surface for runtime component UUIDs, per-thread statistics, scheduler health, and completion-handler metrics.


## Explicit Compensation

Rollback semantics are implemented using explicit compensation jobs rather than distributed transactions.

## Client Configuration

Each registered client is associated with:

• admission policy
• rate-limit policy
• workflow policy

These policies are loaded during initialization and applied during workflow admission.

---

# Technology Stack

## Core

- Modern C++20
- Boost.Asio (coroutines)
- Boost.Beast
- nlohmann/json
- JSON Schema validation (nlohmann/json-schema)
- Redis
- SQLite

## Planned Extensions

* Docker
* Kubernetes deployment
* Prometheus metrics
* Grafana dashboards
* Go-based workers
* Distributed scheduler coordination
* Runtime status, health monitoring, and recovery for schedulers and worker threads

---

# Example Workflow

```json
{
  "workflow_id": "order-1001",
  "jobs": [
    {
      "job_id": "reserve-inventory",
      "type": "reserve_inventory"
    },
    {
      "job_id": "charge-payment",
      "type": "charge_payment",
      "depends_on": ["reserve-inventory"]
    }
  ]
}
```

---

## Implementation Status

The workflow admission subsystem is feature-complete and includes idempotent request handling, client policy enforcement, Redis-based admission control, durable request auditing, semantic workflow validation, DAG dependency validation, SQLite persistence, Redis runtime initialization, and initial execution-queue population.

The workflow execution subsystem is in progress. The current code supports Redis runtime state, priority-based execution queueing, scheduler ownership through `owned_by`, bounded scheduler-local worker dispatch, QUEUED -> PENDING_EXECUTION -> RUNNING transitions, a Redis Stream-based completion path, runtime-managed scheduler/completion-handler lifecycles, configurable Redis I/O/scheduler/completion-handler counts, dynamic scheduler/completion-handler add/remove, clean shutdown tests, a runtime status API, and durable SQLite lifecycle timestamps.

The new multi-component runtime also exposes the next data-integrity work clearly. The ordinary Redis command connection must be serialized per request/reply transaction (or replaced by a connection pool) before multiple Redis I/O threads are considered fully safe. Completion-side read/modify/write updates must become atomic and idempotent before multiple completion handlers are considered data-integrity safe, and the current destructive stream dequeue should evolve to consumer-group acknowledgement/recovery semantics. These are active execution-engine tasks rather than completed guarantees.


### Current Project Status

✔ HTTP API

✔ Workflow validation

✔ JSON Schema validation

✔ DAG validation

✔ Client policies

✔ Request idempotency

✔ Redis admission control

✔ SQLite persistence

✔ Audit history

✔ Runtime manager for Redis I/O threads, schedulers, and completion handlers

✔ Runtime status API

✔ Clean shutdown process tests

✔ Redis runtime initialization

✔ Initial READY/QUEUED job preparation

✔ Priority execution queue

✔ Scheduler ownership for claimed jobs

✔ Local worker dispatch foundation

✔ Completion-handler runtime loop foundation

✔ Dynamic scheduler/completion-handler add/remove lifecycle

✔ READY/QUEUED/PENDING_EXECUTION/RUNNING lifecycle tests

🚧 Serialize/pool ordinary Redis command connections for multi-I/O-thread safety

🚧 Atomic/idempotent completion transitions for multi-handler safety

🚧 Reliable completion acknowledgement/recovery (Redis Stream consumer groups)

🚧 Scheduler recovery and health monitoring

🚧 Worker execution backend integration

🚧 Full dependency advancement after completion

🚧 Scheduler-local health monitor and recovery

🚧 Distributed workers

🚧 Retry engine

🚧 Compensation workflows

### Phase 1 — Workflow Admission ✅

- HTTP API
- Admission pipeline
- Redis coordination
- SQLite persistence
- Workflow validation
- DAG validation
- Redis runtime initialization
- Initial execution queue population
- Unit testing

### Phase 2 — Workflow Execution (In Progress)

- Explicit PENDING/READY/QUEUED/PENDING_EXECUTION/RUNNING job lifecycle
- Workflow execution-slot reservation
- Priority-based Redis execution queue
- Scheduler ownership for claimed jobs
- Bounded local scheduler queue
- Worker dispatch and PENDING_EXECUTION -> RUNNING transition
- Runtime-managed scheduler and completion-handler lifecycles
- Configurable Redis I/O, scheduler, and completion-handler counts
- Runtime status API
- Completion handling foundation
- Multi-handler-safe atomic completion transitions (in progress)
- Reliable completion acknowledgement/recovery (planned)
- Dependency advancement
- Retry handling
- Scheduler-local health monitoring and recovery

### Phase 3 — Distributed Orchestration (Planned)

- Multi-node scheduler
- Dead-scheduler ownership recovery
- Redis runtime reconstruction
- Horizontal scaling
- Observability

---

## Concurrency & Data-Integrity Focus

FlowPilot now supports multiple process-local schedulers and completion handlers and can run the Redis Asio I/O context on multiple threads. The lifecycle management for those components is implemented, but safe lifecycle scaling is not the same as atomic workflow-state scaling. Current hardening work is focused on:

* Serializing a complete Redis request/reply transaction per connection, or introducing a connection pool.
* Moving completion counter/slot/state changes into atomic, idempotent Redis transitions.
* Making completion delivery recoverable until processing succeeds.
* Reclaiming PENDING_EXECUTION/RUNNING work owned by a failed scheduler.
* Stress-testing invariants under simultaneous completions and runtime component churn.

The core execution invariant is that workflow capacity is reserved before scheduler dispatch: `reserved_execution_slots` represents QUEUED, PENDING_EXECUTION, and RUNNING jobs consuming workflow capacity. Redis is the active runtime authority; SQLite provides durable persisted state and recovery data.

## Building & Running Locally

Requirements

- C++20 toolchain (g++/clang++)
- CMake
- Boost (system)
- SQLite3
- Redis (for integration tests / runtime)

Build (debug):

```bash
cmake -B build
cmake --build build --target flow_pilot
```

Build tests only:

```bash
cmake --build build --target flow_pilot_tests
```

The project now builds a static library `flow_pilot_lib` (all sources except `src/main.cpp`) and links both the `flow_pilot` binary and `flow_pilot_tests` against it for faster incremental builds.

Run the application (example):

```bash
./build/flow_pilot
```

If you need a local Redis for integration tests or runtime, start one locally (Docker):

```bash
docker run -p 6379:6379 --rm redis:7
```

---

# Documentation

Additional architecture and design documents are available in:

```text
docs/
```

---

# Future Goals

* Distributed scheduler coordination
* Workflow persistence recovery
* Kubernetes deployment
* Worker autoscaling
* Advanced retry semantics
* Distributed tracing
* Dead-letter queues

---

# License

MIT
