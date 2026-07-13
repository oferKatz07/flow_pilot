# FlowPilot

A distributed reliable workflow orchestration platform designed for reliable asynchronous task execution, dependency-aware scheduling, and extensible worker integration.

FlowPilot is a modern backend infrastructure project that explores production-grade workflow orchestration using asynchronous C++, coroutines, Redis, and SQLite.

The project focuses on the engineering challenges behind reliable distributed systems rather than workflow business logic. Its architecture emphasizes idempotent request admission, dependency-aware scheduling, durable request auditing and workflow persistence, and scalable asynchronous execution.

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


# Key Design Decisions

FlowPilot is being developed in phases. 
The current implementation focuses on building a robust admission layer that guarantees only valid workflows are registered for execution. 
Subsequent phases build the execution engine and distributed orchestration capabilities on top of this foundation.

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

The workflow admission subsystem is feature-complete and includes idempotent request handling, client policy enforcement, Redis-based admission control, durable request auditing, semantic workflow validation, and DAG dependency validation. Current development is focused on the workflow execution engine, including job scheduling, worker dispatch, and execution lifecycle management.


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

🚧 Scheduler

🚧 Worker execution

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
- Unit testing

### Phase 2 — Workflow Execution (In Progress)

- Job initialization
- Scheduler
- Worker dispatch
- Execution monitoring

### Phase 3 — Distributed Orchestration (Planned)

- Multi-node scheduler
- Recovery
- Horizontal scaling
- Observability

---

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
