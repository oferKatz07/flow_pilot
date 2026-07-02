# FlowPilot

A distributed workflow orchestration platform designed for reliable asynchronous task execution, dependency-aware scheduling, and extensible worker integration.

FlowPilot is a modern backend infrastructure project focused on orchestration, concurrency, reliability, and distributed systems design using modern C++ and asynchronous IO. real-world distributed systems concepts including:

---

# Project Goals

FlowPilot was created as a hands-on engineering project to explore and implement modern backend architecture concepts including:

* Asynchronous networking
* Workflow orchestration
* DAG-based dependency scheduling
* Distributed systems patterns
* Idempotent APIs
* Rate limiting
* Redis coordination
* Durable persistence
* Extensible execution models
* Observability and structured logging
* Cloud-oriented microservice architecture

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
                | Admission & DAG      |
                | Validation Layer     |
                +----------+-----------+
                           |
        +------------------+------------------+
        |                                     |
        v                                     v
+-------------------+             +-------------------+
| Redis             |             | SQLite            |
| Coordination Layer|             | Durable Storage   |
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

# Key Design Decisions

## Atomic Workflow Admission

Workflow requests undergo multiple validation stages before execution begins:

- JSON parsing
- JSON schema validation
- semantic workflow validation
- dependency validation
- DAG cycle detection

Execution begins only after the workflow is successfully persisted.

## Immutable Workflow Definitions

Workflow DAGs are immutable after admission to simplify execution semantics and avoid runtime graph mutation complexity.

## Asynchronous Execution

Workflow execution is fully asynchronous.

The API gateway never blocks waiting for workflow completion.

## Explicit Compensation

Rollback semantics are implemented using explicit compensation jobs rather than distributed transactions.

---

# Technology Stack

## Core

- Modern C++20
- Boost.Asio
- Boost.Beast
- nlohmann/json
- nlohmann/json-schema
- Redis
- SQLite
- Docker

## Planned Extensions

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

## Current Status

FlowPilot is actively developed with core components implemented and unit-tested. Recent work focuses on asynchronous IO, coroutine-friendly database APIs, and improving test isolation.

Completed / in-progress items:

- [x] Workflow schema design
- [x] Async-friendly SQLite wrapper (`AsyncSQLiteDatabase`) — provides awaitable APIs and offloads blocking SQLite calls to a thread pool
- [x] Redis client refactor (`RedisDatabase`) — supports awaitable APIs and test-local construction to avoid global io_context lifetime issues
- [x] SQLite persistence core (`SQLiteDatabase`) with improved transactional handling
- [x] Unit tests for async DB APIs and workflow validation (see `tests/`) — async DB tests pass in current CI runs
- [x] Build refactor: project sources are packaged into `flow_pilot_lib` for faster incremental builds and reuse in tests
- [ ] Full integration tests with external Redis instance (recommendation: run a local Redis for integration tests)
- [ ] API gateway production hardening
- [ ] Worker execution engine and scheduler clustering

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
