# FlowPilot Architecture

## Overview

FlowPilot is a C++20 workflow orchestration service designed around asynchronous I/O, coroutine-based request handling, durable persistence, and admission control.

The current implementation focuses on workflow admission, validation, duplicate prevention, rate limiting, and persistence.

## Architecture Goals

FlowPilot is designed around several architectural principles:

- High throughput using asynchronous I/O
- Durable workflow persistence
- Idempotent workflow admission
- Strict separation of transport and business logic
- Fast rejection of invalid requests
- Stateless HTTP layer
- Horizontally scalable admission service

## Design Principles

1. Asynchronous by Default - Request processing should never block I/O threads.
2. Durability Before Execution - Accepted workflows are persisted before they can be scheduled.
3. Admission Is Atomic - Idempotency, concurrency limits, and rate limiting are enforced as a single logical operation.
4. Redis for Coordination, SQLite for Truth - Redis accelerates coordination; SQLite remains the authoritative source of state.
5. Dependency Injection and Testability - Core services are accessed through interfaces to enable isolated unit testing.
6. Separation of Concerns - Admission, persistence, scheduling, and execution remain independent subsystems.
7. Scalability Through Stateless Services - The architecture is designed so multiple FlowPilot instances can eventually share Redis and 
   persistent storage without fundamental redesign.


### Current Architecture

```text
Client
  |
  v
HTTP Server (Boost.Beast)
  |
  v
Request Handler
  |
  v
Workflow Service
  |
  +--> JSON Parsing
  |
  +--> JSON Schema Validation
  |
  +--> Client Validation
  |
  +--> Redis Admission Control
  |
  +--> Persist Request
  |
  +--> Policy Validation
  |
  +--> Semantic Validation
  |
  +--> Dependency Validation
  |
  +--> DAG Validation
  |
  +--> Persist Workflow
  |
  +--> Execution Initialization
  |
  +--> Redis Status Update
  |
  v
HTTP Response
```
Redis is intentionally placed before durable persistence.
This provides:
- Constant-time duplicate detection
- Atomic request admission
- Fast rate limiting
- Reduced database load
- Early rejection of malformed duplicate requests

| Redis                 | SQLite            |
| --------------------- | ----------------- |
| Fast admission        | Durable storage   |
| Duplicate detection   | Source of truth   |
| Rate limiting         | Audit history     |
| Active workflow state | Workflow metadata |
| Temporary             | Persistent        |



                    +------------------+
                    |      Client      |
                    +------------------+
                             |
                             v
                 +-----------------------+
                 |   Boost.Beast Server  |
                 +-----------------------+
                             |
                             v
                 +-----------------------+
                 |   Request Handler     |
                 +-----------------------+
                             |
                             v
                 +-----------------------+
                 |   Workflow Service    |
                 +-----------------------+
                     |             |
          Validation |             | Persistence
                     |             |
        +------------+             +-------------+
        |                                      |
        v                                      v
+-------------------+                +------------------+
|       Redis       |                |      SQLite      |
|-------------------|                |------------------|
| Rate limiting     |                | Workflows        |
| Duplicate check   |                | Requests         |
| Admission control |                | Jobs             |
| Active state      |                | Dependencies     |
+-------------------+                +------------------+

## Idempotent Request Processing

Workflow submissions are designed to be idempotent.

Each request contains a client identifier and request identifier.

Redis atomically rejects duplicate requests before validation begins.

This prevents:

- duplicate workflow creation
- repeated validation
- accidental client retries

## Core Components

### HTTP Server

The HTTP server is implemented using Boost.Beast and Boost.Asio coroutines.

Responsibilities:

* Accept incoming REST requests.
* Parse HTTP requests.
* Route requests to handlers.
* Return HTTP responses.

The server is fully asynchronous and designed to support large numbers of concurrent connections without blocking worker threads.

### Request Handlers

Request handlers perform:

* Request routing
* Request parsing
* Context creation
* Delegation to WorkflowService

Business logic is intentionally kept outside the handlers.

### Workflow Service

WorkflowService contains the workflow admission pipeline and business validation logic.

Responsibilities:

* JSON schema validation
* Client lookup and validation
* Policy enforcement
* Admission control
* Semantic validation
* Dependency validation
* Persistence

WorkflowService acts as the orchestration layer between Redis and SQLite.

## Admission Pipeline

Workflow submission follows the sequence below.

### 1. JSON Parsing

Incoming workflow requests are parsed into internal workflow models.

### 2. Schema Validation

The request is validated against the workflow JSON schema.

Validation includes:

* Required fields
* Field types
* Structural constraints

### 3. Client Validation

The client identifier is validated against configured client metadata.

### 4. Policy Validation

Client-specific workflow policies are enforced.

Examples:

* Maximum workflow size
* Maximum job count
* Maximum dependency count

### 5. Redis Admission Control

Redis is used as the first admission gate.

Responsibilities:

* Duplicate request detection
* Request rate limiting
* Concurrent workflow limits
* Fast rejection of duplicate submissions

Admission decisions are implemented using Redis Lua scripts to guarantee atomicity.

### 6. Semantic Validation

Workflow-level validation is performed.

Examples:

* Duplicate job identifiers
* Invalid job definitions
* Dependency consistency

### 7. DAG Validation

Workflow dependencies are validated using Kahn's algorithm.

Validation ensures:

* No cycles exist
* All referenced jobs exist
* Dependency graph is executable

### 8. SQLite Persistence

After successful validation the workflow is persisted.

SQLite is the system of record.

Persisted information includes:

* Workflow metadata
* Workflow requests
* Job definitions
* Dependency relationships

### 9. Redis Status Update

After persistence succeeds, Redis is updated with workflow admission state information.

## Redis Design

Redis serves two independent purposes.

### Admission Control

Redis maintains:

* Request rate limits
* Concurrent workflow counters
* Admission decisions

### Duplicate Request Protection

Redis stores recently processed request identifiers.

This prevents:

* Duplicate submissions
* Reprocessing malformed requests
* Excessive validation load

Rejected requests may remain registered in Redis for a configurable retention period to preserve deduplication guarantees.

## SQLite Design

SQLite is the durable persistence layer.

Current design goals:

* Simplicity
* Reliability
* Transactional consistency

SQLite remains the authoritative source of workflow state.

Redis should be considered a transient coordination and caching layer.

## Async Architecture

### Network Layer

Boost.Asio coroutines are used throughout the HTTP stack.

```cpp
co_await
use_awaitable
awaitable<T>
```

are used as the primary asynchronous abstractions.

### SQLite Integration

SQLite is synchronous by nature.

To integrate with the coroutine model:

```text
SQLiteDatabase
        |
        v
AsyncSQLiteDatabase
        |
        v
Boost.Asio Thread Pool
```

Blocking database operations execute on a dedicated thread pool while exposing coroutine-friendly APIs.

### Redis Integration

Redis operations are exposed as awaitable APIs and integrate directly into the coroutine execution flow.

## Current Status

Implemented:

* HTTP server
* Request routing
* Workflow admission
* JSON schema validation
* Client validation
* Policy validation
* Redis admission control
* Duplicate request protection
* DAG validation
* SQLite persistence
* Coroutine-based database wrappers

Planned:

* Workflow scheduler
* Job execution engine
* Worker coordination
* Workflow state transitions
* Recovery processing
* Metrics and observability

## Architectural Principles

* SQLite is the source of truth.
* Redis is used for fast coordination and admission decisions.
* Validation occurs before persistence.
* Duplicate requests are rejected as early as possible.
* All network-facing operations are asynchronous.
* Blocking operations are isolated behind async wrappers.
* Business logic remains outside HTTP handlers.
