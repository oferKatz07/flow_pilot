
// workflow_service.h - Workflow submission and validation for FlowPilot

#pragma once

#include <string>

namespace flow_pilot {

// Validation Errors dtrings:
const std::string INVALID_JSON_FORMAT = "Invalid JSON format";
const std::string SCHEMA_VALIDATION_FAILED = "Schema validation failed";
const std::string CLIENT_NOT_FOUND = "Client not found";
const std::string WORKFLOW_SIZE_EXCEEDED = "Workflow size exceeds the maximum allowed by client's policy";
const std::string JOB_COUNT_EXCEEDED = "Number of jobs in workflow exceeds the maximum allowed by client's policy";
const std::string JOB_COUNT_ZERO = "Workflow must contain at least one job";
const std::string JOB_SIZE_EXCEEDED = "A job in the workflow exceeds the maximum allowed size by client's policy";
const std::string MISSING_DEPENDENCY = "Missing dependency detected for job";
const std::string DUPLICATE_DEPENDENCY = "Duplicate dependency found for job";
const std::string DUPLICATE_JOB_ID = "Duplicate job ID found for job";
const std::string CIRCULAR_DEPENDENCY = "Circular dependency detected in workflow";
const std::string CLIENT_POLICY_VIOLATION = "Workflow violates client's policy constraints";
const std::string INTERNAL_DB_FAILURE = "Internal DB failure";
const std::string DUPLICATE_REQUEST = "Duplicate request";
const std::string WORKFLOW_ID_EXISTS = "Workflow ID already exists";
const std::string RATE_LIMIT_EXCEEDED = "Rate limit exceeded";
const std::string CONCURRENT_WORKFLOW_LIMIT_EXCEEDED = "Concurrent workflow limit exceeded";

} // namespace flow_pilot
