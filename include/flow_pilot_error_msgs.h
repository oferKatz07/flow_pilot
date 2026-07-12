
// flow_pilot_error_msgs.h - Possible error messages for FlowPilot validation

#pragma once

#include <string>

namespace flow_pilot::error_msgs {

// Validation Errors dtrings:
inline constexpr std::string_view  INVALID_JSON_FORMAT = "Invalid JSON format";
inline constexpr std::string_view  SCHEMA_VALIDATION_FAILED = "Schema validation failed";
inline constexpr std::string_view  CLIENT_NOT_FOUND = "Client not found";
inline constexpr std::string_view  WORKFLOW_SIZE_EXCEEDED = "Workflow size exceeds the maximum allowed by client's policy";
inline constexpr std::string_view  JOB_COUNT_EXCEEDED = "Number of jobs in workflow exceeds the maximum allowed by client's policy";
inline constexpr std::string_view  JOB_COUNT_ZERO = "Workflow must contain at least one job";
inline constexpr std::string_view  JOB_SIZE_EXCEEDED = "A job in the workflow exceeds the maximum allowed size by client's policy";
inline constexpr std::string_view  MISSING_DEPENDENCY = "A job has a missing dependency";
inline constexpr std::string_view  DUPLICATE_DEPENDENCY = "Duplicate dependency found for job";
inline constexpr std::string_view  DUPLICATE_JOB_ID = "Duplicate job ID found for job";
inline constexpr std::string_view  CIRCULAR_DEPENDENCY = "Circular dependency detected in workflow";
inline constexpr std::string_view  CLIENT_POLICY_VIOLATION = "Workflow violates client's policy constraints";
inline constexpr std::string_view  INTERNAL_DB_FAILURE = "Internal DB failure";
inline constexpr std::string_view  DUPLICATE_REQUEST = "Duplicate request";
inline constexpr std::string_view  WORKFLOW_ID_EXISTS = "Workflow ID already exists";
inline constexpr std::string_view  RATE_LIMIT_EXCEEDED = "Rate limit exceeded";
inline constexpr std::string_view  CONCURRENT_WORKFLOW_LIMIT_EXCEEDED = "Concurrent workflow limit exceeded";
inline constexpr std::string_view  WORKFLOW_ADMITTED = "Workflow was validated and admitted";

} // namespace flow_pilot::error_msg
