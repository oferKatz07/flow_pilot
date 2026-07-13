
// flow_pilot_error_msgs.h - Possible error messages for FlowPilot validation

#pragma once

#include <string>
#include <string_view>

namespace flow_pilot {

namespace error_msgs {

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

} // namespace error_msg

enum class StatusCodes {
    INVALID_JSON_FORMAT,
    SCHEMA_VALIDATION_FAILED,
    CLIENT_NOT_FOUND,
    WORKFLOW_SIZE_EXCEEDED,
    JOB_COUNT_EXCEEDED,
    JOB_COUNT_ZERO,
    JOB_SIZE_EXCEEDED,
    MISSING_DEPENDENCY,
    DUPLICATE_DEPENDENCY,
    DUPLICATE_JOB_ID,
    CIRCULAR_DEPENDENCY,
    CLIENT_POLICY_VIOLATION,
    INTERNAL_DB_FAILURE,
    DUPLICATE_REQUEST,
    WORKFLOW_ID_EXISTS,
    RATE_LIMIT_EXCEEDED,
    CONCURRENT_WORKFLOW_LIMIT_EXCEEDED,
    WORKFLOW_ADMITTED
};

inline std::string_view status_code_to_string(StatusCodes status_code) {
    switch (status_code) {
        case StatusCodes::INVALID_JSON_FORMAT:
            return std::string(error_msgs::INVALID_JSON_FORMAT);
        case StatusCodes::SCHEMA_VALIDATION_FAILED:
            return std::string(error_msgs::SCHEMA_VALIDATION_FAILED);
        case StatusCodes::CLIENT_NOT_FOUND:
            return std::string(error_msgs::CLIENT_NOT_FOUND);
        case StatusCodes::WORKFLOW_SIZE_EXCEEDED:
            return std::string(error_msgs::WORKFLOW_SIZE_EXCEEDED);
        case StatusCodes::JOB_COUNT_EXCEEDED:
            return std::string(error_msgs::JOB_COUNT_EXCEEDED);
        case StatusCodes::JOB_COUNT_ZERO:
            return std::string(error_msgs::JOB_COUNT_ZERO);
        case StatusCodes::JOB_SIZE_EXCEEDED:
            return std::string(error_msgs::JOB_SIZE_EXCEEDED);
        case StatusCodes::MISSING_DEPENDENCY:
            return std::string(error_msgs::MISSING_DEPENDENCY);
        case StatusCodes::DUPLICATE_DEPENDENCY:
            return std::string(error_msgs::DUPLICATE_DEPENDENCY);
        case StatusCodes::DUPLICATE_JOB_ID:
            return std::string(error_msgs::DUPLICATE_JOB_ID);
        case StatusCodes::CIRCULAR_DEPENDENCY:
            return std::string(error_msgs::CIRCULAR_DEPENDENCY);
        case StatusCodes::CLIENT_POLICY_VIOLATION:
            return std::string(error_msgs::CLIENT_POLICY_VIOLATION);
        case StatusCodes::INTERNAL_DB_FAILURE:
            return std::string(error_msgs::INTERNAL_DB_FAILURE);
        case StatusCodes::DUPLICATE_REQUEST:
            return std::string(error_msgs::DUPLICATE_REQUEST);
        case StatusCodes::WORKFLOW_ID_EXISTS:
            return std::string(error_msgs::WORKFLOW_ID_EXISTS);
        case StatusCodes::RATE_LIMIT_EXCEEDED:
            return std::string(error_msgs::RATE_LIMIT_EXCEEDED);
        case StatusCodes::CONCURRENT_WORKFLOW_LIMIT_EXCEEDED:
            return std::string(error_msgs::CONCURRENT_WORKFLOW_LIMIT_EXCEEDED);
        case StatusCodes::WORKFLOW_ADMITTED:
            return std::string(error_msgs::WORKFLOW_ADMITTED);
        default:
            return "Unknown status code";
    }
    
    // Code should not reach this point
    return "";
}

} // namespace flow_pilot
