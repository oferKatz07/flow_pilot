// db_interface.h - Database interface for FlowPilot

#pragma once

#include <string>
#include <vector>
#include <unordered_map>

#include "client_config.h"
#include "flow_pilot_error_msgs.h"

namespace flow_pilot {

enum class ClientStatus : uint8_t {
    ACTIVE = 0,
    SUSPENDED = 1,
    DISABLED = 2,
    PENDING = 3,
    DELETED = 4,
    // UNKNOWN must remain the last enumerator.
    // Values >= UNKNOWN are considered invalid.
    UNKNOWN
};

// Convertion from int to ClientStatus
constexpr ClientStatus from_int_to_ClientStatus(int val) noexcept {
    if (val >= 0 && val < static_cast<int>(ClientStatus::UNKNOWN)) {
        return static_cast<ClientStatus>(val);
    }

    return ClientStatus::UNKNOWN;
}

constexpr uint8_t to_int(ClientStatus status) noexcept
{
    return static_cast<uint8_t>(status);
}

inline ClientStatus client_status_from_string(const std::string& str) noexcept {
    if (str == "ACTIVE") return ClientStatus::ACTIVE;
    if (str == "SUSPENDED") return ClientStatus::SUSPENDED;
    if (str == "DISABLED") return ClientStatus::DISABLED;
    if (str == "PENDING") return ClientStatus::PENDING;
    if (str == "DELETED") return ClientStatus::DELETED;

    return ClientStatus::UNKNOWN;
}

inline std::string_view to_string(ClientStatus status) noexcept {
    switch(status) {
        case ClientStatus::ACTIVE: return "ACTIVE";
        case ClientStatus::SUSPENDED: return "SUSPENDED";
        case ClientStatus::DISABLED: return "DISABLED";
        case ClientStatus::PENDING: return "PENDING";
        case ClientStatus::DELETED: return "DELETED";
    }
    
    return "UNKNOWN";
}


enum class RequestStatus : uint8_t {
    RECEIVED = 0,
    REJECTED = 1,
    COMPLETED = 2,
    // UNKNOWN must remain the last enumerator.
    // Values >= UNKNOWN are considered invalid.
    UNKNOWN
};

// Convertion from int to RequestStatus
constexpr RequestStatus from_int_to_RequestStatus(int val) noexcept {
    if (val >= 0 && val < static_cast<int>(RequestStatus::UNKNOWN)) {
        return static_cast<RequestStatus>(val);
    }

    return RequestStatus::UNKNOWN;
}

constexpr uint8_t to_int(RequestStatus status) noexcept
{
    return static_cast<uint8_t>(status);
}

inline std::string_view to_string(RequestStatus status) noexcept {
    switch(status) {
        case RequestStatus::RECEIVED: return "RECEIVED"; 
        case RequestStatus::REJECTED: return "REJECTED";
        case RequestStatus::COMPLETED: return "COMPLETED";
    }
    
    return "UNKNOWN";
}

enum class WorkflowStatus: uint8_t {
    ADMITTED = 0,
    RUNNING = 1,
    COMPLETED = 2,
    FAILED = 3,
    CANCELED = 4,
    // UNKNOWN must remain the last enumerator.
    // Values >= UNKNOWN are considered invalid.
    UNKNOWN
};

// Convertion from int to WorkflowStatus
constexpr WorkflowStatus from_int_to_WorkflowStatus(int val) noexcept {
    if (val >= 0 && val < static_cast<int>(WorkflowStatus::UNKNOWN)) {
        return static_cast<WorkflowStatus>(val);
    }

    return WorkflowStatus::UNKNOWN;
}

constexpr uint8_t to_int(WorkflowStatus status) noexcept
{
    return static_cast<uint8_t>(status);
}

// Conversion functions for WorkflowStatus
inline std::string_view to_string(WorkflowStatus status) noexcept{
    switch(status) {
        case WorkflowStatus::ADMITTED: return "ADMITTED";
        case WorkflowStatus::RUNNING: return "RUNNING";
        case WorkflowStatus::COMPLETED: return "COMPLETED";
        case WorkflowStatus::FAILED: return "FAILED";
        case WorkflowStatus::CANCELED: return "CANCELED";
        default: return "UNKNOWN";
    }

    return "UNKNOWN";
}

enum class JobStatus : uint8_t {
    PENDING = 0,
    READY = 1,
    RUNNING = 2,
    COMPLETED = 3,
    FAILED = 4,
    CANCELED = 5,
    // UNKNOWN must remain the last enumerator.
    // Values >= UNKNOWN are considered invalid.
    UNKNOWN
};

// Convertion from int to JobStatus
constexpr JobStatus from_int_to_JobStatus(int val) noexcept {
    if (val >= 0 && val < static_cast<int>(JobStatus::UNKNOWN)) {
        return static_cast<JobStatus>(val);
    }

    return JobStatus::UNKNOWN;
}

// Conversion functions for JobStatus
constexpr uint8_t to_int(JobStatus status) noexcept
{
    return static_cast<uint8_t>(status);
}

inline std::string_view to_string(JobStatus status) noexcept{
    switch(status) {
        case JobStatus::PENDING: return "PENDING";
        case JobStatus::READY: return "READY";
        case JobStatus::RUNNING: return "RUNNING";
        case JobStatus::COMPLETED: return "COMPLETED";
        case JobStatus::FAILED: return "FAILED";
        case JobStatus::CANCELED: return "CANCELED";
        default: return "UNKNOWN";
    }
}

template <typename EnumT>
inline std::string serialize_status(EnumT status)
{
    return std::string(to_string(status));
}

template <typename EnumT, typename FromIntFn>
inline bool parse_status(const std::string& value, EnumT& status, FromIntFn from_int_fn)
{
    try {
        const int parsed = std::stoi(value);
        status = from_int_fn(parsed);
        return status != EnumT::UNKNOWN;
    } catch (...) {
        status = EnumT::UNKNOWN;
        return false;
    }
}

struct RequestData {
    std::string client_id;
    std::string request_id;
    std::string workflow_id;
    int workflow_payload_size_bytes = 0;
    std::string operation;
    RequestStatus status;
    std::string reject_reason;
};

struct WorkflowfullData {
    RequestData info;
    WorkflowStatus status;
    std::string workflow_type;
    std::string workflow_version;
    int total_jobs;
    std::time_t received_at;
    std::time_t started_at;
    std::time_t completed_at;
};

struct WorkflowJob {
    std::string job_uuid;
    std::string client_id;
    std::string workflow_id;
    std::string job_id;
    JobStatus status;
    int retry_num;
    std::time_t submitted_at;
    std::time_t started_at;
    std::time_t updated_at;
};

struct workflow_payload_data {
    std::string client_id;
    std::string workflow_id;
    std::string payload;
};

class IDatabase {
public:
    virtual ~IDatabase() = default;

    virtual bool get_rate_limit_plans(
        std::unordered_map<std::string, RateLimitConfig> &rate_limit_plans
    )  const = 0;

    virtual bool get_rate_limit_plan(
        RateLimitConfig& rate_limit_plan
    ) const = 0;

    virtual bool upsert_rate_limit_plan(
        const RateLimitConfig& rate_limit_plan
    ) = 0;

    virtual bool get_policy_plans(
        std::unordered_map<std::string, PolicyPlan> &policy_plans
    )  const = 0;

    virtual bool get_policy_plan(
        PolicyPlan& plan_policy
    ) const = 0;

    // Insert or replace a policy plan
    virtual bool upsert_policy_plan(
        const PolicyPlan& policy_plan
    ) = 0;

    virtual bool get_user_config(
        ClientData& user_data
    ) const = 0;

    virtual bool get_all_users(
        std::unordered_map<std::string, ClientData> &clients
    ) const = 0;

    virtual bool upsert_user_config(
        const std::string& user_id,
        const std::string& rate_limit_plan_name,
        const std::string& policy_plan_name
    ) = 0;

    // Add a new received rejected request
    virtual bool add_request(
        const RequestData& request_data,
        StatusCodes& error_status
    ) = 0;

    /// Get all workflows for the requested client from the DB. This is used for auditing and debugging purposes.
    virtual bool get_all_requests_for_client(const std::string& client_id, std::vector<RequestData>& workflows) const = 0;

    // Add a new received request and perform validations
    virtual bool add_request(
        const RequestData& request_data,
        const std::string& workflow_payload,
        const ClientConfig& client_config,
        StatusCodes& error_status
    ) = 0;

    // Update the request status in the DB. This is used for durability and auditing of request processing.
    virtual bool update_request_status(const RequestData& request_data) = 0;

    // Add a new workflow data to the DB. This is used for durability and auditing of workflow submissions.
    virtual bool add_workflow(
        const WorkflowfullData& workflow_data,
        StatusCodes& error_status
    ) = 0;

    // Update the workflow status in the DB. This is used for durability and auditing of workflow execution.
    virtual bool update_workflow_status(const std::string& client_id, const std::string& workflow_id, const WorkflowStatus status) = 0;

    /// For recovery get all active workflows (with status RECEIVED, ADMITTED, RUNNING) from the DB.
    virtual bool get_all_active_workflows(std::vector<WorkflowfullData>& workflows) const = 0;

    /// Get all workflows for the requested client from the DB. This is used for auditing and debugging purposes.
    virtual bool get_all_workflows_for_client(const std::string& client_id, std::vector<WorkflowfullData>& workflows) const = 0;

private:
    /// Create required tables and indexes if they do not exist.
    virtual bool create_schema() = 0;
    virtual bool get_client_active_workflows_count(const std::string& client_id, int& active_workflows) = 0;
};

} // namespace flow_pilot