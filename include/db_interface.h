// db_interface.h - Database interface for FlowPilot

#pragma once

#include <string>
#include <vector>
#include <unordered_map>

#include "client_data.h"
#include <stdexcept>


namespace flow_pilot {

enum class ClientStatus {
    ACTIVE,
    SUSPENDED,
    DISABLED,
    PENDING,
    DELETED
};

// Conversion functions for ClientStatus
inline bool to_string(ClientStatus status, std::string& str) {
    switch(status) {
        case ClientStatus::ACTIVE: str = "ACTIVE"; return true;
        case ClientStatus::SUSPENDED: str = "SUSPENDED"; return true;
        case ClientStatus::DISABLED: str = "DISABLED"; return true;
        case ClientStatus::PENDING: str = "PENDING"; return true;
        case ClientStatus::DELETED: str = "DELETED"; return true;
        default: return false;
    }
}

inline ClientStatus client_status_from_string(const std::string& str) {
    if (str == "ACTIVE") return ClientStatus::ACTIVE;
    if (str == "SUSPENDED") return ClientStatus::SUSPENDED;
    if (str == "DISABLED") return ClientStatus::DISABLED;
    if (str == "PENDING") return ClientStatus::PENDING;
    if (str == "DELETED") return ClientStatus::DELETED;
    throw std::invalid_argument("Unknown ClientStatus: " + str);
}


enum class RequestStatus {
    RECEIVED,
    REJECTED,
    COMPLETED,
};

inline bool to_string(RequestStatus status, std::string& str) {
    switch(status) {
        case RequestStatus::RECEIVED: str = "RECEIVED"; return true;
        case RequestStatus::REJECTED: str = "REJECTED"; return true;
        case RequestStatus::COMPLETED: str = "COMPLETED"; return true;
        default: return false;
    }
}

inline bool request_status_from_string(const std::string& str, RequestStatus& request_status) {
    if (str == "RECEIVED") {
        request_status = RequestStatus::RECEIVED;
        return true;
    }
    if (str == "REJECTED") {
        request_status = RequestStatus::REJECTED;
        return true;
    }
    if (str == "COMPLETED") {
        request_status = RequestStatus::COMPLETED;
        return true;
    }
    return false;
}


enum class WorkflowStatus {
    ADMITTED,
    RUNNING,
    COMPLETED,
    FAILED,
    CANCELED
};

// Conversion functions for WorkflowStatus
inline bool to_string(WorkflowStatus status, std::string& str) {
    switch(status) {
        case WorkflowStatus::ADMITTED: str = "ADMITTED"; return true;
        case WorkflowStatus::RUNNING: str = "RUNNING"; return true;
        case WorkflowStatus::COMPLETED: str = "COMPLETED"; return true;
        case WorkflowStatus::FAILED: str = "FAILED"; return true;
        case WorkflowStatus::CANCELED: str = "CANCELED"; return true;
        default: return false;
    }
}

inline bool workflow_status_from_string(const std::string& str, WorkflowStatus& workflow_status) {
    if (str == "ADMITTED") {
        workflow_status = WorkflowStatus::ADMITTED;
        return true;
    }
    if (str == "RUNNING") {
        workflow_status = WorkflowStatus::RUNNING;
        return true;
    }
    if (str == "COMPLETED") {
        workflow_status = WorkflowStatus::COMPLETED;
        return true;
    }
    if (str == "FAILED") {
        workflow_status = WorkflowStatus::FAILED;
        return true;
    }
    if (str == "CANCELED") {
        workflow_status = WorkflowStatus::CANCELED;
        return true;
    }
    return false;
}

enum class JobStatus {
    PENDING,
    RUNNING,
    COMPLETED,
    FAILED,
    CANCELED
};

// Conversion functions for JobStatus
inline bool to_string(JobStatus status, std::string& str) {
    switch(status) {
        case JobStatus::PENDING: str = "PENDING"; return true;
        case JobStatus::RUNNING: str = "RUNNING"; return true;
        case JobStatus::COMPLETED: str = "COMPLETED"; return true;
        case JobStatus::FAILED: str = "FAILED"; return true;
        case JobStatus::CANCELED: str = "CANCELED"; return true;
        default: return false;
    }
}

inline bool job_status_from_string(const std::string& str, JobStatus& job_status) {
    if (str == "PENDING") {
        job_status = JobStatus::PENDING;
        return true;
    }
    if (str == "RUNNING") {
        job_status = JobStatus::RUNNING;
        return true;
    }
    if (str == "COMPLETED") {
        job_status = JobStatus::COMPLETED;
        return true;
    }
    if (str == "FAILED") {
        job_status = JobStatus::FAILED;
        return true;
    }
    if (str == "CANCELED") {
        job_status = JobStatus::CANCELED;
        return true;
    }
    return false;
}

struct RequestData {
    std::string client_id;
    std::string request_id;
    std::string workflow_id;
    int workflow_payload_size_bytes = 0;
    std::string operation;
    std::string status;
    std::string reject_reason;
};

struct WorkflowfullData {
    RequestData info;
    std::string status;
    std::string workflow_type;
    std::string workflow_version;
    int total_jobs;
    int jobs_running;
    int jobs_completed;
    int received_at;
    int started_at;
    int updated_at;
    int last_state_change_at;
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
        std::string& error_message
    ) = 0;

    /// Get all workflows for the requested client from the DB. This is used for auditing and debugging purposes.
    virtual bool get_all_requests_for_client(const std::string& client_id, std::vector<RequestData>& workflows) const = 0;

    // Add a new received request and perform validations
    virtual bool add_request(
        const RequestData& request_data,
        const std::string& workflow_payload,
        const ClientConfig& client_config,
        std::string& error_message
    ) = 0;

    // Update the request status in the DB. This is used for durability and auditing of request processing.
    virtual bool update_request_status(const RequestData& request_data) = 0;

    // Add a new workflow data to the DB. This is used for durability and auditing of workflow submissions.
    virtual bool add_workflow(
        const WorkflowfullData& workflow_data,
        std::string& error_message
    ) = 0;

    // Update the workflow status in the DB. This is used for durability and auditing of workflow execution.
    virtual bool update_workflow_status(const std::string& client_id, const std::string& workflow_id, const std::string& status) = 0;

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