// database_models.h - Define the types and structures that access the persistence database tables

#pragma once

#include <cstdint>
#include <ctime>
#include <string>
#include <vector>

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
    ADMITTED = 2,
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
        case RequestStatus::ADMITTED: return "ADMITTED";
    }
    
    return "UNKNOWN";
}

enum class WorkflowStatus: uint8_t {
    ADMITTED = 0,
    READY = 1,
    RUNNING = 2,
    COMPLETED = 3,
    FAILED = 4,
    CANCELED = 5,
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
        case WorkflowStatus::READY: return "READY";
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

struct JobData {
    std::string job_uuid;
    std::string job_id;
    JobStatus status;
};

struct WorkflowJobList {
    std::string client_id;
    std::string workflow_id;
    int retry_count;
    // List of the workflow jobs
    std::vector<JobData> jobs;
};

} // namespace flow_pilot

