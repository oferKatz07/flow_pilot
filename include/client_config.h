
// client_config.h - Data structures for client information in FlowPilot

#pragma once

#include <string>

namespace flow_pilot {

struct RateLimitConfig {
    std::string plan_name;
    int max_concurrent_workflows;
    int max_requests;
    int window_sec;
};

struct PolicyPlan {
    std::string plan_name;
    // Workflow-structure policies
    int max_workflow_size_kb;
    int max_jobs_in_workflow;
    int max_job_size_bytes;
    // Workflow-execution policies
    int max_workflow_runtime_sec;
    int max_workflow_total_retries;
    int max_job_runtime_sec;
    int max_job_retries;
    int max_concurrent_jobs;
    int max_pending_jobs;
    // Data retention policies
    int workflow_retention_days;
    int request_retention_days;
    int payload_retention_days;
};

struct ClientData {
    std::string client_id;
    std::string rate_limit_config_plan_name;
    std::string policy_plan_name;
};

struct ClientConfig {
    std::string client_id;
    RateLimitConfig rate_limit_config;
    PolicyPlan policy_config;
};

} // namespace flow_pilot
