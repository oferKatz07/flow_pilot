// db_interface.h - Database interface for FlowPilot

#pragma once

#include <string>
#include <vector>
#include <unordered_map>

#include "database_models.h"
#include "client_config.h"
#include "flow_pilot_error_msgs.h"

namespace flow_pilot {

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

    // Add a new received request and perform validations
    virtual bool add_request(
        const RequestData& request_data,
        const std::string& workflow_payload,
        const ClientConfig& client_config,
        StatusCodes& error_status
    ) = 0;

    // Update the request status in the DB. This is used for durability and auditing of request processing.
    virtual bool update_request_status(const RequestData& request_data) = 0;

    /// Get all workflows for the requested client from the DB. This is used for auditing and debugging purposes.
    virtual bool get_all_requests_for_client(const std::string& client_id, std::vector<RequestData>& workflows) const = 0;

    // Add a new workflow data to the DB. This is used for durability and auditing of workflow submissions.
    virtual bool add_workflow(
        const WorkflowData& workflow_data,
        StatusCodes& error_status
    ) = 0;

    // Update the workflow status in the DB. This is used for durability and auditing of workflow execution.
    virtual bool update_workflow_status(const std::string& client_id, const std::string& workflow_id, const WorkflowStatus status) = 0;

    /// For recovery get all active workflows (with status RECEIVED, ADMITTED, RUNNING) from the DB.
    virtual bool get_all_active_workflows(std::vector<WorkflowData>& workflows) const = 0;

    /// Get all workflows for the requested client from the DB. This is used for auditing and debugging purposes.
    virtual bool get_all_workflows_for_client(const std::string& client_id, std::vector<WorkflowData>& workflows) const = 0;
    
    // Set all pending workflow jobs status (job with status PENDING and READY) to FAILED
    virtual bool fail_workflow(const std::string& client_id, const std::string& workflow_id) = 0;

    // Get all jobs for a workflow from the DB
    virtual bool get_all_jobs_for_workflow(const std::string& client_id, const std::string& workflow_id, 
                                           std::vector<WorkflowJob>& jobs) const = 0;
    
    virtual bool add_workflow_jobs(
        const WorkflowJobList& job_list,
        StatusCodes& error_status
    ) = 0;

    // Update the workflow ready jobs in the DB
    virtual bool update_ready_jobs(const std::string& client_id, const std::string& workflow_id, 
                                   const std::vector<std::string>& queued_jobs,
                                   const std::vector<std::string>& ready_jobs) = 0;

    // Update the job status for a specific job in a workflow
    virtual bool update_job_status(const std::string& client_id, const std::string& workflow_id, const std::string& job_id, 
                                   const JobStatus status) = 0;

    // Get a specific job data for a workflow from the DB
    virtual bool get_job_data(const std::string& client_id, const std::string& workflow_id, const std::string& job_id, 
                              WorkflowJob& job_data) const = 0;
    

private:
    /// Create required tables and indexes if they do not exist.
    virtual bool create_schema() = 0;
    virtual bool get_client_active_workflows_count(const std::string& client_id, int& active_workflows) = 0;
    virtual bool fail_all_pending_jobs(const std::string& client_id, const std::string& workflow_id) = 0;
};

} // namespace flow_pilot