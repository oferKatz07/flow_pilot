
// workflow_service.cpp - Implementation of WorkflowService for FlowPilot

#include <fstream>
#include <exception>
#include <queue>

#include "flow_pilot_error_msgs.h"
#include "logger.h"
#include "client_data.h"
#include "config.h"
#include "client_data_manager.h"
#include "redis_db_async.h"
#include "db_factory.h"
#include "workflow_service.h"

namespace flow_pilot {
using namespace boost::asio;

WorkflowService::WorkflowService(const std::string& schema_path)
{
    std::ifstream schema_file(schema_path);
    if (!schema_file.is_open()) {
        throw std::runtime_error("Unable to open schema file: " + schema_path);
    }

    json schema;
    try {
        schema_file >> schema;
    }
    catch (const std::exception& e) {
        throw std::runtime_error(std::string("Failed to parse schema file: ") + e.what());
    }

    try {
        validator_.set_root_schema(schema);
    }
    catch (const std::exception& e) {
        throw std::runtime_error(std::string("Failed to initialize JSON schema validator: ") + e.what());
    }
}

 awaitable<ValidationResult> WorkflowService::submit_workflow(const std::string& body)
{
    json workflow_data;
    RequestData workflow_request_info;
    ValidationResult result = {
        true,
        error_msg::WORKFLOW_ADMITTED,  // "Workflow was validated and admitted"
        std::string(error_msg::WORKFLOW_ADMITTED)  // No errors
    };

    if (!parse_request(body, workflow_data, workflow_request_info, result)) {
        co_return result;
    }

    // Get client config from DB for policy and rate limit validation
    // If client is not found, the assumption is that the client is not registered and the request is rejected
    ClientConfig client_config;
    if (!get_client_config_data(workflow_request_info.client_id, client_config, result)) {
        co_return result;
    }

    std::string rejection_reason;
    // Check if the client can accept the request based on rate limits and active workflows
    bool res = co_await admit_request(workflow_request_info, client_config.rate_limit_config, rejection_reason);
    if (!res) {
        co_return co_await handle_request_rejection(result, workflow_request_info, rejection_reason, false);
    }

    // Persist the request in the DB for auditing purposes
    res = co_await persist_request(workflow_request_info, body, client_config, rejection_reason);
    if (!res) {
        co_return co_await handle_request_rejection(result, workflow_request_info, rejection_reason);
    }

    // Validate received workflow against client's policy plan and semantic correctness
    std::unordered_map<std::string, DagData> jobs_map;
    WorkflowfullData workflow_info;
    res = validate_workflow(workflow_data, client_config.policy_config, jobs_map, workflow_info, rejection_reason);
    if (!res) {
        co_return co_await handle_request_rejection(result, workflow_request_info, rejection_reason);
    }
    workflow_info.info = std::move(workflow_request_info);

    res = co_await persist_workflow(workflow_info, rejection_reason);
    if (!res) {
        co_return co_await handle_request_rejection(result, workflow_request_info, rejection_reason);
    }

    co_return co_await handle_request_accepted(result, workflow_request_info);
}

bool WorkflowService::parse_request(const std::string& body, json& workflow_data, 
                                    RequestData& request_info, ValidationResult& result) {
    try {
        workflow_data = json::parse(body);
    }
    catch (const nlohmann::json::parse_error& e) {
        result.valid = false;
        result.status_str = error_msg::INVALID_JSON_FORMAT;
        result.errors_msg = std::string("JSON parse error: ") + e.what();

        return false;
    }

    try {
        validator_.validate(workflow_data);
    }
    catch (const std::exception& e) {
        result.valid = false;
        result.status_str = error_msg::SCHEMA_VALIDATION_FAILED;
        result.errors_msg = std::string("Schema validation error: ") + e.what();

        return false;
    }

    request_info.client_id = workflow_data.value("client_id", std::string());
    request_info.request_id = workflow_data.value("request_id", std::string());
    request_info.workflow_id = workflow_data.value("workflow_id", std::string());
    request_info.workflow_payload_size_bytes = static_cast<int>(body.size());
    request_info.operation = "CREATE";
    to_string(RequestStatus::RECEIVED, request_info.status);

    return true;
}

bool WorkflowService::get_client_config_data(const std::string& client_id, ClientConfig& client_config, ValidationResult& result) {
    if (!get_client_config(client_id, client_config)) {
        result.valid = false;
        result.status_str = error_msg::CLIENT_NOT_FOUND;
        result.errors_msg = std::string(error_msg::CLIENT_NOT_FOUND);
        return false;
    }
    
    return true;
}

boost::asio::awaitable<bool> WorkflowService::admit_request(const RequestData& request_info, 
                                                            const RateLimitConfig& rate_limit_config, 
                                                            std::string& rejection_reason) {
    auto redis_db = RedisDatabaseAsync::get_instance();
    co_return co_await redis_db->admit_request_async(request_info.client_id,
                                                      request_info.request_id,
                                                      request_info.workflow_id,  
                                                      rate_limit_config.max_concurrent_workflows,  
                                                      rate_limit_config.max_requests,  
                                                      rate_limit_config.window_sec,  
                                                      rejection_reason);  
}

boost::asio::awaitable<bool> WorkflowService::persist_request(const RequestData& request_info, 
                                                              const std::string& body, 
                                                              const ClientConfig& client_config, 
                                                              std::string& rejection_reason) {
    co_return co_await DBFactory::get_database().add_request_async(request_info, body, 
                                                                   client_config, rejection_reason);
}

bool WorkflowService::validate_workflow(const json& workflow_data, const PolicyPlan& policy_config, 
                                        std::unordered_map<std::string, DagData>& jobs_map, 
                                        WorkflowfullData& workflow_info,
                                        std::string& rejection_reason) {
    if (!validate_admission_client_workflow_policy(workflow_data, policy_config, rejection_reason)) {
        return false;
    }

    if (!validate_semantic(workflow_data, jobs_map, rejection_reason)) {
        return false;
    }

    if (!validate_dependencies(workflow_data, jobs_map, rejection_reason)) {
        return false;
    }

    // Extract the workflow info from the received data
    workflow_info.workflow_type = workflow_data.value("workflow_type", std::string());
    workflow_info.total_jobs = static_cast<int>(workflow_data["jobs"].size());
    workflow_info.workflow_version = Config::get_config().workflow().version;
    to_string(WorkflowStatus::ADMITTED, workflow_info.status);

    return true;
}

boost::asio::awaitable<bool> WorkflowService::persist_workflow(const WorkflowfullData& workflow_info, std::string& rejection_reason) {
    co_return co_await DBFactory::get_database().add_workflow_async(workflow_info, rejection_reason);
}

bool WorkflowService::validate_admission_client_workflow_policy(const json& workflow_data,  
                                                                const PolicyPlan& policy_config, 
                                                                std::string& rejection_reason) {
    std::string error_msg;                                                        
    int job_count = workflow_data["jobs"].size();
    if (job_count > policy_config.max_jobs_in_workflow) {
        error_msg = "Workflow has " + std::to_string(job_count) + " jobs, but the maximum allowed is " + std::to_string(policy_config.max_jobs_in_workflow);
        Logger::get_instance()->error(error_msg);
        rejection_reason = error_msg::JOB_COUNT_EXCEEDED;
        return false;
    }

    if (job_count == 0) {
        error_msg = "Workflow has no jobs";
        Logger::get_instance()->error(error_msg);
        rejection_reason = error_msg::JOB_COUNT_ZERO;
        return false;
    }

    for (const auto& job : workflow_data["jobs"]) {
        int job_size_bytes = job.dump().size();
        if (job_size_bytes > policy_config.max_job_size_bytes) {
            error_msg = "Job '" + job["job_id"].get<std::string>() + "' size is " + std::to_string(job_size_bytes) + " bytes, but the maximum allowed is " + std::to_string(policy_config.max_job_size_bytes) + " bytes";
            Logger::get_instance()->error(error_msg);
            rejection_reason = error_msg::JOB_SIZE_EXCEEDED;
            return false;
        }
    }

    return true;
}

bool WorkflowService::validate_semantic(const json& data, 
                                        std::unordered_map<std::string, DagData>& jobs_map, 
                                        std::string& rejection_reason)
{
    std::string error_msg;
    for (const auto& job : data["jobs"]) {
        DagData job_data;
        job_data.job_id = job["job_id"].get<std::string>();
        // Check for duplicate jobs in the dependency list of the same job
        json dependencies = job.value("depends_on", json::array());
        for (const auto& dep : dependencies) {
            auto res = job_data.dependencies.insert(dep.get<std::string>());
            if (!res.second) {
                error_msg = "Duplicate dependency found for job " + job_data.job_id;
                Logger::get_instance()->error(error_msg);
                rejection_reason = error_msg::DUPLICATE_DEPENDENCY;
                return false;
            }
        }

        if (jobs_map.find(job_data.job_id) != jobs_map.end()) {
            // Duplicate job ID found
            error_msg = "Duplicate job ID found for job '" + job_data.job_id + "'";
            Logger::get_instance()->error(error_msg);
            rejection_reason = error_msg::DUPLICATE_JOB_ID;
            return false;
        }

        jobs_map[job_data.job_id] = job_data;
    }

    return true;
}

bool WorkflowService::validate_dependencies(const json& data, 
                                            std::unordered_map<std::string, DagData>& jobs_map, 
                                            std::string& rejection_reason)
{
     std::string error_msg;
    // Preparing the data structures for creating a DAG inorder to apply the kahn’s algorithm
    std::queue<std::string> dag_queue;
    for (auto& job : jobs_map) {
        // Fill incoming_edges and out_deg_dependencies for each job
        for (const std::string& dep_name : job.second.dependencies) {
            job.second.incoming_edges.insert(dep_name);
            // Check if the dependency exists in the jobs map
            if (jobs_map.find(dep_name) == jobs_map.end()) {
                error_msg ="Dependency '" + dep_name + "' is not defined as a job required by job '" + job.first + "'";
                Logger::get_instance()->error(error_msg);
                rejection_reason = error_msg::MISSING_DEPENDENCY;
                return false;
            }

            jobs_map[dep_name].outgoing_edges.insert(job.first);
        }

        if (job.second.incoming_edges.empty()) {
            dag_queue.push(job.first);
        }
    }

    size_t job_visited_count = 0;
    while (!dag_queue.empty()) {
        std::string current_job_id = dag_queue.front();
        dag_queue.pop();
        ++job_visited_count;
        DagData& current_job = jobs_map[current_job_id];

        for (const auto& dep_job_name : current_job.outgoing_edges) {
            // iterate over the out degree dependencies of the current job 
            // and remove the current job from their in degree dependencies
            jobs_map[dep_job_name].incoming_edges.erase(current_job_id);
            if (jobs_map[dep_job_name].incoming_edges.empty()) {
                dag_queue.push(dep_job_name);
            }
        }
    }

    if (job_visited_count != jobs_map.size()) {
        std::string error_msg = "Cyclic dependency detected in the workflow";
        Logger::get_instance()->error(error_msg);
        rejection_reason = error_msg::CIRCULAR_DEPENDENCY;
        return false;
    }

    return true;
}



awaitable<ValidationResult> WorkflowService::handle_request_rejection(ValidationResult& result, 
                                                                      RequestData& request_info, 
                                                                      const std::string& rejection_reason,
                                                                      bool update_redis) {
    std::string reject_status;
    to_string(RequestStatus::REJECTED, reject_status);

   // Update request status in Redis since validation failed
    result.valid = false;
    result.status_str = rejection_reason;
    result.errors_msg = rejection_reason;

    request_info.status = std::move(reject_status);
    request_info.reject_reason = std::move(rejection_reason);

    if (update_redis) {
        co_await update_redis_request_status(request_info);
    }

    // Update request status in the database
    co_await DBFactory::get_database().update_request_status_async(request_info);

    co_return result;
}

awaitable<ValidationResult> WorkflowService::handle_request_accepted(ValidationResult& result, 
                                                                     RequestData& request_info) {
    std::string ok_status;
    to_string(RequestStatus::COMPLETED, ok_status);

    request_info.status = std::move(ok_status);
    request_info.reject_reason = error_msg::WORKFLOW_ADMITTED;

    co_await update_redis_request_status(request_info);

    // Update request status in the database
    co_await DBFactory::get_database().update_request_status_async(request_info);

    co_return result;
}

awaitable<void> WorkflowService::update_redis_request_status(const RequestData& request_info) {
    if (!co_await RedisDatabaseAsync::get_instance()->update_request_status_async(request_info.client_id, request_info.request_id, request_info.status)) {
        Logger::get_instance()->warn("Failed to update Redis request status for {}/{}", request_info.client_id, request_info.request_id);
    }

    std::string complete_status;
    to_string(RequestStatus::COMPLETED, complete_status);
    if(request_info.status != complete_status) {
        if (!co_await RedisDatabaseAsync::get_instance()->release_request_id_async(request_info.client_id, request_info.request_id)) {
            Logger::get_instance()->warn("Failed to release Redis request ID for {}/{}", request_info.client_id, request_info.request_id);
        }

        if (!co_await RedisDatabaseAsync::get_instance()->remove_active_workflow_async(request_info.client_id, request_info.workflow_id)) {
            Logger::get_instance()->warn("Failed to remove active workflow from Redis for {}/{}", request_info.client_id, request_info.workflow_id);
        }
    }
}

} // namespace flow_pilot
