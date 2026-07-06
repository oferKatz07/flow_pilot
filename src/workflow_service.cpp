
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
    try {
        workflow_data = json::parse(body);
    }
    catch (const nlohmann::json::parse_error& e) {
        co_return ValidationResult{
            false,
            INVALID_JSON_FORMAT,
            {std::string("Parse error: ") + e.what()}
        };
    }

    try {
        validator_.validate(workflow_data);
    }
    catch (const std::exception& e) {
        co_return ValidationResult{
            false,
            SCHEMA_VALIDATION_FAILED,
            {std::string("Schema error: ") + e.what()}
        };
    }

    RequestData workflow_request_info;
    workflow_request_info.client_id = workflow_data.value("client_id", std::string());
    workflow_request_info.request_id = workflow_data.value("request_id", std::string());
    workflow_request_info.workflow_id = workflow_data.value("workflow_id", std::string());
    workflow_request_info.workflow_payload_size_bytes = static_cast<int>(body.size());
    workflow_request_info.operation = "CREATE";
//    workflow_info.workflow_type = workflow_data.value("workflow_type", std::string());
//    workflow_info.job_count = static_cast<int>(workflow_data["jobs"].size());
//    workflow_info.workflow_version = Config::get_config().workflow().version;
//    to_string(WorkflowStatus::ADMITTED, workflow_info.status);

    // Get client config from DB for policy and rate limit validation
    // If client is not found, the assumption is that the client is not registered and the request is rejected
    ClientConfig client_config;
    if (!get_client_config(workflow_request_info.client_id, client_config)) {
        co_return ValidationResult{
            false,
            CLIENT_NOT_FOUND,
            CLIENT_NOT_FOUND
        };
    }

    ValidationResult result = {
        true,
        "Workflow validation successful",
        {}
    };

    // Validate received workflow against client's policy plan
    validate_admission_client_workflow_policy(workflow_data, body.size(), client_config.policy_config, result);
    if (!result.valid) {
        co_return result;
    }

    std::string rejection_reason;
    std::string reject_status;
    to_string(WorkflowStatus::REJECTED, reject_status);
    auto redis_db = RedisDatabaseAsync::get_instance();
    bool res = co_await redis_db->admit_request_async(workflow_request_info.client_id,
                                                      workflow_request_info.request_id,
                                                      workflow_request_info.workflow_id,  
                                                      client_config.rate_limit_config.max_concurrent_workflows,  
                                                      client_config.rate_limit_config.max_requests,  
                                                      client_config.rate_limit_config.window_sec,  
                                                      rejection_reason);  
    if (!res) {
        result.valid = false;
        result.message = rejection_reason;
        result.errors = rejection_reason;

        if (rejection_reason != DUPLICATE_REQUEST) {
            // Persist the failed request in the DB for auditing purposes
            workflow_request_info.status = reject_status;
            workflow_request_info.reject_reason = rejection_reason;
            co_await DBFactory::get_database().add_request_async(workflow_request_info, body, rejection_reason);
        }

        co_return result;
    }

    std::unordered_map<std::string, DagData> jobs_map;
    validate_semantic(workflow_data, jobs_map, result);
    if (!result.valid) {
        // Update request status in Redis since semantic validation failed
        std::ignore = update_redis_request_status(workflow_request_info.client_id, workflow_request_info.request_id, reject_status);
        co_return result;
    }

    validate_dependencies(workflow_data, jobs_map, result);
    if (!result.valid) {
        // Update request status in Redis since dependency validation failed
        std::ignore = update_redis_request_status(workflow_request_info.client_id, workflow_request_info.request_id, reject_status);
        co_return result;
    }

    // Add the workflow request record to the DB for durability and auditing
    std::string error_message;
    if (!co_await DBFactory::get_database().add_workflow_async(workflow_request_info, error_message)) {
        result.valid = false;
        result.message = std::move(error_message);
        result.errors = "Failed to add workflow to the database";

        // Update request status in Redis since DB insertion failed
        std::ignore = update_redis_request_status(workflow_request_info.client_id, workflow_request_info.request_id, reject_status);

        co_return result;
    }

    if (!co_await redis_db->update_request_status_async(workflow_request_info.client_id, 
                                                        workflow_request_info.request_id, 
                                                        workflow_request_info.status)) {
        Logger::get_instance()->warn("Failed to update Redis request status for {}/{}", 
                                     workflow_request_info.client_id, workflow_request_info.request_id);
    }

    co_return result;
}

void WorkflowService::validate_admission_client_workflow_policy(const json& workflow_data, const size_t workflow_size_bytes, 
                                                               const PolicyPlan& policy_config, ValidationResult& result) {
    float workflow_size_kb = workflow_size_bytes / 1024.0f;
    if (workflow_size_kb > policy_config.max_workflow_size_kb) {
        result.valid = false;
        result.message = WORKFLOW_SIZE_EXCEEDED;
        result.errors = "Workflow size is " + std::to_string(workflow_size_kb) + " KB, but the maximum allowed is " + 
                        std::to_string(policy_config.max_workflow_size_kb) + " KB";

        return;
    } 

    int job_count = workflow_data["jobs"].size();
    if (job_count > policy_config.max_jobs_in_workflow) {
        result.valid = false;
        result.message = JOB_COUNT_EXCEEDED;
        result.errors = "Workflow contains " + std::to_string(job_count) + " jobs, but the maximum allowed is " + std::to_string(policy_config.max_jobs_in_workflow);
    } else if (job_count == 0) {
        result.valid = false;
        result.message = JOB_COUNT_ZERO;
        result.errors = JOB_COUNT_ZERO;
    }

    if (!result.valid) {
        return;
    }

    for (const auto& job : workflow_data["jobs"]) {
        int job_size_bytes = job.dump().size();
        if (job_size_bytes > policy_config.max_job_size_bytes) {
            result.valid = false;
            result.message = JOB_SIZE_EXCEEDED;
            result.errors = "Job '" + job["job_id"].get<std::string>() + "' size is " + std::to_string(job_size_bytes) + " bytes, but the maximum allowed is " + std::to_string(policy_config.max_job_size_bytes) + " bytes";
        }
    }
}

void WorkflowService::validate_semantic(const json& data, std::unordered_map<std::string, DagData>& jobs_map, ValidationResult& result)
{
    for (const auto& job : data["jobs"]) {
        DagData job_data;
        job_data.job_id = job["job_id"].get<std::string>();
        // Check for duplicate jobs in the dependency list of the same job
        json dependencies = job.value("depends_on", json::array());
        for (const auto& dep : dependencies) {
            auto res = job_data.dependencies.insert(dep.get<std::string>());
            if (!res.second) {
                result.valid = false;
                result.message = "Duplicate dependency found for job " + job_data.job_id;
                result.errors = "Dependency '" + dep.get<std::string>() + "' is duplicated for job '" + job_data.job_id + "'";
                return;
            }
        }

        if (jobs_map.find(job_data.job_id) != jobs_map.end()) {
            // Duplicate job ID found
            result.valid = false;
            result.message = "Duplicate job ID found for job '" + job_data.job_id + "'";
            result.errors = "Job ID '" + job_data.job_id + "' is duplicated";
            return;
        }

        jobs_map[job_data.job_id] = job_data;
    }
}

void WorkflowService::validate_dependencies(const json& data, std::unordered_map<std::string, DagData>& jobs_map, ValidationResult& result)
{
    // Preparing the data structures for creating a DAG inorder to apply the kahn’s algorithm
    std::queue<std::string> dag_queue;
    for (auto& job : jobs_map) {
        // Fill incoming_edges and out_deg_dependencies for each job
        for (const std::string& dep_name : job.second.dependencies) {
            job.second.incoming_edges.insert(dep_name);
            // Check if the dependency exists in the jobs map
            if (jobs_map.find(dep_name) == jobs_map.end()) {
                result.valid = false;
                result.message = "Missing dependency detected for job '" + job.first + "'";
                result.errors ="Dependency '" + dep_name + "' is not defined as a job";
                return;
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
        result.valid = false;
        result.message = "Cyclic dependency detected in the workflow";
        result.errors = "The workflow contains a cycle, which is not allowed.";
    }
}

awaitable<void> WorkflowService::update_redis_request_status(const std::string& client_id, const std::string& request_id, const std::string& status) {
    if (!co_await RedisDatabaseAsync::get_instance()->update_request_status_async(client_id, request_id, status)) {
        Logger::get_instance()->warn("Failed to update Redis request status for {}/{}", client_id, request_id);
    }

    if(status != "ADMITTED") {
        if (!co_await RedisDatabaseAsync::get_instance()->release_request_id_async(client_id, request_id)) {
            Logger::get_instance()->warn("Failed to release Redis request ID for {}/{}", client_id, request_id);
        }
    }
}

} // namespace flow_pilot
