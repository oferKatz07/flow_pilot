
// workflow_admission_service.cpp - Implementation of WorkflowService for FlowPilot

#include <fstream>
#include <exception>
#include <queue>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/time_generator_v7.hpp>
#include <boost/uuid/uuid_io.hpp>

#include "flow_pilot_error_msgs.h"
#include "logger.h"
#include "client_config.h"
#include "config.h"
#include "client_config_manager_factory.h"
#include "db_factory.h"
#include "workflow_admission_service.h"

namespace flow_pilot {
using namespace boost::asio;

WorkflowAdmissionService::WorkflowAdmissionService(const std::string& schema_path)
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

 awaitable<ValidationResult> WorkflowAdmissionService::submit_workflow(const std::string& body)
{
    json workflow_data;
    RequestData workflow_request_info;
    ValidationResult result = {
        true,
        StatusCodes::WORKFLOW_ADMITTED,  // "Workflow was validated and admitted"
        "" // No errors
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

    StatusCodes rejection_reason;
    // Check if the client can accept the request based on rate limits and active workflows
    bool res = co_await admit_request(workflow_request_info, client_config.rate_limit_config, rejection_reason);
    if (!res) {
        co_return co_await handle_request_rejection(result, workflow_request_info, ValidationStage::ADMIT_REQUEST, rejection_reason);
    }

    // Persist the request in the DB for auditing purposes
    res = co_await persist_request(workflow_request_info, body, client_config, rejection_reason);
    if (!res) {
        co_return co_await handle_request_rejection(result, workflow_request_info, ValidationStage::PERSIST_REQUEST, rejection_reason);
    }

    // Validate received workflow against client's policy plan and semantic correctness
    std::unordered_map<std::string, DagData> jobs_map;
    std::unordered_map<std::string, job_runtime_data> jobs_runtime_info;
    std::vector<std::string> ready_jobs;
    WorkflowfullData workflow_info;
    res = get_jobs_runtime_info(workflow_data, client_config.policy_config, jobs_runtime_info, ready_jobs);
    if (!res) {
        co_return co_await handle_request_rejection(result, workflow_request_info, ValidationStage::VALIDATE_WORKFLOW, 
                                                    StatusCodes::JOB_POLICY_VIOLATION);
    }

    res = validate_workflow(workflow_data, client_config.policy_config, jobs_runtime_info, jobs_map, 
                            workflow_info, rejection_reason);
    if (!res) {
        co_return co_await handle_request_rejection(result, workflow_request_info, ValidationStage::VALIDATE_WORKFLOW, 
                                                    rejection_reason);
    }

    workflow_info.info = std::move(workflow_request_info);
    res = co_await persist_workflow(workflow_info, jobs_map, client_config.policy_config.max_job_retries, 
                                    rejection_reason);
    if (!res) {
        co_return co_await handle_request_rejection(result, workflow_info.info, ValidationStage::PERSIST_WORKFLOW, rejection_reason);
    }

    if (!co_await generate_workflow_runtime_data(workflow_data, client_config.policy_config, workflow_info, jobs_runtime_info, ready_jobs)) {
        rejection_reason = StatusCodes::INTERNAL_DB_FAILURE;
        co_return co_await handle_request_rejection(result, workflow_info.info, ValidationStage::GENERATE_RUNTIME_DATA, rejection_reason);
    }

    co_return co_await handle_request_accepted(result, workflow_info.info);
}

bool WorkflowAdmissionService::parse_request(const std::string& body, json& workflow_data, 
                                    RequestData& request_info, ValidationResult& result) {
    try {
        workflow_data = json::parse(body);
    }
    catch (const nlohmann::json::parse_error& e) {
        result.valid = false;
        result.status_code = StatusCodes::INVALID_JSON_FORMAT;
        result.errors_msg = std::string("JSON parse error: ") + e.what();

        return false;
    }

    try {
        validator_.validate(workflow_data);
    }
    catch (const std::exception& e) {
        result.valid = false;
        result.status_code = StatusCodes::SCHEMA_VALIDATION_FAILED;
        result.errors_msg = std::string("Schema validation error: ") + e.what();

        return false;
    }

    request_info.client_id = workflow_data.value("client_id", std::string());
    request_info.request_id = workflow_data.value("request_id", std::string());
    request_info.workflow_id = workflow_data.value("workflow_id", std::string());
    request_info.workflow_payload_size_bytes = static_cast<int>(body.size());
    request_info.operation = "CREATE";

    request_info.status = RequestStatus::RECEIVED;

    return true;
}

bool WorkflowAdmissionService::get_client_config_data(const std::string& client_id, ClientConfig& client_config, ValidationResult& result) {
    if (!ClientConfigManagerFactory::get().get_client_config(client_id, client_config)) {
        result.valid = false;
        result.status_code = StatusCodes::CLIENT_NOT_FOUND;
        result.errors_msg = std::string(error_msgs::CLIENT_NOT_FOUND);
        return false;
    }
    
    return true;
}

boost::asio::awaitable<bool> WorkflowAdmissionService::admit_request(const RequestData& request_info, 
                                                            const RateLimitConfig& rate_limit_config, 
                                                            StatusCodes& rejection_reason) {
    auto redis_db = RedisDatabaseAsync::get_instance();
    co_return co_await redis_db->admit_request_async(request_info.client_id,
                                                      request_info.request_id,
                                                      request_info.workflow_id,  
                                                      rate_limit_config.max_concurrent_workflows,  
                                                      rate_limit_config.max_requests,  
                                                      rate_limit_config.window_sec,  
                                                      rejection_reason);  
}

boost::asio::awaitable<bool> WorkflowAdmissionService::persist_request(const RequestData& request_info, 
                                                              const std::string& body, 
                                                              const ClientConfig& client_config, 
                                                              StatusCodes& rejection_reason) {
    co_return co_await DBFactory::get().add_request_async(request_info, body, 
                                                          client_config, rejection_reason);
}

bool WorkflowAdmissionService::validate_workflow(const json& workflow_data, const PolicyPlan& policy_config, 
                                        std::unordered_map<std::string, job_runtime_data>& jobs_runtime_info, 
                                        std::unordered_map<std::string, DagData>& jobs_map, 
                                        WorkflowfullData& workflow_info,
                                        StatusCodes& rejection_reason) {
    if (!validate_admission_client_workflow_policy(workflow_data, policy_config, rejection_reason)) {
        return false;
    }

    if (!validate_semantic(workflow_data, jobs_map, rejection_reason)) {
        return false;
    }

    if (!validate_dependencies(workflow_data, jobs_runtime_info, jobs_map, rejection_reason)) {
        return false;
    }

    // Extract the workflow info from the received data
    workflow_info.workflow_type = workflow_data.value("workflow_type", std::string());
    workflow_info.total_jobs = static_cast<int>(workflow_data["jobs"].size());
    workflow_info.workflow_version = Config::get().workflow().version;
    workflow_info.status = WorkflowStatus::ADMITTED;

    return true;
}

boost::asio::awaitable<bool> WorkflowAdmissionService::persist_workflow(const WorkflowfullData& workflow_info, 
                                                               const std::unordered_map<std::string, DagData>& jobs_map, 
                                                               int policy_jobs_retry_num,
                                                               StatusCodes& rejection_reason) {
    
    bool ret_val = co_await DBFactory::get().add_workflow_async(workflow_info, rejection_reason);
    if (ret_val) {
        // Prepare the workflow jobs list to be persisted in the database
        WorkflowJobList job_list;
        boost::uuids::time_generator_v7 gen;
        job_list.client_id = workflow_info.info.client_id;
        job_list.workflow_id = workflow_info.info.workflow_id;
        job_list.retry_count = policy_jobs_retry_num;
        for (const auto& [job_id, dag_data] : jobs_map) {
            JobData job_info;
            job_info.job_uuid = boost::uuids::to_string(gen());
            job_info.job_id = dag_data.job_id;
            job_info.status = JobStatus::PENDING;
            job_list.jobs.emplace_back(job_info);
        }

        ret_val = co_await DBFactory::get().add_workflow_jobs_async(job_list, rejection_reason);

        if (ret_val) {
            co_await DBFactory::get().update_workflow_status_async(workflow_info.info.client_id, workflow_info.info.workflow_id, WorkflowStatus::READY);
        } else {
            co_await DBFactory::get().update_workflow_status_async(workflow_info.info.client_id, workflow_info.info.workflow_id, WorkflowStatus::FAILED);
        }
    }

    co_return ret_val;
}

bool WorkflowAdmissionService::validate_admission_client_workflow_policy(const json& workflow_data,  
                                                                const PolicyPlan& policy_config, 
                                                                StatusCodes& rejection_reason) {
    std::string error_msg;                                                        
    int job_count = workflow_data["jobs"].size();
    if (job_count > policy_config.max_jobs_in_workflow) {
        error_msg = "Workflow has " + std::to_string(job_count) + " jobs, but the maximum allowed is " + std::to_string(policy_config.max_jobs_in_workflow);
        Logger::get_logger()->error(error_msg);
        rejection_reason = StatusCodes::JOB_COUNT_EXCEEDED;
        return false;
    }

    if (job_count == 0) {
        error_msg = "Workflow has no jobs";
        Logger::get_logger()->error(error_msg);
        rejection_reason = StatusCodes::JOB_COUNT_ZERO;
        return false;
    }

    for (const auto& job : workflow_data["jobs"]) {
        size_t job_size_bytes = job.dump().size();
        if (job_size_bytes > policy_config.max_job_size_bytes) {
            error_msg = "Job '" + job["job_id"].get<std::string>() + "' size is " + std::to_string(job_size_bytes); 
            error_msg += " bytes, but the maximum allowed is " + std::to_string(policy_config.max_job_size_bytes) + " bytes";
            Logger::get_logger()->error(error_msg);
            rejection_reason = StatusCodes::JOB_SIZE_EXCEEDED;
            return false;
        }
    }

    return true;
}

bool WorkflowAdmissionService::validate_semantic(const json& data, 
                                        std::unordered_map<std::string, DagData>& jobs_map, 
                                        StatusCodes& rejection_reason)
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
                Logger::get_logger()->error(error_msg);
                rejection_reason = StatusCodes::DUPLICATE_DEPENDENCY;
                return false;
            }
        }

        if (jobs_map.find(job_data.job_id) != jobs_map.end()) {
            // Duplicate job ID found
            error_msg = "Duplicate job ID found for job '" + job_data.job_id + "'";
            Logger::get_logger()->error(error_msg);
            rejection_reason = StatusCodes::DUPLICATE_JOB_ID;
            return false;
        }

        jobs_map[job_data.job_id] = job_data;
    }

    return true;
}

bool WorkflowAdmissionService::validate_dependencies(const json& data, 
                                            std::unordered_map<std::string, job_runtime_data>& jobs_runtime_info, 
                                            std::unordered_map<std::string, DagData>& jobs_map, 
                                            StatusCodes& rejection_reason)
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
                Logger::get_logger()->error(error_msg);
                rejection_reason = StatusCodes::MISSING_DEPENDENCY;
                return false;
            }
            // Add the current job to the outgoing edges (successors list) of its dependencies
            jobs_map[dep_name].outgoing_edges.insert(job.first);
            jobs_runtime_info[dep_name].successors.push_back(job.first);
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
        Logger::get_logger()->error(error_msg);
        rejection_reason = StatusCodes::CIRCULAR_DEPENDENCY;
        return false;
    }

    return true;
}

awaitable<ValidationResult> WorkflowAdmissionService::handle_request_rejection(ValidationResult& result, 
                                                                      RequestData& request_info,
                                                                      ValidationStage validation_stage,
                                                                      const StatusCodes& rejection_reason) {
    std::string_view errors_msg;
    errors_msg = status_code_to_string(rejection_reason);

   // Update request status in Redis since validation failed
    result.valid = false;
    result.status_code = rejection_reason;
    result.errors_msg = errors_msg;

    request_info.status = RequestStatus::REJECTED;
    request_info.reject_reason = errors_msg;

    if ( validation_stage >= ValidationStage::PERSIST_REQUEST) {
        co_await update_redis_request_status(request_info);
    }

    if ( validation_stage > ValidationStage::PERSIST_REQUEST) {
        // Update request status in the database
        co_await DBFactory::get().update_request_status_async(request_info);
    }

    if ( validation_stage > ValidationStage::PERSIST_REQUEST) {
        // Update Workflow status and its jobs status to FAILED
        co_await DBFactory::get().fail_workflow_async(request_info.client_id, request_info.workflow_id);
    }

    co_return result;
}

awaitable<ValidationResult> WorkflowAdmissionService::handle_request_accepted(ValidationResult& result, 
                                                                              RequestData& request_info) {
    request_info.status = RequestStatus::ADMITTED;
    request_info.reject_reason = error_msgs::WORKFLOW_ADMITTED;

    co_await update_redis_request_status(request_info);

    co_await DBFactory::get().update_request_status_async(request_info);

    co_return result;
}

awaitable<void> WorkflowAdmissionService::update_redis_request_status(const RequestData& request_info) {
    if (!co_await RedisDatabaseAsync::get_instance()->update_request_status_async(request_info.client_id, request_info.request_id, std::string(to_string(request_info.status)))) {
        Logger::get_logger()->warn("Failed to update Redis request status for {}/{}", request_info.client_id, request_info.request_id);
    }

    if(request_info.status != RequestStatus::ADMITTED) {
        if (!co_await RedisDatabaseAsync::get_instance()->release_request_id_async(request_info.client_id, request_info.request_id)) {
            Logger::get_logger()->warn("Failed to release Redis request ID for {}/{}", request_info.client_id, request_info.request_id);
        }

        if (!co_await RedisDatabaseAsync::get_instance()->remove_active_workflow_async(request_info.client_id, request_info.workflow_id)) {
            Logger::get_logger()->warn("Failed to remove active workflow from Redis for {}/{}", request_info.client_id, request_info.workflow_id);
        }
    }
}

boost::asio::awaitable<bool> WorkflowAdmissionService::generate_workflow_runtime_data(const json& workflow_data, const PolicyPlan& policy_config, 
                                                              const WorkflowfullData& workflow_info, 
                                                              const std::unordered_map<std::string, job_runtime_data>& jobs_runtime_info,
                                                              const std::vector<std::string>& ready_jobs) {
    workflow_runtime_info workflow_runtime;
    // Set the Workflow identity
    workflow_runtime.identity.client_id = workflow_info.info.client_id;
    workflow_runtime.identity.workflow_id = workflow_info.info.workflow_id;
    // Set the Workflow runtime data
    workflow_runtime.workflow.workflow_id = workflow_info.info.workflow_id;
    workflow_runtime.workflow.status = to_string(WorkflowStatus::RUNNING);
    workflow_runtime.workflow.max_concurrent_jobs = policy_config.max_concurrent_jobs; 
    workflow_runtime.workflow.max_runtime_sec = policy_config.max_workflow_runtime_sec;
    workflow_runtime.workflow.total_jobs = workflow_info.total_jobs;
    workflow_runtime.workflow.pending_jobs = workflow_info.total_jobs - static_cast<int>(ready_jobs.size());
    workflow_runtime.workflow.completed_jobs = 0;
    workflow_runtime.workflow.failed_jobs = 0;

    // Update the workflow jobs runtime data
    for (const auto& [job_id, job_data] : jobs_runtime_info) {
        workflow_runtime.jobs.push_back(job_data);
    }

    if (!co_await RedisDatabaseAsync::get_instance()->create_workflow_runtime_data_async(workflow_runtime)) {
        Logger::get_logger()->error("Failed to create workflow runtime data in Redis for {}/{}", workflow_info.info.client_id, workflow_info.info.workflow_id);
        co_return false;
    }

    // Update the jobs payload in redis
    // To minimize the required stack the payload is added one job at a time
    std::vector<std::string> added_payload_jobs;
    for (const auto& job : workflow_data["jobs"]) {
        std::vector<uint8_t> job_payload;
        if (job.contains("payload") && !job["payload"].is_null()) {
            // Update redis only with jobs that have a payload
            std::string job_id = job["job_id"].get<std::string>();
            job_payload = job["payload"].get<std::vector<uint8_t>>();
            if (!co_await RedisDatabaseAsync::get_instance()->set_job_payload_async(workflow_runtime.identity, job_id, job_payload)) {
                Logger::get_logger()->error("Failed to add job payload in Redis for {}/{}/{}", 
                                            workflow_info.info.client_id, workflow_info.info.workflow_id, job["job_id"].get<std::string>());
                co_await RedisDatabaseAsync::get_instance()->delete_all_jobs_payload_async(workflow_runtime.identity, added_payload_jobs);
                co_await RedisDatabaseAsync::get_instance()->delete_workflow_runtime_data_async(workflow_runtime);
                co_return false;
            }
            
            added_payload_jobs.push_back(job_id);
        }
    }
    // Persist Redy job status
    if (!co_await DBFactory::get().update_ready_jobs_async(workflow_runtime.identity.client_id, 
                                                           workflow_runtime.identity.workflow_id, ready_jobs)) {
        Logger::get_logger()->error("Failed to publish ready jobs in Redis for {}/{}", workflow_info.info.client_id, workflow_info.info.workflow_id);
        co_await RedisDatabaseAsync::get_instance()->delete_all_jobs_payload_async(workflow_runtime.identity, added_payload_jobs);
        co_await RedisDatabaseAsync::get_instance()->delete_workflow_runtime_data_async(workflow_runtime);
        co_return false;
    }

    // Update the ready jobes queue
    if (!co_await RedisDatabaseAsync::get_instance()->publish_workflow_ready_jobs_async(workflow_runtime.identity, ready_jobs)) {
        Logger::get_logger()->error("Failed to publish ready jobs in Redis for {}/{}", workflow_info.info.client_id, workflow_info.info.workflow_id);
        co_await RedisDatabaseAsync::get_instance()->delete_all_jobs_payload_async(workflow_runtime.identity, added_payload_jobs);
        co_await RedisDatabaseAsync::get_instance()->delete_workflow_runtime_data_async(workflow_runtime);
        co_return false;
    }

    co_return true;
}

bool WorkflowAdmissionService::get_jobs_runtime_info(const json& workflow_data, 
                                            const PolicyPlan& policy_config,
                                            std::unordered_map<std::string, job_runtime_data>& jobs_runtime_info,
                                            std::vector<std::string>& ready_jobs) {
    for (const auto& job : workflow_data["jobs"]) {
        job_runtime_data job_info;
        job_info.job_id = job["job_id"].get<std::string>();
        job_info.remaining_dependencies = static_cast<int>(job.value("depends_on", json::array()).size());
        job_info.priority = job.value("priority", policy_config.max_job_priority);
        job_info.timeout_sec = job.value("timeout_sec", policy_config.max_job_runtime_sec);
        job_info.max_retries = job.value("max_retries", policy_config.max_job_retries);
        job_info.current_retry_count = 0;
        job_info.retry_delay_sec = job.value("retry_delay_sec", 0);
        job_info.retry_backoff_policy = job.value("retry_backoff_policy", "IMMEDIAT");
        if (job_info.remaining_dependencies == 0) {
            ready_jobs.push_back(job_info.job_id);
            job_info.status = to_string(JobStatus::READY);
        } else {
            job_info.status = to_string(JobStatus::PENDING);
        }

        if (job_info.remaining_dependencies < 0) {
            Logger::get_logger()->error("Job '{}' has negative remaining dependencies: {}", job_info.job_id, job_info.remaining_dependencies);
            return false;
        }
        if (job_info.remaining_dependencies > policy_config.max_job_dependencies) {
            Logger::get_logger()->error("Job '{}' has too many remaining dependencies: {}", job_info.job_id, job_info.remaining_dependencies);
            return false;
        }
        if (job_info.priority > policy_config.max_job_priority) {
            Logger::get_logger()->error("Job '{}' priority {} exceeds max allowed {}", job_info.job_id, job_info.priority, policy_config.max_job_priority);
            return false;
        }
        if (job_info.timeout_sec > policy_config.max_job_runtime_sec) {
            Logger::get_logger()->error("Job '{}' timeout {} exceeds max allowed {}", job_info.job_id, job_info.timeout_sec, policy_config.max_job_runtime_sec);
            return false;
        }
        if (job_info.max_retries > policy_config.max_job_retries) {
            Logger::get_logger()->error("Job '{}' max retries {} exceeds max allowed {}", job_info.job_id, job_info.max_retries, policy_config.max_job_retries);
            return false;
        }
        if (job_info.retry_delay_sec > policy_config.max_job_retry_delay_sec) {
            Logger::get_logger()->error("Job '{}' retry delay {} exceeds max allowed {}", job_info.job_id, job_info.retry_delay_sec, policy_config.max_job_retry_delay_sec);
            return false;
        }

        jobs_runtime_info[job_info.job_id] = job_info;
    }

    return true;
}

}// namespace flow_pilot
