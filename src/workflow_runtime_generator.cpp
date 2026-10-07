// workflow_runtime_generator.cpp - Build and persist workflow runtime data for FlowPilot

#include "workflow_runtime_generator.h"

#include <boost/uuid/time_generator_v7.hpp>

#include "db_factory.h"
#include "logger.h"

namespace flow_pilot {
using namespace boost::asio;
using json = nlohmann::json;

bool WorkflowRuntimeGenerator::build_jobs_runtime_info(const json& workflow_data,
                                                       const PolicyPlan& policy_config,
                                                       JobRuntimeMap& jobs_runtime_info,
                                                       JobPriorityQueue& ready_jobs) const {
    boost::uuids::time_generator_v7 gen;

    for (const auto& job : workflow_data["jobs"]) {
        JobRuntimeData job_info;
        job_info.job_uuid = gen();
        job_info.job_id = job["job_id"].get<std::string>();
        job_info.remaining_dependencies = static_cast<int>(job.value("depends_on", json::array()).size());
        job_info.priority = job.value("priority", policy_config.max_job_priority);
        job_info.timeout_sec = job.value("timeout_sec", policy_config.max_job_runtime_sec);
        job_info.max_retries = job.value("max_retries", policy_config.max_job_retries);
        job_info.current_retry_count = 0;
        job_info.retry_delay_sec = job.value("retry_delay_sec", 0);
        job_info.retry_backoff_policy = job.value("retry_backoff_policy", "IMMEDIAT");
        job_info.payload_size_bytes = 0;
        if (job.contains("payload") && !job["payload"].is_null()) {
            job_info.payload_size_bytes = static_cast<int>(job["payload"].get<std::vector<uint8_t>>().size());
        }

        if (job_info.remaining_dependencies == 0) {
            ready_jobs.emplace(PrioritizedJob{job_info.job_id, job_info.job_uuid, job_info.priority});
            job_info.status = to_string(JobStatus::READY);
        } else {
            job_info.status = to_string(JobStatus::PENDING);
        }

        if (job_info.remaining_dependencies < 0) {
            Logger::get_logger()->error(
                "Job '{}' has negative remaining dependencies: {}",
                job_info.job_id,
                job_info.remaining_dependencies);
            return false;
        }
        if (job_info.remaining_dependencies > policy_config.max_job_dependencies) {
            Logger::get_logger()->error(
                "Job '{}' has too many remaining dependencies: {}",
                job_info.job_id,
                job_info.remaining_dependencies);
            return false;
        }
        if (job_info.priority > policy_config.max_job_priority) {
            Logger::get_logger()->error(
                "Job '{}' priority {} exceeds max allowed {}",
                job_info.job_id,
                job_info.priority,
                policy_config.max_job_priority);
            return false;
        }
        if (job_info.timeout_sec > policy_config.max_job_runtime_sec) {
            Logger::get_logger()->error(
                "Job '{}' timeout {} exceeds max allowed {}",
                job_info.job_id,
                job_info.timeout_sec,
                policy_config.max_job_runtime_sec);
            return false;
        }
        if (job_info.max_retries > policy_config.max_job_retries) {
            Logger::get_logger()->error(
                "Job '{}' max retries {} exceeds max allowed {}",
                job_info.job_id,
                job_info.max_retries,
                policy_config.max_job_retries);
            return false;
        }
        if (job_info.retry_delay_sec > policy_config.max_job_retry_delay_sec) {
            Logger::get_logger()->error(
                "Job '{}' retry delay {} exceeds max allowed {}",
                job_info.job_id,
                job_info.retry_delay_sec,
                policy_config.max_job_retry_delay_sec);
            return false;
        }

        jobs_runtime_info[job_info.job_id] = job_info;
    }

    return true;
}

awaitable<bool> WorkflowRuntimeGenerator::initialize_runtime_data(const json& workflow_data,
                                                                  const PolicyPlan& policy_config,
                                                                  const WorkflowData& workflow_info,
                                                                  const JobPriorityQueue& ready_jobs,
                                                                  JobRuntimeMap& jobs_runtime_info) const {
    WorkflowRuntimeInfo workflow_runtime;
    std::vector<std::string> queued_jobs_list;
    std::vector<std::string> ready_jobs_list;

    workflow_runtime.identity.client_id = workflow_info.info.client_id;
    workflow_runtime.identity.workflow_id = workflow_info.info.workflow_id;
    workflow_runtime.workflow.workflow_id = workflow_info.info.workflow_id;
    workflow_runtime.workflow.status = to_string(WorkflowStatus::READY);
    workflow_runtime.workflow.max_concurrent_jobs = policy_config.max_concurrent_jobs;
    workflow_runtime.workflow.max_runtime_sec = policy_config.max_workflow_runtime_sec;
    workflow_runtime.workflow.total_jobs = workflow_info.total_jobs;
    workflow_runtime.workflow.pending_jobs = workflow_info.total_jobs - static_cast<int>(ready_jobs.size());
    workflow_runtime.workflow.completed_jobs = 0;
    workflow_runtime.workflow.failed_jobs = 0;

    size_t queued_job_list_size = workflow_runtime.workflow.max_concurrent_jobs;
    size_t ready_job_list_size = 0;
    if (ready_jobs.size() <= queued_job_list_size) {
        queued_job_list_size = ready_jobs.size();
    } else {
        ready_job_list_size = ready_jobs.size() - queued_job_list_size;
    }

    workflow_runtime.workflow.reserved_execution_slots = queued_job_list_size;
    workflow_runtime.jobs_queued_for_execution.reserve(queued_job_list_size);
    workflow_runtime.ready_job_list.reserve(ready_job_list_size);
    queued_jobs_list.reserve(queued_job_list_size);
    ready_jobs_list.reserve(ready_job_list_size);

    size_t index = 0;
    for (const auto& job_info : ready_jobs) {
        if (index < queued_job_list_size) {
            ++index;
            workflow_runtime.jobs_queued_for_execution.push_back(job_info);
            jobs_runtime_info.at(job_info.job_id).status = to_string(JobStatus::QUEUED);
            queued_jobs_list.push_back(job_info.job_id);
        } else {
            workflow_runtime.ready_job_list.push_back(job_info);
            ready_jobs_list.push_back(job_info.job_id);
        }
    }

    for (const auto& [job_id, job_data] : jobs_runtime_info) {
        workflow_runtime.jobs.push_back(job_data);
    }

    if (!co_await RedisDatabaseAsync::get_instance()->create_workflow_runtime_data_async(workflow_runtime)) {
        Logger::get_logger()->error(
            "initialize_runtime_data - Failed to create workflow runtime data in Redis for {}/{}",
            workflow_info.info.client_id,
            workflow_info.info.workflow_id);
        co_return false;
    }

    std::vector<std::string> added_payload_jobs;
    for (const auto& job : workflow_data["jobs"]) {
        if (job.contains("payload") && !job["payload"].is_null()) {
            const std::string job_id = job["job_id"].get<std::string>();
            std::vector<uint8_t> job_payload = job["payload"].get<std::vector<uint8_t>>();

            if (!co_await RedisDatabaseAsync::get_instance()->set_job_payload_async(
                    workflow_runtime.identity,
                    job_id,
                    job_payload)) {
                Logger::get_logger()->error(
                    "initialize_runtime_data - Failed to add job payload in Redis for {}/{}/{}",
                    workflow_info.info.client_id,
                    workflow_info.info.workflow_id,
                    job_id);
                co_await RedisDatabaseAsync::get_instance()->delete_all_jobs_payload_async(
                    workflow_runtime.identity,
                    added_payload_jobs);
                co_await RedisDatabaseAsync::get_instance()->delete_workflow_runtime_data_async(workflow_runtime);
                co_return false;
            }

            added_payload_jobs.push_back(job_id);
        }
    }

    if (!co_await DBFactory::get().update_ready_jobs_async(
            workflow_runtime.identity.client_id,
            workflow_runtime.identity.workflow_id,
            queued_jobs_list,
            ready_jobs_list)) {
        Logger::get_logger()->error(
            "initialize_runtime_data - Failed to update ready jobs status in the database for workflow {}/{}",
            workflow_info.info.client_id,
            workflow_info.info.workflow_id);
        co_await RedisDatabaseAsync::get_instance()->delete_all_jobs_payload_async(
            workflow_runtime.identity,
            added_payload_jobs);
        co_await RedisDatabaseAsync::get_instance()->delete_workflow_runtime_data_async(workflow_runtime);
        co_return false;
    }

    if (!co_await RedisDatabaseAsync::get_instance()->queue_workflow_jobs_for_execution_async(workflow_runtime.identity,
                                                                                              workflow_runtime.jobs_queued_for_execution)) {
        Logger::get_logger()->error(
            "initialize_runtime_data - Failed to publish queued jobs for execution in Redis for workflow {}/{}",
            workflow_info.info.client_id,
            workflow_info.info.workflow_id);
        co_await RedisDatabaseAsync::get_instance()->remove_jobs_from_execution_queue_async(workflow_runtime.identity,
                                                                                            workflow_runtime.jobs_queued_for_execution);
        co_await RedisDatabaseAsync::get_instance()->delete_all_jobs_payload_async(workflow_runtime.identity, added_payload_jobs);
        co_await RedisDatabaseAsync::get_instance()->delete_workflow_runtime_data_async(workflow_runtime);
        co_return false;
    }

    co_return true;
}

} // namespace flow_pilot 
