// completion_handler.cpp


#include "logger.h"
#include "db_factory.h"
#include "redis_db_async.h"
#include "completion_handler.h"

#include <algorithm>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <chrono>
#include <ctime>
#include <random>
#include <string>
#include <unordered_map>
#include <vector>

namespace flow_pilot {

namespace {

constexpr auto JOB_COMPLETION_WAIT_TIMEOUT = std::chrono::seconds(1);

int parse_int_field(const std::unordered_map<std::string, std::string>& fields,
                    const std::string& field_name,
                    int default_value = 0)
{
    const auto it = fields.find(field_name);
    if (it == fields.end()) {
        return default_value;
    }

    try {
        return std::stoi(it->second);
    } catch (...) {
        return default_value;
    }
}

std::string now_as_string()
{
    return std::to_string(std::time(nullptr));
}

long long now_ms()
{
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
}

int retry_jitter_ms()
{
    thread_local std::mt19937 generator(std::random_device{}());
    std::uniform_int_distribution<int> distribution(1, 1000);
    return distribution(generator);
}

bool is_waiting_to_run(JobStatus status)
{
    return status == JobStatus::PENDING ||
           status == JobStatus::RETRY_DELAY ||
           status == JobStatus::READY ||
           status == JobStatus::QUEUED;
}

bool is_job_runtime_expired(const JobRuntimeData& job_runtime)
{
    const int max_job_runtime_sec = job_runtime.timeout_sec;
    if (job_runtime.start_run_time <= 0 || max_job_runtime_sec <= 0) {
        return false;
    }

    const auto now = static_cast<long long>(std::time(nullptr));
    return now - job_runtime.start_run_time > max_job_runtime_sec;
}

} // namespace

CompletionHandler::CompletionHandler(bool auto_start)
    : redis_context_(std::make_unique<RedisConnectionContext>(RedisDatabaseAsync::get_instance()->io_context()))
{
    if (auto_start) {
        start();
    } else {
        running_.store(true, std::memory_order_release);
    }
}

CompletionHandler::~CompletionHandler()
{
    request_stop();
    wait_for_main_loop_finished();
}

void CompletionHandler::start()
{
    running_.store(true, std::memory_order_release);
    main_loop_started_.store(true, std::memory_order_release);

    auto redis_db = RedisDatabaseAsync::get_instance();
    boost::asio::co_spawn(
        redis_db->io_context(),
        [this]() -> boost::asio::awaitable<void> {
            try {
                co_await completion_handler_main_loop();
            } catch (const std::exception& e) {
                Logger::get_logger()->error("Completion handler main loop failed: {}", e.what());
            } catch (...) {
                Logger::get_logger()->error("Completion handler main loop failed with an unknown exception");
            }
            mark_main_loop_finished();
            co_return;
        },
        boost::asio::detached);
}

void CompletionHandler::request_stop()
{
    bool was_running = true;
    if (!running_.compare_exchange_strong(was_running, false, std::memory_order_acq_rel)) {
        return;
    }
    redis_context_->close();
}

void CompletionHandler::mark_main_loop_finished()
{
    {
        std::lock_guard<std::mutex> lock(main_loop_finished_mutex_);
        main_loop_finished_ = true;
    }
    main_loop_finished_cv_.notify_all();
}

void CompletionHandler::wait_for_main_loop_finished()
{
    if (!main_loop_started_.load(std::memory_order_acquire)) {
        return;
    }

    std::unique_lock<std::mutex> lock(main_loop_finished_mutex_);
    main_loop_finished_cv_.wait(lock, [this]() {
        return main_loop_finished_;
    });
}

boost::asio::awaitable<void> CompletionHandler::completion_handler_main_loop() {
    while (running_.load(std::memory_order_acquire)) {
        // Wait for a job completion event from Redis
        JobCompletionData completion_data;
        if (!co_await get_completed_jobs(completion_data)) {
            if (!running_.load(std::memory_order_acquire)) {
                break; // Exit the loop if the handler is no longer running
            }
            Logger::get_logger()->error("Failed to get completed jobs");
            continue;
        }
        // Process the completed job
        Logger::get_logger()->info(
            "Processing completed job: client_id={}, workflow_id={}, job_id={}, status={}, error_code={}",
            completion_data.identity.client_id,
            completion_data.identity.workflow_id,
            completion_data.job_id,
            to_string(completion_data.status),
            status_code_to_string(completion_data.error_code));
        if (!co_await process_completed_jobs(completion_data)) {
            Logger::get_logger()->error("Failed to process completed job: client_id={}, workflow_id={}, job_id={}",
                                        completion_data.identity.client_id,
                                        completion_data.identity.workflow_id,
                                        completion_data.job_id);
        }
    }

    Logger::get_logger()->info("Completion handler main loop has exited.");
}

boost::asio::awaitable<bool> CompletionHandler::get_completed_jobs(JobCompletionData& completion_data)
{
    auto redis_db = RedisDatabaseAsync::get_instance();

    while (running_.load(std::memory_order_acquire)) {
        const auto wait_result = co_await redis_db->wait_for_job_completion_event_async(
            *redis_context_,
            JOB_COMPLETION_WAIT_TIMEOUT);

        if (wait_result == JobCompletionWaitResult::TIMEOUT) {
            continue;
        }

        if (wait_result == JobCompletionWaitResult::ERROR) {
            Logger::get_logger()->error("Failed while waiting for job completion event");
            co_return false;
        }

        if (!co_await redis_db->dequeue_job_completion_async(*redis_context_, completion_data)) {
            Logger::get_logger()->error("Failed to dequeue job completion message");
            co_return false;
        }

        if (!completion_data.stream_id.empty()) {
            co_return true;
        }
    }

    co_return false;
}

boost::asio::awaitable<bool> CompletionHandler::process_completed_jobs(JobCompletionData& completion_data)
{
    auto redis_db = RedisDatabaseAsync::get_instance();
    const auto& identity = completion_data.identity;

    std::unordered_map<std::string, std::string> workflow_fields;
    if (!co_await redis_db->fetch_workflow_runtime_async(*redis_context_, identity, workflow_fields) ||
        workflow_fields.empty()) {
        Logger::get_logger()->error(
            "process_completed_jobs - failed to fetch workflow runtime; validating and repairing SQLite state: "
            "client_id={}, workflow_id={}",
            identity.client_id,
            identity.workflow_id);

        std::vector<WorkflowData> workflows;
        if (!co_await DBFactory::get().get_all_workflows_for_client_async(identity.client_id, workflows)) {
            Logger::get_logger()->error(
                "process_completed_jobs - failed to load SQLite workflows: client_id={}, workflow_id={}",
                identity.client_id,
                identity.workflow_id);
            co_return false;
        }

        const auto workflow_it = std::find_if(
            workflows.begin(),
            workflows.end(),
            [&identity](const WorkflowData& workflow) {
                return workflow.info.workflow_id == identity.workflow_id;
            });
        const bool workflow_failed =
            workflow_it != workflows.end() &&
            workflow_it->status == WorkflowStatus::FAILED;
        const bool workflow_canceled =
            workflow_it != workflows.end() &&
            workflow_it->status == WorkflowStatus::CANCELED;

        if (!workflow_failed && !workflow_canceled) {
            Logger::get_logger()->error(
                "process_completed_jobs - workflow runtime is missing but SQLite workflow is not terminal; "
                "failing workflow: "
                "client_id={}, workflow_id={}",
                identity.client_id,
                identity.workflow_id);
            co_await persist_failed_workflow_after_missing_runtime(identity);
        }

        co_return false;
    }

    if (completion_data.status == JobStatus::COMPLETED) {
        const bool handled = co_await handle_job_completed(completion_data, workflow_fields);
        if (handled) {
            co_return true;
        }

        Logger::get_logger()->error(
            "process_completed_jobs - Failure while updating a completed job due to workflow max_runtime or "
            "internal error: client_id={}, workflow_id={}, job_id={}, status={}",
            identity.client_id,
            identity.workflow_id,
            completion_data.job_id,
            to_string(completion_data.status));

        completion_data.status = JobStatus::FAILED;
        completion_data.error_code = StatusCodes::INTERNAL_DB_FAILURE;
    }

    if (completion_data.status != JobStatus::COMPLETED &&
        completion_data.status != JobStatus::FAILED &&
        completion_data.status != JobStatus::ABORTED) {
        Logger::get_logger()->error(
            "process_completed_jobs - unsupported completion status; treating as internal failure: client_id={}, "
            "workflow_id={}, job_id={}, status={}",
            identity.client_id,
            identity.workflow_id,
            completion_data.job_id,
            to_string(completion_data.status));
        completion_data.status = JobStatus::FAILED;
        completion_data.error_code = StatusCodes::INTERNAL_DB_FAILURE;
    }

    if (completion_data.status == JobStatus::FAILED ||
        completion_data.status == JobStatus::ABORTED) {
        JobRuntimeData job_runtime;
        if (!co_await redis_db->fetch_job_runtime_async(*redis_context_,
                                                        identity,
                                                        completion_data.job_id,
                                                        job_runtime)) {
            Logger::get_logger()->error(
                "process_completed_jobs - failed to fetch job runtime; failing workflow: client_id={}, "
                "workflow_id={}, job_id={}",
                identity.client_id,
                identity.workflow_id,
                completion_data.job_id);
            job_runtime.job_id = completion_data.job_id;
            completion_data.error_code = StatusCodes::INTERNAL_DB_FAILURE;
            co_return co_await handle_job_failed(completion_data, job_runtime, workflow_fields);
        }

        if (completion_data.error_code == StatusCodes::INTERNAL_DB_FAILURE) {
            job_runtime.current_retry_count = job_runtime.max_retries;
        }

        const bool handled = co_await handle_job_failed(completion_data, job_runtime, workflow_fields);
        co_return handled;
    }

    co_return false;
}

boost::asio::awaitable<bool> CompletionHandler::handle_job_completed(
    const JobCompletionData& completion_data,
    const std::unordered_map<std::string, std::string>&)
{
    auto redis_db = RedisDatabaseAsync::get_instance();
    auto& db = DBFactory::get();
    const auto& identity = completion_data.identity;
    const std::string now = now_as_string();

    bool workflow_completed = false;
    std::vector<std::string> ready_job_ids;
    std::string promoted_job_id;
    if (!co_await redis_db->complete_successful_job_runtime_async(
            *redis_context_,
            identity,
            completion_data.job_id,
            now,
            ready_job_ids,
            promoted_job_id,
            workflow_completed)) {
        co_return false;
    }

    if (!co_await db.update_job_status_async(identity.client_id,
                                             identity.workflow_id,
                                             completion_data.job_id,
                                             JobStatus::COMPLETED)) {
        co_return false;
    }

    if (workflow_completed &&
        !co_await db.update_workflow_status_async(identity.client_id,
                                                  identity.workflow_id,
                                                  WorkflowStatus::COMPLETED)) {
        co_return false;
    }

    if (!promoted_job_id.empty() || !ready_job_ids.empty()) {
        std::vector<std::string> queued_jobs;
        if (!promoted_job_id.empty()) {
            queued_jobs.push_back(promoted_job_id);
        }
        if (!co_await db.update_ready_jobs_async(identity.client_id,
                                                 identity.workflow_id,
                                                 queued_jobs,
                                                 ready_job_ids)) {
            co_return false;
        }
    }

    if (workflow_completed && !co_await cleanup_completed_workflow_redis_entries(identity)) {
        co_return false;
    }

    co_return true;
}

boost::asio::awaitable<bool> CompletionHandler::cleanup_completed_workflow_redis_entries(
    const WorkflowIdentity& identity)
{
    auto redis_db = RedisDatabaseAsync::get_instance();
    auto& db = DBFactory::get();

    std::vector<WorkflowJob> workflow_jobs;
    if (!co_await db.get_all_jobs_for_workflow_async(identity.client_id,
                                                     identity.workflow_id,
                                                     workflow_jobs)) {
        co_return false;
    }

    WorkflowJobsList redis_jobs;
    std::vector<std::string> job_ids;
    redis_jobs.reserve(workflow_jobs.size());
    job_ids.reserve(workflow_jobs.size());

    for (const auto& job : workflow_jobs) {
        JobRuntimeData redis_job;
        redis_job.job_id = job.job_id;
        redis_jobs.push_back(std::move(redis_job));
        job_ids.push_back(job.job_id);
    }

    if (!co_await redis_db->remove_active_workflow_async(identity.client_id, identity.workflow_id)) {
        co_return false;
    }

    if (!co_await redis_db->delete_all_jobs_payload_async(identity, job_ids)) {
        co_return false;
    }

    WorkflowRuntimeInfo cleanup_info;
    cleanup_info.identity = identity;
    cleanup_info.jobs = std::move(redis_jobs);
    if (!co_await redis_db->delete_workflow_runtime_data_async(cleanup_info)) {
        co_return false;
    }

    co_return true;
}

boost::asio::awaitable<bool> CompletionHandler::handle_job_failed(
    const JobCompletionData& completion_data,
    const JobRuntimeData& job_runtime,
    const std::unordered_map<std::string, std::string>& workflow_fields)
{
    auto redis_db = RedisDatabaseAsync::get_instance();
    auto& db = DBFactory::get();
    const auto& identity = completion_data.identity;
    const int updated_retry_count = job_runtime.current_retry_count + 1;
    const bool can_retry = completion_data.status == JobStatus::FAILED;
    const JobStatus terminal_job_status =
        completion_data.status == JobStatus::ABORTED ? JobStatus::ABORTED : JobStatus::FAILED;
    const auto workflow_status_it = workflow_fields.find("status");
    const bool workflow_running =
        workflow_status_it != workflow_fields.end() &&
        workflow_status_it->second == std::string(to_string(WorkflowStatus::RUNNING));

    if (can_retry &&
        workflow_running &&
        updated_retry_count <= job_runtime.max_retries &&
        !is_job_runtime_expired(job_runtime)) {
        const bool retried = co_await handle_job_retry(completion_data,
                                                       updated_retry_count,
                                                       job_runtime.max_retries,
                                                       job_runtime.retry_delay_sec);
        co_return retried;
    }

    std::vector<WorkflowJob> workflow_jobs;
    if (!co_await db.get_all_jobs_for_workflow_async(identity.client_id,
                                                     identity.workflow_id,
                                                     workflow_jobs)) {
        Logger::get_logger()->error(
            "handle_job_failed - failed to load workflow jobs; applying conservative SQLite fallback: client_id={}, "
            "workflow_id={}, job_id={}",
            identity.client_id,
            identity.workflow_id,
            completion_data.job_id);
        co_return co_await persist_failed_workflow_after_runtime_failure(identity,
                                                                         completion_data.job_id);
    }

    std::vector<std::string> workflow_job_ids;
    workflow_job_ids.reserve(workflow_jobs.size());
    for (const auto& job : workflow_jobs) {
        workflow_job_ids.push_back(job.job_id);
    }

    std::vector<std::string> canceled_job_ids;
    bool workflow_failed = false;
    bool workflow_completed = false;
    if (!co_await redis_db->complete_failed_job_runtime_async(*redis_context_,
                                                              identity,
                                                              completion_data.job_id,
                                                              updated_retry_count,
                                                              workflow_job_ids,
                                                              canceled_job_ids,
                                                              workflow_failed,
                                                              workflow_completed)) {
        Logger::get_logger()->error(
            "handle_job_failed - Redis runtime failure while failing workflow; applying conservative SQLite "
            "fallback: client_id={}, workflow_id={}, job_id={}",
            identity.client_id,
            identity.workflow_id,
            completion_data.job_id);
        co_return co_await persist_failed_workflow_after_runtime_failure(identity,
                                                                         completion_data.job_id,
                                                                         terminal_job_status);
    }

    bool persisted = true;

    if (terminal_job_status == JobStatus::ABORTED) {
        std::unordered_map<std::string, std::string> canceled_job_updates{
            {"status", std::string(to_string(JobStatus::ABORTED))},
            {"last_update_time", now_as_string()}
        };
        if (!co_await redis_db->update_job_runtime_async(*redis_context_,
                                                         identity,
                                                         completion_data.job_id,
                                                         canceled_job_updates)) {
            Logger::get_logger()->error(
                "handle_job_failed - failed to persist canceled job runtime status: client_id={}, workflow_id={}, "
                "job_id={}",
                identity.client_id,
                identity.workflow_id,
                completion_data.job_id);
            persisted = false;
        }
    }

    if (!co_await db.update_job_status_async(identity.client_id,
                                             identity.workflow_id,
                                             completion_data.job_id,
                                             terminal_job_status)) {
        Logger::get_logger()->error(
            "handle_job_failed - failed to persist terminal job status: client_id={}, workflow_id={}, job_id={}, "
            "status={}",
            identity.client_id,
            identity.workflow_id,
            completion_data.job_id,
            to_string(terminal_job_status));
        persisted = false;
    }

    for (const auto& canceled_job_id : canceled_job_ids) {
        if (!co_await db.update_job_status_async(identity.client_id,
                                                 identity.workflow_id,
                                                 canceled_job_id,
                                                 JobStatus::ABORTED)) {
            Logger::get_logger()->error(
                "handle_job_failed - failed to persist canceled job status: client_id={}, workflow_id={}, job_id={}",
                identity.client_id,
                identity.workflow_id,
                canceled_job_id);
            persisted = false;
        }
    }

    if (workflow_failed &&
        !co_await db.update_workflow_status_async(identity.client_id,
                                                  identity.workflow_id,
                                                  WorkflowStatus::FAILED)) {
        Logger::get_logger()->error(
            "handle_job_failed - failed to persist failed workflow status: client_id={}, workflow_id={}",
            identity.client_id,
            identity.workflow_id);
        persisted = false;
    }

    if (workflow_completed && !co_await cleanup_completed_workflow_redis_entries(identity)) {
        Logger::get_logger()->error(
            "handle_job_failed - failed to clean completed workflow runtime data: client_id={}, workflow_id={}",
            identity.client_id,
            identity.workflow_id);
        persisted = false;
    }

    co_return persisted;
}

boost::asio::awaitable<bool> CompletionHandler::persist_failed_workflow_after_runtime_failure(
    const WorkflowIdentity& identity,
    const std::string& failed_job_id,
    JobStatus terminal_job_status)
{
    auto& db = DBFactory::get();
    bool persisted = true;

    if (!co_await db.update_job_status_async(identity.client_id,
                                             identity.workflow_id,
                                             failed_job_id,
                                             terminal_job_status)) {
        Logger::get_logger()->error(
            "persist_failed_workflow_after_runtime_failure - failed to persist terminal job status: client_id={}, "
            "workflow_id={}, job_id={}, status={}",
            identity.client_id,
            identity.workflow_id,
            failed_job_id,
            to_string(terminal_job_status));
        persisted = false;
    }

    std::vector<WorkflowJob> workflow_jobs;
    if (!co_await db.get_all_jobs_for_workflow_async(identity.client_id,
                                                     identity.workflow_id,
                                                     workflow_jobs)) {
        Logger::get_logger()->error(
            "persist_failed_workflow_after_runtime_failure - failed to load workflow jobs: client_id={}, "
            "workflow_id={}",
            identity.client_id,
            identity.workflow_id);
        persisted = false;
    }

    for (const auto& job : workflow_jobs) {
        if (job.job_id == failed_job_id) {
            continue;
        }

        JobStatus target_status = JobStatus::UNKNOWN;
        if (job.status == JobStatus::PENDING ||
            job.status == JobStatus::RETRY_DELAY ||
            job.status == JobStatus::READY ||
            job.status == JobStatus::QUEUED) {
            target_status = JobStatus::ABORTED;
        } else if (job.status == JobStatus::RUNNING) {
            target_status = JobStatus::FAILED;
        } else {
            continue;
        }

        if (!co_await db.update_job_status_async(identity.client_id,
                                                 identity.workflow_id,
                                                 job.job_id,
                                                 target_status)) {
            Logger::get_logger()->error(
                "persist_failed_workflow_after_runtime_failure - failed to persist job status: client_id={}, "
                "workflow_id={}, job_id={}, status={}",
                identity.client_id,
                identity.workflow_id,
                job.job_id,
                static_cast<int>(target_status));
            persisted = false;
        }
    }

    if (!co_await db.update_workflow_status_async(identity.client_id,
                                                  identity.workflow_id,
                                                  WorkflowStatus::FAILED)) {
        Logger::get_logger()->error(
            "persist_failed_workflow_after_runtime_failure - failed to persist failed workflow status: client_id={}, "
            "workflow_id={}",
            identity.client_id,
            identity.workflow_id);
        persisted = false;
    }

    co_return persisted;
}

boost::asio::awaitable<bool> CompletionHandler::persist_failed_workflow_after_missing_runtime(
    const WorkflowIdentity& identity)
{
    auto& db = DBFactory::get();
    bool persisted = true;

    std::vector<WorkflowJob> workflow_jobs;
    if (!co_await db.get_all_jobs_for_workflow_async(identity.client_id,
                                                     identity.workflow_id,
                                                     workflow_jobs)) {
        Logger::get_logger()->error(
            "persist_failed_workflow_after_missing_runtime - failed to load workflow jobs: client_id={}, "
            "workflow_id={}",
            identity.client_id,
            identity.workflow_id);
        persisted = false;
    }

    for (const auto& job : workflow_jobs) {
        JobStatus target_status = JobStatus::UNKNOWN;
        if (is_waiting_to_run(job.status)) {
            target_status = JobStatus::ABORTED;
        } else if (job.status == JobStatus::RUNNING) {
            target_status = JobStatus::FAILED;
        } else {
            continue;
        }

        if (!co_await db.update_job_status_async(identity.client_id,
                                                 identity.workflow_id,
                                                 job.job_id,
                                                 target_status)) {
            Logger::get_logger()->error(
                "persist_failed_workflow_after_missing_runtime - failed to persist job status: client_id={}, "
                "workflow_id={}, job_id={}, status={}",
                identity.client_id,
                identity.workflow_id,
                job.job_id,
                to_string(target_status));
            persisted = false;
        }
    }

    if (!co_await db.update_workflow_status_async(identity.client_id,
                                                  identity.workflow_id,
                                                  WorkflowStatus::FAILED)) {
        Logger::get_logger()->error(
            "persist_failed_workflow_after_missing_runtime - failed to persist failed workflow status: client_id={}, "
            "workflow_id={}",
            identity.client_id,
            identity.workflow_id);
        persisted = false;
    }

    co_return persisted;
}

boost::asio::awaitable<bool> CompletionHandler::handle_job_retry(const JobCompletionData& completion_data,
                                                                 int updated_retry_count,
                                                                 int max_retries,
                                                                 int retry_delay_sec)
{
    auto redis_db = RedisDatabaseAsync::get_instance();
    auto& db = DBFactory::get();
    const auto& identity = completion_data.identity;
    const long long retry_at_ms = now_ms() +
                                  (static_cast<long long>(std::max(0, retry_delay_sec)) * 1000) +
                                  retry_jitter_ms();

    std::string promoted_job_id;
    if (!co_await redis_db->schedule_job_retry_async(*redis_context_,
                                                     identity,
                                                     completion_data.job_id,
                                                     updated_retry_count,
                                                     retry_at_ms,
                                                     promoted_job_id)) {
        co_return false;
    }

    if (!co_await db.update_job_status_async(identity.client_id,
                                             identity.workflow_id,
                                             completion_data.job_id,
                                             JobStatus::RETRY_DELAY)) {
        co_return false;
    }

    if (!promoted_job_id.empty()) {
        std::vector<std::string> queued_jobs{promoted_job_id};
        std::vector<std::string> ready_jobs;
        if (!co_await db.update_ready_jobs_async(identity.client_id,
                                                 identity.workflow_id,
                                                 queued_jobs,
                                                 ready_jobs)) {
            co_return false;
        }
    }

    Logger::get_logger()->info(
        "process_completed_jobs - job failed but retry limit not reached: client_id={}, workflow_id={}, job_id={}, "
        "retry_count={}, max_retries={}, retry_at_ms={}",
        identity.client_id,
        identity.workflow_id,
        completion_data.job_id,
        updated_retry_count,
        max_retries,
        retry_at_ms);
    co_return true;
}

boost::asio::awaitable<bool> CompletionHandler::handle_job_canceled(
    const JobCompletionData& completion_data,
    const std::unordered_map<std::string, std::string>& workflow_fields)
{
    auto redis_db = RedisDatabaseAsync::get_instance();
    auto& db = DBFactory::get();
    const auto& identity = completion_data.identity;
    const std::string now = now_as_string();
    const int reserved_slots = parse_int_field(workflow_fields, "reserved_execution_slots");

    std::unordered_map<std::string, std::string> canceled_job_updates{
        {"status", std::string(to_string(JobStatus::ABORTED))},
        {"last_update_time", now}
    };
    if (!co_await redis_db->update_job_runtime_async(*redis_context_,
                                                     identity,
                                                     completion_data.job_id,
                                                     canceled_job_updates)) {
        co_return false;
    }

    std::unordered_map<std::string, std::string> canceled_workflow_updates{
        {"reserved_execution_slots", std::to_string(std::max(0, reserved_slots - 1))},
        {"last_update_time", now}
    };
    if (!co_await redis_db->update_workflow_runtime_async(*redis_context_,
                                                         identity,
                                                         canceled_workflow_updates)) {
        co_return false;
    }

    const bool job_canceled = co_await db.update_job_status_async(identity.client_id,
                                                                 identity.workflow_id,
                                                                 completion_data.job_id,
                                                                 JobStatus::ABORTED);
    co_return job_canceled;
}

} // namespace flow_pilot
