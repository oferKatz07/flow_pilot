// completion_handler.h 

#pragma once

#include <boost/asio/awaitable.hpp>
#include <atomic>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

#include "flow_pilot_error_msgs.h"
#include "database_models.h"
#include "redis_db_async.h"

namespace flow_pilot {


class ICompletionHandler {
public:
    virtual ~ICompletionHandler() = default;
    virtual boost::asio::awaitable<void> completion_handler_main_loop() = 0;
    virtual boost::asio::awaitable<bool> get_completed_jobs(JobCompletionData& completion_data) = 0;
    virtual boost::asio::awaitable<bool> process_completed_jobs(JobCompletionData& completion_data) = 0;

};

class CompletionHandler : public ICompletionHandler {
public:
    explicit CompletionHandler(bool auto_start = true);
    virtual ~CompletionHandler();
    boost::asio::awaitable<void> completion_handler_main_loop() override;
    boost::asio::awaitable<bool> get_completed_jobs(JobCompletionData& completion_data) override;
    boost::asio::awaitable<bool> process_completed_jobs(JobCompletionData& completion_data) override;
    void request_stop();

private:
    void start();
    void mark_main_loop_finished();
    void wait_for_main_loop_finished();

    boost::asio::awaitable<bool> handle_job_completed(
        const JobCompletionData& completion_data,
        const std::unordered_map<std::string, std::string>& workflow_fields);
    boost::asio::awaitable<bool> handle_job_failed(
        const JobCompletionData& completion_data,
        const JobRuntimeData& job_runtime,
        const std::unordered_map<std::string, std::string>& workflow_fields);
    boost::asio::awaitable<bool> handle_job_retry(
        const JobCompletionData& completion_data,
        int updated_retry_count,
        int max_retries,
        int retry_delay_sec);
    boost::asio::awaitable<bool> persist_failed_workflow_after_runtime_failure(
        const WorkflowIdentity& identity,
        const std::string& failed_job_id,
        JobStatus terminal_job_status = JobStatus::FAILED);
    boost::asio::awaitable<bool> persist_failed_workflow_after_missing_runtime(
        const WorkflowIdentity& identity);
    boost::asio::awaitable<bool> handle_job_canceled(
        const JobCompletionData& completion_data,
        const std::unordered_map<std::string, std::string>& workflow_fields);
    boost::asio::awaitable<bool> cleanup_completed_workflow_redis_entries(
        const WorkflowIdentity& identity);

    std::unique_ptr<RedisConnectionContext> redis_context_;
    std::atomic<bool> running_{false};
    std::atomic<bool> main_loop_started_{false};
    bool main_loop_finished_{false};
    std::mutex main_loop_finished_mutex_;
    std::condition_variable main_loop_finished_cv_;
};

} // namespace flow_pilot
