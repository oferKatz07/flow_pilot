
// completion_handler.h 

#pragma once

#include <boost/asio/awaitable.hpp>
#include <string>
#include <atomic>
#include <thread>
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

private:
    void start();
    void stop() {
        running_.store(false, std::memory_order_release);
    }

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
        int max_retries);
    boost::asio::awaitable<bool> handle_job_canceled(
        const JobCompletionData& completion_data,
        const std::unordered_map<std::string, std::string>& workflow_fields);

    std::thread completion_handler_thread_;
    std::atomic<bool> running_{false};
};

} // namespace flow_pilot
