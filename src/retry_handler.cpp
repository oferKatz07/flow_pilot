// retry_handler.cpp

#include "retry_handler.h"

#include "db_factory.h"
#include "logger.h"

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <chrono>
#include <ctime>
#include <string>
#include <vector>

namespace flow_pilot {

namespace {

constexpr auto JOB_RETRY_WAIT_TIMEOUT = std::chrono::seconds(1);

long long now_ms()
{
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
}

} // namespace

RetryHandler::RetryHandler(bool auto_start)
    : redis_context_(std::make_unique<RedisConnectionContext>(RedisDatabaseAsync::get_instance()->io_context()))
{
    if (auto_start) {
        start();
    } else {
        running_.store(true, std::memory_order_release);
    }
}

RetryHandler::~RetryHandler()
{
    request_stop();
    wait_for_main_loop_finished();
}

void RetryHandler::start()
{
    running_.store(true, std::memory_order_release);
    main_loop_started_.store(true, std::memory_order_release);

    auto redis_db = RedisDatabaseAsync::get_instance();
    boost::asio::co_spawn(
        redis_db->io_context(),
        [this]() -> boost::asio::awaitable<void> {
            try {
                co_await retry_handler_main_loop();
            } catch (const std::exception& e) {
                Logger::get_logger()->error("Retry handler main loop failed: {}", e.what());
            } catch (...) {
                Logger::get_logger()->error("Retry handler main loop failed with an unknown exception");
            }
            mark_main_loop_finished();
            co_return;
        },
        boost::asio::detached);
}

void RetryHandler::request_stop()
{
    bool was_running = true;
    if (!running_.compare_exchange_strong(was_running, false, std::memory_order_acq_rel)) {
        return;
    }
    redis_context_->close();
}

void RetryHandler::mark_main_loop_finished()
{
    {
        std::lock_guard<std::mutex> lock(main_loop_finished_mutex_);
        main_loop_finished_ = true;
    }
    main_loop_finished_cv_.notify_all();
}

void RetryHandler::wait_for_main_loop_finished()
{
    if (!main_loop_started_.load(std::memory_order_acquire)) {
        return;
    }

    std::unique_lock<std::mutex> lock(main_loop_finished_mutex_);
    main_loop_finished_cv_.wait(lock, [this]() {
        return main_loop_finished_;
    });
}

boost::asio::awaitable<void> RetryHandler::retry_handler_main_loop()
{
    while (running_.load(std::memory_order_acquire)) {
        JobRetryData retry_data;
        if (!co_await get_retry_job(retry_data)) {
            if (!running_.load(std::memory_order_acquire)) {
                break;
            }
            continue;
        }

        Logger::get_logger()->info(
            "Processing retry job: client_id={}, workflow_id={}, job_id={}",
            retry_data.identity.client_id,
            retry_data.identity.workflow_id,
            retry_data.job_id);
        if (!co_await process_retry_job(retry_data)) {
            Logger::get_logger()->error(
                "Failed to process retry job: client_id={}, workflow_id={}, job_id={}",
                retry_data.identity.client_id,
                retry_data.identity.workflow_id,
                retry_data.job_id);
        }
    }

    Logger::get_logger()->info("Retry handler main loop has exited.");
}

boost::asio::awaitable<bool> RetryHandler::get_retry_job(JobRetryData& retry_data)
{
    auto redis_db = RedisDatabaseAsync::get_instance();

    while (running_.load(std::memory_order_acquire)) {
        if (!co_await redis_db->dequeue_due_job_retry_async(*redis_context_, now_ms(), retry_data)) {
            co_return false;
        }

        if (!retry_data.job_id.empty()) {
            co_return true;
        }

        const auto wait_result = co_await redis_db->wait_for_job_retry_event_async(
            *redis_context_,
            JOB_RETRY_WAIT_TIMEOUT);
        if (wait_result == JobCompletionWaitResult::TIMEOUT) {
            continue;
        }
        if (wait_result == JobCompletionWaitResult::ERROR) {
            Logger::get_logger()->error("Failed while waiting for job retry event");
            co_return false;
        }
    }

    co_return false;
}

boost::asio::awaitable<bool> RetryHandler::process_retry_job(const JobRetryData& retry_data)
{
    auto redis_db = RedisDatabaseAsync::get_instance();
    auto& db = DBFactory::get();

    std::string promoted_job_id;
    if (!co_await redis_db->promote_retry_job_async(
            *redis_context_,
            retry_data.identity,
            retry_data.job_id,
            promoted_job_id)) {
        co_return false;
    }

    std::vector<std::string> queued_jobs;
    std::vector<std::string> ready_jobs;
    if (!promoted_job_id.empty()) {
        queued_jobs.push_back(promoted_job_id);
    }
    if (promoted_job_id != retry_data.job_id) {
        ready_jobs.push_back(retry_data.job_id);
    }

    if (!queued_jobs.empty() || !ready_jobs.empty()) {
        co_return co_await db.update_ready_jobs_async(
            retry_data.identity.client_id,
            retry_data.identity.workflow_id,
            queued_jobs,
            ready_jobs);
    }

    co_return true;
}

} // namespace flow_pilot
