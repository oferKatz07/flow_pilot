// retry_handler.h

#pragma once

#include <atomic>
#include <boost/asio/awaitable.hpp>
#include <condition_variable>
#include <memory>
#include <mutex>

#include "redis_db_async.h"

namespace flow_pilot {

class IRetryHandler {
public:
    virtual ~IRetryHandler() = default;
    virtual boost::asio::awaitable<void> retry_handler_main_loop() = 0;
    virtual boost::asio::awaitable<bool> get_retry_job(JobRetryData& retry_data) = 0;
    virtual boost::asio::awaitable<bool> process_retry_job(const JobRetryData& retry_data) = 0;
};

class RetryHandler : public IRetryHandler {
public:
    explicit RetryHandler(bool auto_start = true);
    ~RetryHandler() override;

    boost::asio::awaitable<void> retry_handler_main_loop() override;
    boost::asio::awaitable<bool> get_retry_job(JobRetryData& retry_data) override;
    boost::asio::awaitable<bool> process_retry_job(const JobRetryData& retry_data) override;

    void request_stop();

private:
    void start();
    void mark_main_loop_finished();
    void wait_for_main_loop_finished();

    std::unique_ptr<RedisConnectionContext> redis_context_;
    std::atomic<bool> running_{false};
    std::atomic<bool> main_loop_started_{false};
    bool main_loop_finished_{false};
    std::mutex main_loop_finished_mutex_;
    std::condition_variable main_loop_finished_cv_;
};

} // namespace flow_pilot
