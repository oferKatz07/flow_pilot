// flow_pilot_runtime.cpp

#include "flow_pilot_runtime.h"

#include <algorithm>

namespace flow_pilot {

std::atomic<FlowPilotRuntime*> FlowPilotRuntime::active_runtime_{nullptr};

FlowPilotRuntime::FlowPilotRuntime(boost::asio::io_context& redis_ioc,
                                   std::size_t redis_io_threads,
                                   std::size_t scheduler_count,
                                   std::size_t completion_handler_count)
    : redis_ioc_(redis_ioc),
      redis_io_thread_count_(std::max<std::size_t>(1, redis_io_threads)),
      initial_scheduler_count_(std::max<std::size_t>(1, scheduler_count)),
      initial_completion_handler_count_(std::max<std::size_t>(1, completion_handler_count))
{
    active_runtime_.store(this, std::memory_order_release);
}

FlowPilotRuntime::~FlowPilotRuntime()
{
    shutdown();

    FlowPilotRuntime* expected = this;
    active_runtime_.compare_exchange_strong(
        expected,
        nullptr,
        std::memory_order_acq_rel,
        std::memory_order_acquire);
}

FlowPilotRuntime* FlowPilotRuntime::active_runtime()
{
    return active_runtime_.load(std::memory_order_acquire);
}

void FlowPilotRuntime::start()
{
    std::lock_guard<std::mutex> lock(mutex_);
    if (started_) {
        return;
    }

    completion_handlers_.reserve(initial_completion_handler_count_);
    for (std::size_t i = 0; i < initial_completion_handler_count_; ++i) {
        completion_handlers_.emplace_back(std::make_unique<CompletionHandler>());
    }

    schedulers_.reserve(initial_scheduler_count_);
    for (std::size_t i = 0; i < initial_scheduler_count_; ++i) {
        schedulers_.emplace_back(std::make_unique<scheduler>());
    }

    redis_io_threads_.reserve(redis_io_thread_count_);
    for (std::size_t i = 0; i < redis_io_thread_count_; ++i) {
        redis_io_threads_.emplace_back([this]() {
            redis_ioc_.run();
        });
    }

    started_ = true;
}

void FlowPilotRuntime::shutdown()
{
    std::vector<std::unique_ptr<scheduler>> schedulers_to_destroy;
    std::vector<std::unique_ptr<CompletionHandler>> completion_handlers_to_destroy;
    std::vector<std::thread> io_threads_to_join;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (shutting_down_) {
            return;
        }
        shutting_down_ = true;

        for (auto& workflow_scheduler : schedulers_) {
            workflow_scheduler->request_stop();
        }
        for (auto& completion_handler : completion_handlers_) {
            completion_handler->request_stop();
        }

        schedulers_to_destroy = std::move(schedulers_);
        completion_handlers_to_destroy = std::move(completion_handlers_);
    }

    schedulers_to_destroy.clear();
    completion_handlers_to_destroy.clear();
    redis_ioc_.stop();

    {
        std::lock_guard<std::mutex> lock(mutex_);
        io_threads_to_join = std::move(redis_io_threads_);
        started_ = false;
    }

    for (auto& io_thread : io_threads_to_join) {
        if (io_thread.joinable()) {
            io_thread.join();
        }
    }
}

void FlowPilotRuntime::add_scheduler()
{
    std::lock_guard<std::mutex> lock(mutex_);
    if (shutting_down_) {
        return;
    }
    schedulers_.emplace_back(std::make_unique<scheduler>());
}

bool FlowPilotRuntime::remove_scheduler()
{
    std::unique_ptr<scheduler> removed_scheduler;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (schedulers_.empty()) {
            return false;
        }

        removed_scheduler = std::move(schedulers_.back());
        schedulers_.pop_back();
        removed_scheduler->request_stop();
    }

    removed_scheduler.reset();
    return true;
}

void FlowPilotRuntime::add_completion_handler()
{
    std::lock_guard<std::mutex> lock(mutex_);
    if (shutting_down_) {
        return;
    }
    completion_handlers_.emplace_back(std::make_unique<CompletionHandler>());
}

bool FlowPilotRuntime::remove_completion_handler()
{
    std::unique_ptr<CompletionHandler> removed_handler;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (completion_handlers_.empty()) {
            return false;
        }

        removed_handler = std::move(completion_handlers_.back());
        completion_handlers_.pop_back();
        removed_handler->request_stop();
    }

    removed_handler.reset();
    return true;
}

FlowPilotRuntimeStatus FlowPilotRuntime::status() const
{
    std::lock_guard<std::mutex> lock(mutex_);
    return FlowPilotRuntimeStatus{
        redis_io_threads_.size(),
        schedulers_.size(),
        completion_handlers_.size(),
        started_,
        shutting_down_
    };
}

std::size_t FlowPilotRuntime::scheduler_count() const
{
    return status().scheduler_count;
}

std::size_t FlowPilotRuntime::completion_handler_count() const
{
    return status().completion_handler_count;
}

} // namespace flow_pilot
