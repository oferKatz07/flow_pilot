// flow_pilot_runtime.h - Runtime ownership for FlowPilot worker components

#pragma once

#include <atomic>
#include <boost/asio/io_context.hpp>
#include <cstddef>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

#include "completion_handler.h"
#include "retry_handler.h"
#include "scheduler.h"

namespace flow_pilot {

struct FlowPilotRuntimeStatus {
    std::size_t redis_io_threads{0};
    std::size_t scheduler_count{0};
    std::size_t completion_handler_count{0};
    std::size_t retry_handler_count{0};
    bool started{false};
    bool shutting_down{false};
};

class FlowPilotRuntime {
public:
    FlowPilotRuntime(boost::asio::io_context& redis_ioc,
                     std::size_t redis_io_threads,
                     std::size_t scheduler_count,
                     std::size_t completion_handler_count);
    ~FlowPilotRuntime();

    FlowPilotRuntime(const FlowPilotRuntime&) = delete;
    FlowPilotRuntime& operator=(const FlowPilotRuntime&) = delete;

    static FlowPilotRuntime* active_runtime();

    void start();
    void shutdown();

    void add_scheduler();
    bool remove_scheduler();
    void add_completion_handler();
    bool remove_completion_handler();

    FlowPilotRuntimeStatus status() const;
    std::size_t scheduler_count() const;
    std::size_t completion_handler_count() const;

private:
    boost::asio::io_context& redis_ioc_;
    const std::size_t redis_io_thread_count_;
    const std::size_t initial_scheduler_count_;
    const std::size_t initial_completion_handler_count_;

    mutable std::mutex mutex_;
    std::vector<std::unique_ptr<scheduler>> schedulers_;
    std::vector<std::unique_ptr<CompletionHandler>> completion_handlers_;
    std::unique_ptr<RetryHandler> retry_handler_;
    std::vector<std::thread> redis_io_threads_;
    bool started_{false};
    bool shutting_down_{false};

    static std::atomic<FlowPilotRuntime*> active_runtime_;
};

} // namespace flow_pilot
