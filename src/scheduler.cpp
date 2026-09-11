
// scheduler.cpp 

#include "scheduler.h"

#include <boost/asio/steady_timer.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/uuid/random_generator.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <chrono>

namespace flow_pilot {


scheduler::scheduler(std::size_t worker_thread_count, std::size_t worker_queue_capacity) :
    scheduler_uuid_(boost::uuids::to_string(boost::uuids::random_generator()())),
    worker_thread_queue_(worker_queue_capacity == 0 ? 1 : worker_queue_capacity),
    running_(true)
{
    RedisDatabaseAsync::get_instance()->register_scheduler(scheduler_uuid_);
    const auto thread_count = worker_thread_count == 0 ? 1 : worker_thread_count;
    for (std::size_t i = 0; i < thread_count; ++i) {
        worker_threads_.emplace_back([this]() {
            WorkerThread worker(*this);
            worker.main_worker_loop();
        });
    }
}

scheduler::~scheduler() {
    stop();

    // Wake workers blocked on an empty/full queue before waiting for the dispatcher.
    worker_thread_queue_.close();

    wait_for_main_loop_finished();
    RedisDatabaseAsync::get_instance()->deregister_scheduler(scheduler_uuid_);

    // Join threads to wait for completion
    for (auto& t : worker_threads_) {
        if (t.joinable()) {
            t.join();
        }
    }
}

boost::asio::awaitable<void> scheduler::scheduler_main_loop() {
    auto redis_db = RedisDatabaseAsync::get_instance();
    main_loop_started_.store(true, std::memory_order_release);

    while (running_.load(std::memory_order_acquire)) {
        JobExeData job_info;
        // Waite for the next ready job from the redis execution queue
        if (!co_await get_next_ready_job(job_info)) {
            break;
        }

        if (!running_.load(std::memory_order_acquire)) {
            break;
        }

        // Get the job payload from the workflow runtime data
        co_await redis_db->fetch_job_payload_async(job_info.identity, job_info.job_id, job_info.payload);

        if (!worker_thread_queue_.push(std::move(job_info))) {
            break;
        }
    }

    mark_main_loop_finished();
}

bool scheduler::fetch_job(JobExeData& job_exe_data) {
    return worker_thread_queue_.pop(job_exe_data);
}

////////////////////////////////////////////////////////////////////////////////////
//                                 Private Methods                                //
////////////////////////////////////////////////////////////////////////////////////
void scheduler::mark_main_loop_finished() {
    {
        std::lock_guard<std::mutex> lock(main_loop_finished_mutex_);
        main_loop_finished_ = true;
    }
    main_loop_finished_cv_.notify_all();
}

void scheduler::wait_for_main_loop_finished() {
    if (!main_loop_started_.load(std::memory_order_acquire)) {
        return;
    }

    std::unique_lock<std::mutex> lock(main_loop_finished_mutex_);
    main_loop_finished_cv_.wait(lock, [this]() {
        return main_loop_finished_;
    });
}

boost::asio::awaitable<bool> scheduler::get_next_ready_job(JobExeData& job_info) {
    auto redis_db = RedisDatabaseAsync::get_instance();
    while (running_.load(std::memory_order_acquire)) {
        if (co_await redis_db->blocking_dequeue_job_for_execution_async(job_info.identity, job_info.job_id, scheduler_uuid_)) {
            co_return true;
        }

        if (!running_.load(std::memory_order_acquire)) {
            co_return false;
        }

        // A failed blocking dequeue means this scheduler's dedicated Redis connection may be broken.
        // Drop it, try to create a fresh one
        redis_db->deregister_scheduler(scheduler_uuid_);
        if (!redis_db->register_scheduler(scheduler_uuid_)) {
            // Sleep for 1 second to avoid a tight retry loop if Redis is still unavailable
            auto executor = co_await boost::asio::this_coro::executor;
            boost::asio::steady_timer retry_timer(executor, std::chrono::seconds(1));
            co_await retry_timer.async_wait(boost::asio::use_awaitable);
        }
    }

    co_return false;
}

} // namespace flow_pilot
