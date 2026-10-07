// scheduler.cpp 

#include "scheduler.h"

#include "flow_pilot_error_msgs.h"
#include "logger.h"
#include "redis_db_async.h"

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/uuid/random_generator.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <chrono>
#include <exception>

namespace flow_pilot {


scheduler::scheduler(std::size_t worker_thread_count, std::size_t worker_queue_capacity) :
    scheduler_uuid_(boost::uuids::to_string(boost::uuids::random_generator()())),
    scheduler_redis_connection_id_(scheduler_uuid_ + "-scheduler"),
    redis_context_(std::make_unique<RedisConnectionContext>(RedisDatabaseAsync::get_instance()->io_context())),
    worker_thread_queue_(worker_queue_capacity == 0 ? 1 : worker_queue_capacity),
    running_(true)
{
    const auto thread_count = worker_thread_count == 0 ? 1 : worker_thread_count;
    workers_.reserve(thread_count);
    for (std::size_t i = 0; i < thread_count; ++i) {
        workers_.emplace_back(std::make_unique<WorkerThread>(*this));
    }

    {
        std::lock_guard<std::mutex> lock(worker_loops_finished_mutex_);
        running_worker_loops_ = workers_.size();
    }

    worker_threads_.reserve(workers_.size());
    for (auto& worker : workers_) {
        WorkerThread* worker_ptr = worker.get();
        worker_threads_.emplace_back([this, worker_ptr]() {
            try {
                worker_ptr->run_worker_loop();
            } catch (const std::exception& e) {
                Logger::get_logger()->error("Worker loop failed: {}", e.what());
            } catch (...) {
                Logger::get_logger()->error("Worker loop failed with an unknown exception");
            }
            mark_worker_loop_finished();
        });
    }

    start();
}

scheduler::~scheduler() {
    request_stop();
    wait_for_main_loop_finished();

    for (auto& worker_thread : worker_threads_) {
        if (worker_thread.joinable()) {
            worker_thread.join();
        }
    }

    wait_for_worker_loops_finished();
}

boost::asio::awaitable<void> scheduler::scheduler_main_loop() {
    auto redis_db = RedisDatabaseAsync::get_instance();

    while (running_.load(std::memory_order_acquire)) {
        JobExeData job_info;
        // Wait for the next ready job from the redis execution queue
        if (!co_await get_next_ready_job(job_info)) {
            if (!running_.load(std::memory_order_acquire)) {
                break;
            }
            
            continue;
        }

        if (job_info.payload_size_bytes > 0) {
            const bool payload_fetched = co_await redis_db->fetch_job_payload_async(
                *redis_context_,
                job_info.identity,
                job_info.job_id,
                job_info.payload);
            if (!payload_fetched || job_info.payload.size() != job_info.payload_size_bytes) {
                Logger::get_logger()->error(
                    "scheduler_main_loop - failed to fetch valid payload for job: client_id={}, workflow_id={}, "
                    "job_id={}, expected_payload_size={}, actual_payload_size={}",
                    job_info.identity.client_id,
                    job_info.identity.workflow_id,
                    job_info.job_id,
                    job_info.payload_size_bytes,
                    job_info.payload.size());

                JobCompletionData completion_data;
                completion_data.identity = job_info.identity;
                completion_data.job_id = job_info.job_id;
                completion_data.status = JobStatus::ABORTED;
                completion_data.error_code = StatusCodes::INTERNAL_DB_FAILURE;
                if (!co_await redis_db->enqueue_job_completion_async(*redis_context_, completion_data)) {
                    Logger::get_logger()->error(
                        "scheduler_main_loop - failed to enqueue payload failure completion for job: client_id={}, "
                        "workflow_id={}, job_id={}",
                        job_info.identity.client_id,
                        job_info.identity.workflow_id,
                        job_info.job_id);
                }
                continue;
            }
        } else {
            job_info.payload.clear();
        }

        if (!worker_thread_queue_.push(std::move(job_info))) {
            break;
        }
    }

    mark_main_loop_finished();
}

void scheduler::request_stop() {
    bool was_running = true;
    if (!running_.compare_exchange_strong(was_running, false, std::memory_order_acq_rel)) {
        return;
    }

    // Wake workers blocked on an empty/full queue and the dispatcher blocked in Redis.
    worker_thread_queue_.close();
    redis_context_->close();
}

bool scheduler::fetch_job(JobExeData& job_exe_data) {
    return worker_thread_queue_.pop(job_exe_data);
}

////////////////////////////////////////////////////////////////////////////////////
//                                 Private Methods                                //
////////////////////////////////////////////////////////////////////////////////////
void scheduler::start() {
    main_loop_started_.store(true, std::memory_order_release);

    auto redis_db = RedisDatabaseAsync::get_instance();
    auto& ioc = redis_db->io_context();

    boost::asio::co_spawn(
        ioc,
        [this]() -> boost::asio::awaitable<void> {
            try {
                co_await scheduler_main_loop();
            } catch (const std::exception& e) {
                Logger::get_logger()->error("Scheduler main loop failed: {}", e.what());
                mark_main_loop_finished();
            } catch (...) {
                Logger::get_logger()->error("Scheduler main loop failed with an unknown exception");
                mark_main_loop_finished();
            }
            co_return;
        },
        boost::asio::detached);
}

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

void scheduler::mark_worker_loop_finished() {
    {
        std::lock_guard<std::mutex> lock(worker_loops_finished_mutex_);
        if (running_worker_loops_ > 0) {
            --running_worker_loops_;
        }
    }
    worker_loops_finished_cv_.notify_all();
}

void scheduler::wait_for_worker_loops_finished() {
    std::unique_lock<std::mutex> lock(worker_loops_finished_mutex_);
    worker_loops_finished_cv_.wait(lock, [this]() {
        return running_worker_loops_ == 0;
    });
}

boost::asio::awaitable<bool> scheduler::get_next_ready_job(JobExeData& job_info) {
    auto redis_db = RedisDatabaseAsync::get_instance();
    while (running_.load(std::memory_order_acquire)) {
        if (co_await redis_db->blocking_dequeue_job_for_execution_async(*redis_context_, job_info.identity, job_info.job_id, scheduler_redis_connection_id_)) {
            JobRuntimeData job_runtime;
            if (!co_await redis_db->fetch_job_runtime_async(*redis_context_,
                                                            job_info.identity,
                                                            job_info.job_id,
                                                            job_runtime)) {
                co_return false;
            }
            job_info.job_name = job_runtime.job_name;
            job_info.payload_size_bytes = job_runtime.payload_size_bytes > 0
                ? static_cast<size_t>(job_runtime.payload_size_bytes)
                : 0;
            co_return true;
        }

        if (!running_.load(std::memory_order_acquire)) {
            // If the scheduler is stopping, exit the loop and return false
            co_return false;
        }
    }

    co_return false;
}

} // namespace flow_pilot
