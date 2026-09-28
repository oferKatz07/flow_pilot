// scheduler.h 

#pragma once

#include <boost/asio/awaitable.hpp>

#include <atomic>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "thread_safe_blocking_queue.h"
#include "worker_thread.h"

namespace flow_pilot {

constexpr std::size_t DEFAULT_WORKERS_NUM = 10;

class scheduler : public IJobFetcher {
public:
    explicit scheduler(std::size_t worker_thread_count = DEFAULT_WORKERS_NUM,
                       std::size_t worker_queue_capacity = DEFAULT_WORKERS_NUM);
    ~scheduler();

    scheduler(const scheduler&) = delete; // Don't allow for a copy constructor
    scheduler& operator=(const scheduler&) = delete; // Don't allow assignment operator
    boost::asio::awaitable<void> scheduler_main_loop();
    void request_stop();
    bool fetch_job(JobExeData& job_exe_data) override;

private:
    void start();

    void mark_main_loop_finished();
    void wait_for_main_loop_finished();
    void mark_worker_loop_finished();
    void wait_for_worker_loops_finished();
    boost::asio::awaitable<bool> get_next_ready_job(JobExeData& ready_job_info);

    const std::string scheduler_uuid_;
    const std::string scheduler_redis_connection_id_;
    std::unique_ptr<RedisConnectionContext> redis_context_;
    ThreadSafeBlockingQueue<JobExeData> worker_thread_queue_;
    std::vector<std::unique_ptr<WorkerThread>> workers_;
    std::vector<std::thread> worker_threads_;
    std::atomic<bool> running_;
    std::atomic<bool> main_loop_started_{false};
    bool main_loop_finished_{false};
    std::mutex main_loop_finished_mutex_;
    std::condition_variable main_loop_finished_cv_;
    std::size_t running_worker_loops_{0};
    std::mutex worker_loops_finished_mutex_;
    std::condition_variable worker_loops_finished_cv_;
};

} // namespace flow_pilot
