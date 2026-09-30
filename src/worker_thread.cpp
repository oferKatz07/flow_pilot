// worker_thread.cpp


#include "logger.h"
#include "completion_handler.h"
#include "db_factory.h"
#include "redis_db_async.h"
#include "worker_thread.h"

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/use_future.hpp>

namespace flow_pilot {

WorkerThread::WorkerThread(IJobFetcher& job_fetcher)
    : job_fetcher_(job_fetcher) {}

void WorkerThread::run_worker_loop() {
    while (true) {
        JobExeData job_exe_data;
        if (!job_fetcher_.fetch_job(job_exe_data)) {
            break;
        }

        JobCompletionData completion_data;
        completion_data.identity = job_exe_data.identity;
        completion_data.job_id = job_exe_data.job_id;

        auto start_transition = boost::asio::co_spawn(
            redis_io_context(),
            update_job_status_to_running(job_exe_data),
            boost::asio::use_future);

        if (!start_transition.get()) {
            completion_data.status = JobStatus::CANCELED;
            completion_data.error_code = StatusCodes::STATUS_UPDATED_FAILURE;
            update_completion_handler(completion_data);
            continue;
        }

        if (!execute_job(job_exe_data)) {
            completion_data.status = JobStatus::FAILED;
            completion_data.error_code = StatusCodes::JOB_EXECUTION_FAILURE;
        } else {
            completion_data.status = JobStatus::COMPLETED;
            completion_data.error_code = StatusCodes::OK;
        }

        update_completion_handler(completion_data);
    }
}

boost::asio::awaitable<bool> WorkerThread::update_job_status_to_running(const JobExeData& job_exe_data) {
    auto redis_db = RedisDatabaseAsync::get_instance();
    StartJobResult result;
    if (!co_await redis_db->try_set_job_to_running_async(redis_context(),
                                                         job_exe_data.identity, 
                                                         job_exe_data.job_id, result)) {
        co_return false;
    }

    if (result == StartJobResult::FIRST_TO_START) {
        if (!co_await DBFactory::get().update_workflow_status_async(
                job_exe_data.identity.client_id,
                job_exe_data.identity.workflow_id,
                WorkflowStatus::RUNNING)) {
            co_return false;
        }
    }


    co_return co_await DBFactory::get().update_job_status_async(
        job_exe_data.identity.client_id,
        job_exe_data.identity.workflow_id,
        job_exe_data.job_id,
        JobStatus::RUNNING);
}

RedisConnectionContext& WorkerThread::redis_context() {
    if (!redis_context_) {
        redis_context_ = std::make_unique<RedisConnectionContext>(RedisDatabaseAsync::get_instance()->io_context());
    }
    return *redis_context_;
}

bool WorkerThread::execute_job(const JobExeData& job_exe_data) {
    Logger::get_logger()->info("Executing job: {} for workflow: {}", 
                               job_exe_data.job_id, job_exe_data.identity.workflow_id);
    sleep(1); // Simulate job execution time
    return true;
}

void WorkerThread::update_completion_handler(const JobCompletionData& completion_data) {
    auto redis_db = RedisDatabaseAsync::get_instance();
    auto completion_update = boost::asio::co_spawn(
        redis_io_context(),
        [this, completion_data, redis_db]() -> boost::asio::awaitable<void> {
            if (!co_await redis_db->enqueue_job_completion_async(redis_context(), completion_data)) {
                Logger::get_logger()->error("Failed to enqueue completion message for job: {} in workflow: {}",
                                             completion_data.job_id, completion_data.identity.workflow_id);
            }
            co_return;
        },
        boost::asio::use_future);
    completion_update.get();
}

boost::asio::io_context& WorkerThread::redis_io_context() {
    return RedisDatabaseAsync::get_instance()->io_context();
}

} // namespace flow_pilot
