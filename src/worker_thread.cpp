// worker_thread.cpp


#include "logger.h"
#include "db_factory.h"
#include "redis_db_async.h"
#include "worker_thread.h"

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/use_future.hpp>

namespace flow_pilot {

WorkerThread::WorkerThread(IJobFetcher& job_fetcher)
    : job_fetcher_(job_fetcher) {}

void WorkerThread::main_worker_loop() {
    while (true) {
        JobExeData job_exe_data;
        if (!job_fetcher_.fetch_job(job_exe_data)) {
            // No more jobs to process, exit the loop
            break;
        }

        if (!update_job_status_to_running_sync(job_exe_data)) {
            // Failed to update job status to RUNNING, handle accordingly (e.g., log the error)
            job_exe_data.status = JobStatus::CANCELED;
            continue;
        }

        if (!execute_job(job_exe_data)) {
            // Job execution failed, handle accordingly (e.g., log the error)
            continue;
        }
    }
}

bool WorkerThread::update_job_status_to_running_sync(const JobExeData& job_exe_data) {
    auto redis_db = RedisDatabaseAsync::get_instance();
    auto future = boost::asio::co_spawn(
        redis_db->io_context(),
        update_job_status_to_running(job_exe_data),
        boost::asio::use_future);

    return future.get();
}

boost::asio::awaitable<bool> WorkerThread::update_job_status_to_running(const JobExeData& job_exe_data) {
    auto redis_db = RedisDatabaseAsync::get_instance();
    StartJobResult result;
    if (!co_await redis_db->try_set_job_to_running_async(job_exe_data.identity, 
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

bool WorkerThread::execute_job(const JobExeData& job_exe_data) {
    Logger::get_logger()->info("Executing job: {} for workflow: {}", 
                               job_exe_data.job_id, job_exe_data.identity.workflow_id);
    sleep(1); // Simulate job execution time
    return true;
}

} // namespace flow_pilot
