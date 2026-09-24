
// worker_thread.h 

#pragma once

#include <boost/asio/awaitable.hpp>
#include <string>
#include <vector>

#include "database_models.h"
#include "redis_db_async.h"

namespace flow_pilot {


struct JobExeData {
    WorkflowIdentity identity;
    std::string job_id;
    std::string job_name;
    size_t payload_size_bytes;
    std::vector<uint8_t> payload;
    JobStatus status;
};

struct JobCompletionData;

class IJobFetcher {
public:
    virtual ~IJobFetcher() = default;
    virtual bool fetch_job(JobExeData& job_exe_data) = 0;
};

class WorkerThread {
public:
    explicit WorkerThread(IJobFetcher& job_fetcher);
    virtual ~WorkerThread() = default;
    // The worker thread main loop that executes jobs
    boost::asio::awaitable<void> main_worker_loop();
    void run_worker_loop();

protected:
    virtual boost::asio::awaitable<bool> update_job_status_to_running(const JobExeData& job_exe_data);
    virtual bool execute_job(const JobExeData& job_exe_data);
    virtual void update_completion_handler(const JobCompletionData& completion_data);

private:
    IJobFetcher& job_fetcher_;
};

} // namespace flow_pilot
