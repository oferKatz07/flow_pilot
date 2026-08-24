
// scheduler.h 

#pragma once

#include <string>
#include <list>
#include <vector>
#include <set>
#include <unordered_map>

#include "db_factory.h"
#include "redis_db_async.h"

namespace flow_pilot {

constexpr uint WORKER_NUM = 4;
constexpr uint TOTAL_PENDIG_JOBS = 2 * WORKER_NUM;
// constexpr uint EXE_QUEUE_SIZE = WORKER_NUM + 2;

struct JobExeData {
    WorkflowIdentity identity;
    std::string job_id;
    uint priority;
    uint seq_num;
    std::vector<uint8_t> payload;
};

struct ExeQueueomparator {
    bool operator()(const std::list<JobExeData>::iterator lhs,
                    const std::list<JobExeData>::iterator rhs) const {
        if ((*lhs).priority != (*rhs).priority)
            return (*lhs).priority > (*rhs).priority;

        return (*lhs).seq_num < (*rhs).seq_num;
    }
};

struct WorkflowJobsState {
    std::set<std::list<JobExeData>::iterator, ExeQueueomparator> jobs_for_execution;
};

class scheduler {
public:
    boost::asio::awaitable<void> ready_job_event();
    // JobExeData& get_next_job_for_execution();

private:
    void update_pending_jobs(JobExeData& job_info);
    boost::asio::awaitable<void> update_job_status(const WorkflowIdentity& itentity, const std::string job_id, const JobStatus job_status);
    void creat_workflow_map_key(const WorkflowIdentity& itentity, std::string& workflow_id);

    size_t seq_num = 0;
    size_t total_waiting_jobs = 0;
    std::string scheduler_uuid = "12345";

    std::list<JobExeData> scheduler_pending_jobs;
    std::unordered_map<std::string, WorkflowJobsState> ready_jobs;
    std::set<JobExeData, ExeQueueomparator> execution_queue;
};

} // namespace flow_pilot