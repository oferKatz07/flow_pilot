
// scheduler.cpp 

#include "scheduler.h"

namespace flow_pilot {


boost::asio::awaitable<void> scheduler::ready_job_event() {
    std::string ready_job;
    WorkflowIdentity identity;
    JobRuntimeData ready_job_data;

    if (scheduler_pending_jobs.size() >= TOTAL_PENDIG_JOBS) {
        co_return;
    }

    auto redis_db = RedisDatabaseAsync::get_instance();
    co_await redis_db->dequeue_job_for_execution_async(identity, ready_job, scheduler_uuid);
    // Validate the queue is not empty
    if (ready_job.empty()) {
        co_return;
    }

    // Fetch the job details from redis
    if (!co_await redis_db->fetch_job_runtime_async(identity, ready_job, ready_job_data)) {
        co_return;
    }

    std::string workflow_status;
    bool can_run = co_await redis_db->try_acquire_job_slot_async(identity, workflow_status);
    if (can_run) {
        ;
    } else if (workflow_status == to_string(WorkflowStatus::RUNNING)) {
        // Check if the job can be added to the scheduler pending jobs
        std::string workflow_key;
        creat_workflow_map_key(identity, workflow_key);
        if (ready_jobs[workflow_key].jobs_for_execution.empty()) {
            ;
        }
        ;
    } else {
        // TBD Fail all pending jobs of a failed/canceled workflow

    }

    co_return;
}

// JobExeData& scheduler::get_next_job_for_execution() {
//     JobExeData& job_info = *execution_queue.begin();
//     execution_queue.erase(execution_queue.begin());
//     update_pending_jobs(job_info);
//     update_job_status(job_info.identity, job_info.job_id, JobStatus::RUNNING);

//     return job_info;
// }

////////////////////////////////////////////////////////////////////////////////////
//                                 Private Methods                                //
////////////////////////////////////////////////////////////////////////////////////
void scheduler::update_pending_jobs(JobExeData& job_info) {
    // std::string workflow_id;
    // creat_workflow_map_key(job_info.identity, workflow_id);
    // // Remove the job from the ready list
    // ready_jobs[workflow_id].jobs_for_execution.erase(job_info);
}

boost::asio::awaitable<void> scheduler::update_job_status(const WorkflowIdentity& identity, const std::string job_id, const JobStatus job_status) {
    co_await DBFactory::get().update_workflow_status_async(identity.client_id, identity.workflow_id, WorkflowStatus::RUNNING);
    std::unordered_map<std::string, std::string> fields;
    fields["status"] = to_string(WorkflowStatus::RUNNING);
    co_await RedisDatabaseAsync::get_instance()->update_workflow_runtime_async(identity, fields);
}

void scheduler::creat_workflow_map_key(const WorkflowIdentity& itentity, std::string& workflow_id) {
    workflow_id = itentity.client_id + ":" + itentity.workflow_id;
}

} // namespace flow_pilot