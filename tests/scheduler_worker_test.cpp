#include <gtest/gtest.h>

#include <atomic>
#include <thread>
#include <utility>
#include <vector>

#include "worker_thread.h"

using namespace flow_pilot;

namespace {

class FixedJobFetcher : public IJobFetcher {
public:
    explicit FixedJobFetcher(std::vector<JobExeData> jobs)
        : jobs_(std::move(jobs)) {}

    bool fetch_job(JobExeData& job_exe_data) override {
        if (next_job_ >= jobs_.size()) {
            return false;
        }

        job_exe_data = jobs_[next_job_++];
        return true;
    }

private:
    std::vector<JobExeData> jobs_;
    std::size_t next_job_{0};
};

class CountingWorkerThread : public WorkerThread {
public:
    explicit CountingWorkerThread(IJobFetcher& job_fetcher, bool start_transition_succeeds = true)
        : WorkerThread(job_fetcher),
          start_transition_succeeds_(start_transition_succeeds) {}

    int executed_jobs() const {
        return executed_jobs_.load();
    }

protected:
    bool update_job_status_to_running_sync(const JobExeData&) override {
        return start_transition_succeeds_;
    }

    bool execute_job(const JobExeData& job_exe_data) override {
        if (!job_exe_data.job_id.empty()) {
            executed_jobs_.fetch_add(1);
        }
        return true;
    }

private:
    std::atomic<int> executed_jobs_{0};
    bool start_transition_succeeds_;
};

} // namespace

TEST(WorkerThreadTest, MainLoopFetchesAndExecutesUntilFetcherStops)
{
    std::vector<JobExeData> jobs(2);
    jobs[0].job_id = "job-1";
    jobs[1].job_id = "job-2";

    FixedJobFetcher fetcher(std::move(jobs));
    CountingWorkerThread worker(fetcher);

    std::thread worker_thread([&worker]() {
        worker.main_worker_loop();
    });
    worker_thread.join();

    EXPECT_EQ(worker.executed_jobs(), 2);
}

TEST(WorkerThreadTest, MainLoopDoesNotExecuteJobsWhenStartTransitionFails)
{
    std::vector<JobExeData> jobs(1);
    jobs[0].job_id = "job-1";

    FixedJobFetcher fetcher(std::move(jobs));
    CountingWorkerThread worker(fetcher, false);

    std::thread worker_thread([&worker]() {
        worker.main_worker_loop();
    });
    worker_thread.join();

    EXPECT_EQ(worker.executed_jobs(), 0);
}
