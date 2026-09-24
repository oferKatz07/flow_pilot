#include <gtest/gtest.h>

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/use_future.hpp>

#include <atomic>
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
    boost::asio::awaitable<bool> update_job_status_to_running(const JobExeData&) override {
        co_return start_transition_succeeds_;
    }

    bool execute_job(const JobExeData& job_exe_data) override {
        if (!job_exe_data.job_id.empty()) {
            executed_jobs_.fetch_add(1);
        }
        return true;
    }

    void update_completion_handler(const JobCompletionData&) override {
        completed_jobs_.fetch_add(1);
    }

private:
    std::atomic<int> executed_jobs_{0};
    std::atomic<int> completed_jobs_{0};
    bool start_transition_succeeds_;
};

void run_worker_loop(WorkerThread& worker) {
    boost::asio::io_context ioc;
    auto future = boost::asio::co_spawn(ioc, worker.main_worker_loop(), boost::asio::use_future);
    ioc.run();
    future.get();
}

} // namespace

TEST(WorkerThreadTest, MainLoopFetchesAndExecutesUntilFetcherStops)
{
    std::vector<JobExeData> jobs(2);
    jobs[0].job_id = "job-1";
    jobs[1].job_id = "job-2";

    FixedJobFetcher fetcher(std::move(jobs));
    CountingWorkerThread worker(fetcher);

    run_worker_loop(worker);

    EXPECT_EQ(worker.executed_jobs(), 2);
}

TEST(WorkerThreadTest, MainLoopDoesNotExecuteJobsWhenStartTransitionFails)
{
    std::vector<JobExeData> jobs(1);
    jobs[0].job_id = "job-1";

    FixedJobFetcher fetcher(std::move(jobs));
    CountingWorkerThread worker(fetcher, false);

    run_worker_loop(worker);

    EXPECT_EQ(worker.executed_jobs(), 0);
}
