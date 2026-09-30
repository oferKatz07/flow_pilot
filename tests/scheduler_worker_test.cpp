#include <gtest/gtest.h>

#include <boost/asio/executor_work_guard.hpp>
#include <boost/asio/io_context.hpp>

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
          start_transition_succeeds_(start_transition_succeeds),
          work_guard_(boost::asio::make_work_guard(ioc_)),
          io_thread_([this]() { ioc_.run(); }) {}

    ~CountingWorkerThread() override {
        work_guard_.reset();
        ioc_.stop();
        if (io_thread_.joinable()) {
            io_thread_.join();
        }
    }

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

    boost::asio::io_context& redis_io_context() override {
        return ioc_;
    }

private:
    std::atomic<int> executed_jobs_{0};
    std::atomic<int> completed_jobs_{0};
    bool start_transition_succeeds_;
    boost::asio::io_context ioc_;
    boost::asio::executor_work_guard<boost::asio::io_context::executor_type> work_guard_;
    std::thread io_thread_;
};

void run_worker_loop(WorkerThread& worker) {
    worker.run_worker_loop();
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
