#include <gtest/gtest.h>

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/use_future.hpp>
#include <boost/uuid/random_generator.hpp>
#include <chrono>
#include <future>
#include <random>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "completion_handler.h"
#include "config.h"
#include "database_models.h"
#include "db_factory.h"
#include "flow_pilot_error_msgs.h"
#include "redis_db_async.h"
#include "redis_test_utils.h"

using namespace flow_pilot;

namespace {

std::string generate_unique_id()
{
    static std::mt19937_64 rng(static_cast<unsigned long long>(std::chrono::high_resolution_clock::now().time_since_epoch().count()));
    std::uniform_int_distribution<unsigned long long> dist;
    return std::to_string(dist(rng));
}

template <typename T>
T run_async(boost::asio::io_context& ioc, boost::asio::awaitable<T> awaitable)
{
    auto future = boost::asio::co_spawn(ioc, std::move(awaitable), boost::asio::use_future);
    ioc.restart();
    ioc.run();
    return future.get();
}

void clear_completion_stream(boost::asio::io_context& ioc, RedisDatabaseAsync& redis)
{
    long long deleted = 0;
    std::vector<std::string> delete_args{"DEL", "fp:job_completion_stream"};
    ASSERT_TRUE(run_async(ioc, redis.command_executor().execute_integer_command_async(delete_args, deleted)));
}

WorkflowData make_sqlite_workflow(const WorkflowIdentity& identity,
                                  const std::string& request_id,
                                  WorkflowStatus status,
                                  int total_jobs)
{
    WorkflowData workflow{};
    workflow.info.client_id = identity.client_id;
    workflow.info.request_id = request_id;
    workflow.info.workflow_id = identity.workflow_id;
    workflow.info.status = RequestStatus::ADMITTED;
    workflow.workflow_type = "completion-handler-test";
    workflow.workflow_version = "v1";
    workflow.status = status;
    workflow.total_jobs = total_jobs;
    return workflow;
}

WorkflowJobList make_sqlite_jobs(const WorkflowIdentity& identity,
                                 const std::vector<std::pair<std::string, JobStatus>>& jobs)
{
    WorkflowJobList job_list;
    job_list.client_id = identity.client_id;
    job_list.workflow_id = identity.workflow_id;
    job_list.retry_count = 1;

    for (const auto& [job_id, status] : jobs) {
        job_list.jobs.push_back({"uuid-" + job_id, job_id, status});
    }

    return job_list;
}

JobRuntimeData make_redis_job(const std::string& job_id,
                              JobStatus status,
                              int priority = 10,
                              int max_retries = 1,
                              int current_retry_count = 0)
{
    JobRuntimeData job{};
    job.job_uuid = boost::uuids::random_generator()();
    job.job_id = job_id;
    job.job_name = job_id;
    job.status = std::string(to_string(status));
    job.remaining_dependencies = 0;
    job.priority = priority;
    job.timeout_sec = 30;
    job.max_retries = max_retries;
    job.current_retry_count = current_retry_count;
    job.retry_delay_sec = 0;
    job.payload_size_bytes = 0;
    job.retry_backoff_policy = "IMMEDIATE";
    return job;
}

WorkflowRuntimeInfo make_redis_runtime(const WorkflowIdentity& identity,
                                       WorkflowStatus status,
                                       int total_jobs,
                                       int reserved_slots,
                                       WorkflowJobsList jobs)
{
    WorkflowRuntimeInfo runtime;
    runtime.identity = identity;
    runtime.workflow.workflow_id = identity.workflow_id;
    runtime.workflow.status = std::string(to_string(status));
    runtime.workflow.max_concurrent_jobs = 1;
    runtime.workflow.reserved_execution_slots = reserved_slots;
    runtime.workflow.max_runtime_sec = 60;
    runtime.workflow.total_jobs = total_jobs;
    runtime.workflow.pending_jobs = 0;
    runtime.workflow.completed_jobs = 0;
    runtime.workflow.failed_jobs = 0;
    runtime.jobs = std::move(jobs);
    return runtime;
}

void persist_sqlite_workflow(boost::asio::io_context& ioc,
                             const WorkflowIdentity& identity,
                             const std::vector<std::pair<std::string, JobStatus>>& jobs,
                             WorkflowStatus workflow_status = WorkflowStatus::RUNNING)
{
    StatusCodes error_status = StatusCodes::OK;
    auto& db = DBFactory::get();
    ASSERT_TRUE(run_async(ioc, db.add_workflow_async(
        make_sqlite_workflow(identity, "request-" + generate_unique_id(), workflow_status, static_cast<int>(jobs.size())),
        error_status))) << status_code_to_string(error_status);
    ASSERT_TRUE(run_async(ioc, db.add_workflow_jobs_async(
        make_sqlite_jobs(identity, jobs),
        error_status))) << status_code_to_string(error_status);
}

class CompletionHandlerTest : public ::testing::Test {
protected:
    void SetUp() override
    {
        Config::get().logger().output = LogOutput::CONSOLE_ONLY;
        Config::get().redis().host = "127.0.0.1";
        Config::get().redis().port = 6379;

        try {
            redis_ = RedisDatabaseAsync::init(ioc_, Config::get().redis());
        } catch (const std::exception& ex) {
            GTEST_SKIP() << "Redis server is not available: " << ex.what();
        }

        clear_completion_stream(ioc_, *redis_);
        run_async(ioc_, redis_->clear_execution_queue_async());
    }

    boost::asio::io_context& ioc_ = flow_pilot::test::redis_ioc();
    std::shared_ptr<RedisDatabaseAsync> redis_;
};

TEST_F(CompletionHandlerTest, GetCompletedJobsDequeuesCompletionMessage)
{
    CompletionHandler handler(false);
    JobCompletionData expected;
    expected.identity = {"client-" + generate_unique_id(), "workflow-" + generate_unique_id()};
    expected.job_id = "job-" + generate_unique_id();
    expected.status = JobStatus::COMPLETED;
    expected.error_code = StatusCodes::OK;

    ASSERT_TRUE(run_async(ioc_, redis_->enqueue_job_completion_async(expected)));

    JobCompletionData actual;
    ASSERT_TRUE(run_async(ioc_, handler.get_completed_jobs(actual)));
    EXPECT_FALSE(actual.stream_id.empty());
    EXPECT_EQ(actual.identity.client_id, expected.identity.client_id);
    EXPECT_EQ(actual.identity.workflow_id, expected.identity.workflow_id);
    EXPECT_EQ(actual.job_id, expected.job_id);
    EXPECT_EQ(actual.status, expected.status);
    EXPECT_EQ(actual.error_code, expected.error_code);
}

TEST_F(CompletionHandlerTest, ProcessCompletedJobUpdatesRedisAndSqlite)
{
    CompletionHandler handler(false);
    const WorkflowIdentity identity{"client-" + generate_unique_id(), "workflow-" + generate_unique_id()};
    const std::string job_id = "job-" + generate_unique_id();

    persist_sqlite_workflow(ioc_, identity, {{job_id, JobStatus::RUNNING}});
    auto runtime = make_redis_runtime(
        identity,
        WorkflowStatus::RUNNING,
        1,
        1,
        {make_redis_job(job_id, JobStatus::RUNNING)});
    ASSERT_TRUE(run_async(ioc_, redis_->create_workflow_runtime_data_async(runtime)));

    JobCompletionData completion;
    completion.identity = identity;
    completion.job_id = job_id;
    completion.status = JobStatus::COMPLETED;
    completion.error_code = StatusCodes::OK;

    ASSERT_TRUE(run_async(ioc_, handler.process_completed_jobs(completion)));

    JobRuntimeData redis_job;
    ASSERT_TRUE(run_async(ioc_, redis_->fetch_job_runtime_async(identity, job_id, redis_job)));
    EXPECT_EQ(redis_job.status, to_string(JobStatus::COMPLETED));

    std::unordered_map<std::string, std::string> workflow_fields;
    ASSERT_TRUE(run_async(ioc_, redis_->fetch_workflow_runtime_async(identity, workflow_fields)));
    EXPECT_EQ(workflow_fields["completed_jobs"], "1");
    EXPECT_EQ(workflow_fields["reserved_execution_slots"], "0");
    EXPECT_EQ(workflow_fields["status"], to_string(WorkflowStatus::COMPLETED));

    std::vector<WorkflowJob> sqlite_jobs;
    ASSERT_TRUE(run_async(ioc_, DBFactory::get().get_all_jobs_for_workflow_async(
        identity.client_id,
        identity.workflow_id,
        sqlite_jobs)));
    ASSERT_EQ(sqlite_jobs.size(), 1u);
    EXPECT_EQ(sqlite_jobs[0].status, JobStatus::COMPLETED);

    std::vector<WorkflowData> sqlite_workflows;
    ASSERT_TRUE(run_async(ioc_, DBFactory::get().get_all_workflows_for_client_async(
        identity.client_id,
        sqlite_workflows)));
    ASSERT_EQ(sqlite_workflows.size(), 1u);
    EXPECT_EQ(sqlite_workflows[0].status, WorkflowStatus::COMPLETED);
}

TEST_F(CompletionHandlerTest, ProcessCompletedJobReturnsFalseWhenWorkflowRuntimeIsMissing)
{
    CompletionHandler handler(false);
    JobCompletionData completion;
    completion.identity = {"missing-client-" + generate_unique_id(), "missing-workflow-" + generate_unique_id()};
    completion.job_id = "missing-job";
    completion.status = JobStatus::COMPLETED;
    completion.error_code = StatusCodes::OK;

    EXPECT_FALSE(run_async(ioc_, handler.process_completed_jobs(completion)));
}

TEST_F(CompletionHandlerTest, ProcessFailedJobExhaustingRetriesFailsWorkflowAndCancelsWaitingJobs)
{
    CompletionHandler handler(false);
    const WorkflowIdentity identity{"client-" + generate_unique_id(), "workflow-" + generate_unique_id()};
    const std::string failed_job_id = "failed-job-" + generate_unique_id();
    const std::string waiting_job_id = "waiting-job-" + generate_unique_id();

    persist_sqlite_workflow(
        ioc_,
        identity,
        {{failed_job_id, JobStatus::RUNNING}, {waiting_job_id, JobStatus::QUEUED}});

    auto failed_job = make_redis_job(failed_job_id, JobStatus::RUNNING, 30, 1, 0);
    auto waiting_job = make_redis_job(waiting_job_id, JobStatus::QUEUED, 10, 1, 0);
    auto runtime = make_redis_runtime(
        identity,
        WorkflowStatus::RUNNING,
        2,
        1,
        {failed_job, waiting_job});
    ASSERT_TRUE(run_async(ioc_, redis_->create_workflow_runtime_data_async(runtime)));
    ASSERT_TRUE(run_async(ioc_, redis_->enqueue_job_for_execution_async(
        identity,
        PrioritizedJob{waiting_job_id, waiting_job.job_uuid, waiting_job.priority})));

    JobCompletionData completion;
    completion.identity = identity;
    completion.job_id = failed_job_id;
    completion.status = JobStatus::FAILED;
    completion.error_code = StatusCodes::JOB_EXECUTION_FAILURE;

    ASSERT_TRUE(run_async(ioc_, handler.process_completed_jobs(completion)));

    JobRuntimeData redis_failed_job;
    ASSERT_TRUE(run_async(ioc_, redis_->fetch_job_runtime_async(identity, failed_job_id, redis_failed_job)));
    EXPECT_EQ(redis_failed_job.status, to_string(JobStatus::FAILED));
    EXPECT_EQ(redis_failed_job.current_retry_count, 1);

    JobRuntimeData redis_waiting_job;
    ASSERT_TRUE(run_async(ioc_, redis_->fetch_job_runtime_async(identity, waiting_job_id, redis_waiting_job)));
    EXPECT_EQ(redis_waiting_job.status, to_string(JobStatus::CANCELED));

    std::unordered_map<std::string, std::string> workflow_fields;
    ASSERT_TRUE(run_async(ioc_, redis_->fetch_workflow_runtime_async(identity, workflow_fields)));
    EXPECT_EQ(workflow_fields["status"], to_string(WorkflowStatus::FAILED));
    EXPECT_EQ(workflow_fields["failed_jobs"], "1");
    EXPECT_EQ(workflow_fields["reserved_execution_slots"], "0");

    long long queue_size = -1;
    ASSERT_TRUE(run_async(ioc_, redis_->get_execution_queue_size_async(queue_size)));
    EXPECT_EQ(queue_size, 0);

    std::vector<WorkflowData> sqlite_workflows;
    ASSERT_TRUE(run_async(ioc_, DBFactory::get().get_all_workflows_for_client_async(
        identity.client_id,
        sqlite_workflows)));
    ASSERT_EQ(sqlite_workflows.size(), 1u);
    EXPECT_EQ(sqlite_workflows[0].status, WorkflowStatus::FAILED);

    std::vector<WorkflowJob> sqlite_jobs;
    ASSERT_TRUE(run_async(ioc_, DBFactory::get().get_all_jobs_for_workflow_async(
        identity.client_id,
        identity.workflow_id,
        sqlite_jobs)));
    ASSERT_EQ(sqlite_jobs.size(), 2u);
    for (const auto& job : sqlite_jobs) {
        if (job.job_id == failed_job_id) {
            EXPECT_EQ(job.status, JobStatus::FAILED);
        }
    }
}

} // namespace

TEST_F(CompletionHandlerTest, RequestStopIsIdempotent)
{
    auto work_guard = boost::asio::make_work_guard(ioc_);
    ioc_.restart();
    std::thread io_thread([this]() {
        ioc_.run();
    });

    {
        CompletionHandler handler;
        handler.request_stop();
        handler.request_stop();
    }

    work_guard.reset();
    io_thread.join();
}
