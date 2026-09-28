#include <gtest/gtest.h>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/use_future.hpp>
#include <boost/uuid/time_generator_v7.hpp>
#include <chrono>
#include <future>
#include <memory>
#include <random>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "config.h"
#include "database_models.h"
#include "flow_pilot_error_msgs.h"
#include "redis_db_async.h"
#include "redis_test_utils.h"
#include "scheduler.h"

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

void run_async(boost::asio::io_context& ioc, boost::asio::awaitable<void> awaitable)
{
    auto future = boost::asio::co_spawn(ioc, std::move(awaitable), boost::asio::use_future);
    ioc.restart();
    ioc.run();
    future.get();
}

template <typename T>
bool run_until_future_ready(boost::asio::io_context& ioc,
                            std::future<T>& future,
                            std::chrono::milliseconds timeout)
{
    ioc.restart();
    auto deadline = std::chrono::steady_clock::now() + timeout;
    while (future.wait_for(std::chrono::milliseconds(0)) != std::future_status::ready &&
           std::chrono::steady_clock::now() < deadline) {
        ioc.run_for(std::chrono::milliseconds(10));
        if (ioc.stopped()) {
            ioc.restart();
        }
    }

    const bool ready = future.wait_for(std::chrono::milliseconds(0)) == std::future_status::ready;
    if (!ready) {
        ioc.stop();
        ioc.restart();
    }
    return ready;
}

boost::asio::awaitable<void> enqueue_ready_job_after_delay(std::shared_ptr<RedisDatabaseAsync> redis,
                                                           WorkflowIdentity workflow_id,
                                                           PrioritizedJob ready_job,
                                                           std::chrono::milliseconds delay)
{
    auto executor = co_await boost::asio::this_coro::executor;
    boost::asio::steady_timer timer(executor, delay);
    co_await timer.async_wait(boost::asio::use_awaitable);
    co_await redis->enqueue_job_for_execution_async(workflow_id, ready_job);
}

boost::asio::awaitable<void> queue_ready_jobs_after_delay(std::shared_ptr<RedisDatabaseAsync> redis,
                                                          WorkflowIdentity workflow_id,
                                                          PrioritizedJobsList ready_jobs,
                                                          std::chrono::milliseconds delay)
{
    auto executor = co_await boost::asio::this_coro::executor;
    boost::asio::steady_timer timer(executor, delay);
    co_await timer.async_wait(boost::asio::use_awaitable);
    co_await redis->queue_workflow_jobs_for_execution_async(workflow_id, ready_jobs);
}

boost::asio::awaitable<void> enqueue_completion_after_delay(std::shared_ptr<RedisDatabaseAsync> redis,
                                                            JobCompletionData completion,
                                                            std::chrono::milliseconds delay)
{
    auto executor = co_await boost::asio::this_coro::executor;
    boost::asio::steady_timer timer(executor, delay);
    co_await timer.async_wait(boost::asio::use_awaitable);
    co_await redis->enqueue_job_completion_async(completion);
}

} // namespace

static JobRuntimeData make_job_runtime(const std::string& job_id,
                                       JobStatus status,
                                       int remaining_dependencies,
                                       int priority)
{
    JobRuntimeData job;
    job.job_id = job_id;
    job.status = to_string(status);
    job.remaining_dependencies = remaining_dependencies;
    job.max_retries = 3;
    job.current_retry_count = 0;
    job.priority = priority;
    job.retry_delay_sec = 0;
    job.timeout_sec = 30;
    job.retry_backoff_policy = "IMMEDIATE";
    return job;
}

static WorkflowRuntimeInfo make_single_job_runtime(const WorkflowIdentity& workflow_id,
                                                   WorkflowStatus workflow_status,
                                                   JobStatus job_status)
{
    WorkflowRuntimeInfo runtime;
    runtime.identity = workflow_id;
    runtime.workflow.workflow_id = workflow_id.workflow_id;
    runtime.workflow.status = to_string(workflow_status);
    runtime.workflow.max_concurrent_jobs = 1;
    runtime.workflow.reserved_execution_slots = 1;
    runtime.workflow.max_runtime_sec = 60;
    runtime.workflow.total_jobs = 1;
    runtime.workflow.pending_jobs = 0;
    runtime.workflow.completed_jobs = 0;
    runtime.workflow.failed_jobs = 0;
    runtime.jobs = {make_job_runtime("job-a", job_status, 0, 5)};
    return runtime;
}

TEST(RedisCommandExecutorTest, ZsetRemoveBuildsZremCommand)
{
    boost::asio::io_context ioc;
    std::vector<std::string> observed_args;
    RedisCommandExecutor executor(
        [&observed_args](const std::vector<std::string>& args) -> boost::asio::awaitable<RedisReply> {
            observed_args = args;
            co_return RedisReply{RedisReply::Type::Integer, "", 2, {}};
        });

    ASSERT_TRUE(run_async(ioc, executor.execute_zset_remove_command_async("queue", {"job-a", "job-b"})));

    const std::vector<std::string> expected_args{"ZREM", "queue", "job-a", "job-b"};
    EXPECT_EQ(observed_args, expected_args);
}

TEST(RedisCommandExecutorTest, ZsetRemoveSkipsEmptyMemberList)
{
    boost::asio::io_context ioc;
    bool executed = false;
    RedisCommandExecutor executor(
        [&executed](const std::vector<std::string>&) -> boost::asio::awaitable<RedisReply> {
            executed = true;
            co_return RedisReply{RedisReply::Type::Integer, "", 0, {}};
        });

    ASSERT_TRUE(run_async(ioc, executor.execute_zset_remove_command_async("queue", {})));
    EXPECT_FALSE(executed);
}

TEST(RedisCommandExecutorTest, ZsetBlockingDequeueBuildsBzpopmaxCommand)
{
    boost::asio::io_context ioc;
    std::vector<std::string> observed_args;
    RedisCommandExecutor executor(
        [&observed_args](const std::vector<std::string>& args) -> boost::asio::awaitable<RedisReply> {
            observed_args = args;
            co_return RedisReply{RedisReply::Type::Array, "", 0, {"queue", "job-a", "10"}};
        });

    std::string member;
    ASSERT_TRUE(run_async(ioc, executor.execute_zset_blocking_dequeue_command_async("queue", member, std::chrono::milliseconds(5000))));

    const std::vector<std::string> expected_args{"BZPOPMAX", "queue", "5"};
    EXPECT_EQ(observed_args, expected_args);
    EXPECT_EQ(member, "job-a");
}

class RedisDatabaseAsyncValidatorTest : public ::testing::Test {
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
    }

    boost::asio::io_context& ioc_ = flow_pilot::test::redis_ioc();
    std::shared_ptr<RedisDatabaseAsync> redis_;
};

TEST_F(RedisDatabaseAsyncValidatorTest, RedisKeyBuildersUseExpectedNames)
{
    const WorkflowIdentity workflow_id{"client-a", "workflow-b"};
    EXPECT_EQ(RedisKeys::workflow_key(workflow_id),
              "fp:workflow:client-a:workflow-b");
    EXPECT_EQ(RedisKeys::workflow_waiting_jobs_key(workflow_id),
              "fp:workflow:client-a:workflow-b:waiting_ready");
    EXPECT_EQ(RedisKeys::job_key(workflow_id, "job-c"),
              "fp:job:client-a:workflow-b:job-c");
    EXPECT_EQ(RedisKeys::successors_key(workflow_id, "job-c"),
              "fp:successors:client-a:workflow-b:job-c");
    EXPECT_EQ(RedisKeys::payload_key(workflow_id, "job-c"),
              "fp:payload:client-a:workflow-b:job-c");
}

TEST_F(RedisDatabaseAsyncValidatorTest, ZsetBulkEnqueueDequeuesHighestScoreFirst)
{
    const std::string key = "fp:test:zset:" + generate_unique_id();
    long long deleted = 0;
    std::vector<std::string> delete_args{"DEL", key};
    ASSERT_TRUE(run_async(ioc_, redis_->command_executor().execute_integer_command_async(delete_args, deleted)));

    const std::unordered_map<std::string, unsigned int> members{
        {"low-priority", 10},
        {"high-priority", 30},
        {"medium-priority", 20}
    };

    ASSERT_TRUE(run_async(ioc_, redis_->command_executor().execute_zset_enqueue_command_async(key, members)));

    std::string member;
    ASSERT_TRUE(run_async(ioc_, redis_->command_executor().execute_zset_dequeue_command_async(key, member)));
    EXPECT_EQ(member, "high-priority");

    ASSERT_TRUE(run_async(ioc_, redis_->command_executor().execute_zset_dequeue_command_async(key, member)));
    EXPECT_EQ(member, "medium-priority");

    ASSERT_TRUE(run_async(ioc_, redis_->command_executor().execute_zset_dequeue_command_async(key, member)));
    EXPECT_EQ(member, "low-priority");

    member = "stale";
    ASSERT_TRUE(run_async(ioc_, redis_->command_executor().execute_zset_dequeue_command_async(key, member)));
    EXPECT_TRUE(member.empty());
}

TEST_F(RedisDatabaseAsyncValidatorTest, ZsetBulkEnqueuePreservesExistingMembersWithNx)
{
    const std::string key = "fp:test:zset:" + generate_unique_id();
    long long deleted = 0;
    std::vector<std::string> delete_args{"DEL", key};
    ASSERT_TRUE(run_async(ioc_, redis_->command_executor().execute_integer_command_async(delete_args, deleted)));

    const std::unordered_map<std::string, unsigned int> initial_members{{"first", 10}, {"second", 20}};
    const std::unordered_map<std::string, unsigned int> duplicate_member{{"first", 50}};
    ASSERT_TRUE(run_async(ioc_, redis_->command_executor().execute_zset_enqueue_command_async(key, initial_members)));
    ASSERT_TRUE(run_async(ioc_, redis_->command_executor().execute_zset_enqueue_command_async(key, duplicate_member)));

    std::string member;
    ASSERT_TRUE(run_async(ioc_, redis_->command_executor().execute_zset_dequeue_command_async(key, member)));
    EXPECT_EQ(member, "second");

    ASSERT_TRUE(run_async(ioc_, redis_->command_executor().execute_zset_dequeue_command_async(key, member)));
    EXPECT_EQ(member, "first");
}

TEST_F(RedisDatabaseAsyncValidatorTest, ZsetBulkRemoveDeletesSelectedMembers)
{
    const std::string key = "fp:test:zset:" + generate_unique_id();
    long long deleted = 0;
    std::vector<std::string> delete_args{"DEL", key};
    ASSERT_TRUE(run_async(ioc_, redis_->command_executor().execute_integer_command_async(delete_args, deleted)));

    const std::unordered_map<std::string, unsigned int> members{
        {"kept", 10},
        {"removed-a", 20},
        {"removed-b", 30}
    };

    ASSERT_TRUE(run_async(ioc_, redis_->command_executor().execute_zset_enqueue_command_async(key, members)));
    ASSERT_TRUE(run_async(ioc_, redis_->command_executor().execute_zset_remove_command_async(key, {"removed-a", "removed-b"})));

    std::string member;
    ASSERT_TRUE(run_async(ioc_, redis_->command_executor().execute_zset_dequeue_command_async(key, member)));
    EXPECT_EQ(member, "kept");

    ASSERT_TRUE(run_async(ioc_, redis_->command_executor().execute_zset_dequeue_command_async(key, member)));
    EXPECT_TRUE(member.empty());
}

TEST_F(RedisDatabaseAsyncValidatorTest, EnqueueReadyJobStoresFullRedisJobKey)
{
    run_async(ioc_, redis_->clear_execution_queue_async());

    const WorkflowIdentity workflow_id{"client-" + generate_unique_id(), "workflow-" + generate_unique_id()};
    JobRuntimeData job_data;
    job_data.job_id = "job-" + generate_unique_id();
    job_data.status = to_string(JobStatus::READY);
    job_data.remaining_dependencies = 0;
    job_data.priority = 4;
    job_data.timeout_sec = 30;
    job_data.max_retries = 3;
    job_data.current_retry_count = 0;
    job_data.retry_delay_sec = 1;
    job_data.retry_backoff_policy = "IMMEDIATE";

    ASSERT_TRUE(run_async(ioc_, redis_->set_job_runtime_async(workflow_id, job_data)));

     boost::uuids::time_generator_v7 gen;
    boost::uuids::uuid job_uuid = gen();
    PrioritizedJob ready_job = {job_data.job_id, job_uuid, 5};
    ASSERT_TRUE(run_async(ioc_, redis_->enqueue_job_for_execution_async(workflow_id, ready_job)));

    std::string dequeued_job_key;
    ASSERT_TRUE(run_async(ioc_,
                          redis_->command_executor().execute_zset_dequeue_command_async(
                              "fp:execution_queue",
                              dequeued_job_key)));
    EXPECT_EQ(dequeued_job_key, RedisKeys::job_key(workflow_id, job_data.job_id));

    std::unordered_map<std::string, std::string> fields;
    const std::string job_key = RedisKeys::job_key(workflow_id, job_data.job_id);
    ASSERT_TRUE(run_async(ioc_, redis_->command_executor().execute_hgetall_command_async(job_key, fields)));
    EXPECT_EQ(fields["owned_by"], "");

    ASSERT_TRUE(run_async(ioc_, redis_->delete_all_workflow_jobs_async(workflow_id, {job_data})));
}

TEST_F(RedisDatabaseAsyncValidatorTest, RemoveWorkflowJobsFromExecutionQueueDeletesRedisJobKeys)
{
    run_async(ioc_, redis_->clear_execution_queue_async());

    const WorkflowIdentity workflow_id{"client-" + generate_unique_id(), "workflow-" + generate_unique_id()};
    boost::uuids::time_generator_v7 gen;
    const PrioritizedJobsList queued_jobs{
        {"job-a", gen(), 10},
        {"job-b", gen(), 20}
    };
    const PrioritizedJobsList removed_jobs{
        {"job-a", gen(), 10}
    };

    JobRuntimeData job_b_runtime = make_job_runtime("job-b", JobStatus::READY, 0, 20);
    ASSERT_TRUE(run_async(ioc_, redis_->set_job_runtime_async(workflow_id, job_b_runtime)));
    ASSERT_TRUE(run_async(ioc_, redis_->queue_workflow_jobs_for_execution_async(workflow_id, queued_jobs)));
    ASSERT_TRUE(run_async(ioc_, redis_->remove_jobs_from_execution_queue_async(workflow_id, removed_jobs)));

    std::string ready_job;
    ASSERT_TRUE(run_async(ioc_,
                          redis_->command_executor().execute_zset_dequeue_command_async(
                              "fp:execution_queue",
                              ready_job)));
    EXPECT_EQ(ready_job, RedisKeys::job_key(workflow_id, "job-b"));

    ASSERT_TRUE(run_async(ioc_,
                          redis_->command_executor().execute_zset_dequeue_command_async(
                              "fp:execution_queue",
                              ready_job)));
    EXPECT_TRUE(ready_job.empty());

    ASSERT_TRUE(run_async(ioc_, redis_->delete_all_workflow_jobs_async(workflow_id, {job_b_runtime})));
}

TEST_F(RedisDatabaseAsyncValidatorTest, EnqueueReadyJobDoesNotCreateJobHash)
{
    run_async(ioc_, redis_->clear_execution_queue_async());

    const WorkflowIdentity workflow_id{"client-" + generate_unique_id(), "workflow-" + generate_unique_id()};
    const std::string job_id = "job-" + generate_unique_id();

    boost::uuids::time_generator_v7 gen;
    PrioritizedJob ready_job{job_id, gen(), 9};
    ASSERT_TRUE(run_async(ioc_, redis_->enqueue_job_for_execution_async(workflow_id, ready_job)));

    std::string dequeued_job_key = "stale";
    ASSERT_TRUE(run_async(ioc_,
                          redis_->command_executor().execute_zset_dequeue_command_async(
                              "fp:execution_queue",
                              dequeued_job_key)));
    EXPECT_EQ(dequeued_job_key, RedisKeys::job_key(workflow_id, job_id));

    long long queue_size = -1;
    ASSERT_TRUE(run_async(ioc_, redis_->get_execution_queue_size_async(queue_size)));
    EXPECT_EQ(queue_size, 0);

    long long exists = 0;
    std::vector<std::string> exists_args{"EXISTS", RedisKeys::job_key(workflow_id, job_id)};
    ASSERT_TRUE(run_async(ioc_, redis_->command_executor().execute_integer_command_async(exists_args, exists)));
    EXPECT_EQ(exists, 0);
}

TEST_F(RedisDatabaseAsyncValidatorTest, CompletionStreamEmptyDequeueReturnsNoMessage)
{
    long long deleted = 0;
    std::vector<std::string> delete_args{"DEL", "fp:job_completion_stream"};
    ASSERT_TRUE(run_async(ioc_, redis_->command_executor().execute_integer_command_async(delete_args, deleted)));

    JobCompletionData completion;
    completion.stream_id = "stale";
    completion.identity = {"stale-client", "stale-workflow"};
    completion.job_id = "stale-job";
    completion.status = JobStatus::COMPLETED;

    ASSERT_TRUE(run_async(ioc_, redis_->dequeue_job_completion_async(completion)));
    EXPECT_TRUE(completion.stream_id.empty());
    EXPECT_TRUE(completion.identity.client_id.empty());
    EXPECT_TRUE(completion.identity.workflow_id.empty());
    EXPECT_TRUE(completion.job_id.empty());
    EXPECT_EQ(completion.status, JobStatus::UNKNOWN);
}

TEST_F(RedisDatabaseAsyncValidatorTest, CompletionStreamEnqueueAndDequeueRoundtrip)
{
    long long deleted = 0;
    std::vector<std::string> delete_args{"DEL", "fp:job_completion_stream"};
    ASSERT_TRUE(run_async(ioc_, redis_->command_executor().execute_integer_command_async(delete_args, deleted)));

    JobCompletionData expected;
    expected.identity = {"client-" + generate_unique_id(), "workflow-" + generate_unique_id()};
    expected.job_id = "job-" + generate_unique_id();
    expected.status = JobStatus::FAILED;
    expected.error_code = StatusCodes::JOB_EXECUTION_FAILURE;

    ASSERT_TRUE(run_async(ioc_, redis_->enqueue_job_completion_async(expected)));

    JobCompletionData actual;
    ASSERT_TRUE(run_async(ioc_, redis_->dequeue_job_completion_async(actual)));
    EXPECT_FALSE(actual.stream_id.empty());
    EXPECT_EQ(actual.identity.client_id, expected.identity.client_id);
    EXPECT_EQ(actual.identity.workflow_id, expected.identity.workflow_id);
    EXPECT_EQ(actual.job_id, expected.job_id);
    EXPECT_EQ(actual.status, expected.status);
    EXPECT_EQ(actual.error_code, expected.error_code);

    JobCompletionData empty;
    ASSERT_TRUE(run_async(ioc_, redis_->dequeue_job_completion_async(empty)));
    EXPECT_TRUE(empty.stream_id.empty());
}

TEST_F(RedisDatabaseAsyncValidatorTest, CompletionStreamDequeuesOldestMessageFirst)
{
    long long deleted = 0;
    std::vector<std::string> delete_args{"DEL", "fp:job_completion_stream"};
    ASSERT_TRUE(run_async(ioc_, redis_->command_executor().execute_integer_command_async(delete_args, deleted)));

    JobCompletionData first;
    first.identity = {"client-" + generate_unique_id(), "workflow-" + generate_unique_id()};
    first.job_id = "job-a";
    first.status = JobStatus::COMPLETED;
    first.error_code = StatusCodes::OK;

    JobCompletionData second = first;
    second.job_id = "job-b";
    second.status = JobStatus::CANCELED;
    second.error_code = StatusCodes::STATUS_UPDATED_FAILURE;

    ASSERT_TRUE(run_async(ioc_, redis_->enqueue_job_completion_async(first)));
    ASSERT_TRUE(run_async(ioc_, redis_->enqueue_job_completion_async(second)));

    JobCompletionData actual;
    ASSERT_TRUE(run_async(ioc_, redis_->dequeue_job_completion_async(actual)));
    EXPECT_EQ(actual.job_id, first.job_id);
    EXPECT_EQ(actual.status, first.status);
    EXPECT_EQ(actual.error_code, first.error_code);

    ASSERT_TRUE(run_async(ioc_, redis_->dequeue_job_completion_async(actual)));
    EXPECT_EQ(actual.job_id, second.job_id);
    EXPECT_EQ(actual.status, second.status);
    EXPECT_EQ(actual.error_code, second.error_code);
}

TEST_F(RedisDatabaseAsyncValidatorTest, CompletionStreamEventWakesWaitingHandler)
{
    long long deleted = 0;
    std::vector<std::string> delete_args{"DEL", "fp:job_completion_stream"};
    ASSERT_TRUE(run_async(ioc_, redis_->command_executor().execute_integer_command_async(delete_args, deleted)));

    JobCompletionData completion;
    completion.identity = {"client-" + generate_unique_id(), "workflow-" + generate_unique_id()};
    completion.job_id = "job-" + generate_unique_id();
    completion.status = JobStatus::COMPLETED;
    completion.error_code = StatusCodes::OK;

    auto wait_future = boost::asio::co_spawn(
        ioc_,
        redis_->wait_for_job_completion_event_async(std::chrono::seconds(2)),
        boost::asio::use_future);
    boost::asio::co_spawn(
        ioc_,
        enqueue_completion_after_delay(redis_, completion, std::chrono::milliseconds(50)),
        boost::asio::detached);

    ASSERT_TRUE(run_until_future_ready(ioc_, wait_future, std::chrono::seconds(2)));
    EXPECT_EQ(wait_future.get(), JobCompletionWaitResult::EVENT);

    JobCompletionData actual;
    ASSERT_TRUE(run_async(ioc_, redis_->dequeue_job_completion_async(actual)));
    EXPECT_EQ(actual.identity.client_id, completion.identity.client_id);
    EXPECT_EQ(actual.identity.workflow_id, completion.identity.workflow_id);
    EXPECT_EQ(actual.job_id, completion.job_id);
    EXPECT_EQ(actual.status, completion.status);
    EXPECT_EQ(actual.error_code, completion.error_code);
}

TEST_F(RedisDatabaseAsyncValidatorTest, CompletionStreamWaitReportsTimeout)
{
    long long deleted = 0;
    std::vector<std::string> delete_args{"DEL", "fp:job_completion_stream"};
    ASSERT_TRUE(run_async(ioc_, redis_->command_executor().execute_integer_command_async(delete_args, deleted)));

    EXPECT_EQ(run_async(ioc_, redis_->wait_for_job_completion_event_async(std::chrono::milliseconds(10))),
              JobCompletionWaitResult::TIMEOUT);
}

TEST_F(RedisDatabaseAsyncValidatorTest, RawZsetDequeuePopsQueuedEntryThatIsNotAHash)
{
    run_async(ioc_, redis_->clear_execution_queue_async());

    const WorkflowIdentity workflow_id{"client-" + generate_unique_id(), "workflow-" + generate_unique_id()};
    const std::string job_id = "job-" + generate_unique_id();
    const std::string job_key = RedisKeys::job_key(workflow_id, job_id);

    long long deleted = 0;
    std::vector<std::string> del_args{"DEL", job_key};
    ASSERT_TRUE(run_async(ioc_, redis_->command_executor().execute_integer_command_async(del_args, deleted)));
    ASSERT_TRUE(run_async(ioc_, redis_->command_executor().execute_set_command_async(job_key, "not-a-hash", 0, false)));

    boost::uuids::time_generator_v7 gen;
    PrioritizedJob ready_job{job_id, gen(), 9};
    ASSERT_TRUE(run_async(ioc_, redis_->enqueue_job_for_execution_async(workflow_id, ready_job)));

    std::string dequeued_job_key;
    EXPECT_TRUE(run_async(ioc_,
                          redis_->command_executor().execute_zset_dequeue_command_async(
                              "fp:execution_queue",
                              dequeued_job_key)));
    EXPECT_EQ(dequeued_job_key, job_key);

    long long queue_size = 0;
    ASSERT_TRUE(run_async(ioc_, redis_->get_execution_queue_size_async(queue_size)));
    EXPECT_EQ(queue_size, 0);

    run_async(ioc_, redis_->clear_execution_queue_async());
    ASSERT_TRUE(run_async(ioc_, redis_->command_executor().execute_integer_command_async(del_args, deleted)));
}

TEST_F(RedisDatabaseAsyncValidatorTest, BlockingDequeueWaitsUntilExecutionQueueReceivesJob)
{
    run_async(ioc_, redis_->clear_execution_queue_async());

    const WorkflowIdentity workflow_id{"client-" + generate_unique_id(), "workflow-" + generate_unique_id()};
    const std::string job_id = "job-" + generate_unique_id();
    JobRuntimeData job_data = make_job_runtime(job_id, JobStatus::READY, 0, 17);
    ASSERT_TRUE(run_async(ioc_, redis_->set_job_runtime_async(workflow_id, job_data)));

    boost::uuids::time_generator_v7 gen;
    PrioritizedJob ready_job{job_id, gen(), 17};
    WorkflowIdentity dequeued_identity;
    std::string dequeued_job_id;
    ASSERT_TRUE(redis_->register_scheduler("scheduler-waiting"));

    auto wait_future = boost::asio::co_spawn(
        ioc_,
        redis_->blocking_dequeue_job_for_execution_async(dequeued_identity, dequeued_job_id, "scheduler-waiting"),
        boost::asio::use_future);
    boost::asio::co_spawn(
        ioc_,
        enqueue_ready_job_after_delay(redis_, workflow_id, ready_job, std::chrono::milliseconds(50)),
        boost::asio::detached);

    ASSERT_TRUE(run_until_future_ready(ioc_, wait_future, std::chrono::seconds(2)));
    ASSERT_TRUE(wait_future.get());

    EXPECT_EQ(dequeued_identity.client_id, workflow_id.client_id);
    EXPECT_EQ(dequeued_identity.workflow_id, workflow_id.workflow_id);
    EXPECT_EQ(dequeued_job_id, job_id);

    std::unordered_map<std::string, std::string> fields;
    ASSERT_TRUE(run_async(ioc_, redis_->command_executor().execute_hgetall_command_async(RedisKeys::job_key(workflow_id, job_id), fields)));
    EXPECT_EQ(fields["owned_by"], "scheduler-waiting");

    long long queue_size = 0;
    ASSERT_TRUE(run_async(ioc_, redis_->get_execution_queue_size_async(queue_size)));
    EXPECT_EQ(queue_size, 0);

    redis_->deregister_scheduler("scheduler-waiting");
    ASSERT_TRUE(run_async(ioc_, redis_->delete_all_workflow_jobs_async(workflow_id, {job_data})));
}

TEST_F(RedisDatabaseAsyncValidatorTest, BlockingDequeueSupportsMultipleSchedulers)
{
    run_async(ioc_, redis_->clear_execution_queue_async());

    const WorkflowIdentity workflow_id{"client-" + generate_unique_id(), "workflow-" + generate_unique_id()};
    JobRuntimeData first_job_data = make_job_runtime("job-a-" + generate_unique_id(), JobStatus::READY, 0, 11);
    JobRuntimeData second_job_data = make_job_runtime("job-b-" + generate_unique_id(), JobStatus::READY, 0, 31);
    ASSERT_TRUE(run_async(ioc_, redis_->set_job_runtime_async(workflow_id, first_job_data)));
    ASSERT_TRUE(run_async(ioc_, redis_->set_job_runtime_async(workflow_id, second_job_data)));

    boost::uuids::time_generator_v7 gen;
    PrioritizedJobsList ready_jobs{
        {first_job_data.job_id, gen(), 11},
        {second_job_data.job_id, gen(), 31}
    };
    WorkflowIdentity first_identity;
    WorkflowIdentity second_identity;
    std::string first_dequeued_job;
    std::string second_dequeued_job;
    ASSERT_TRUE(redis_->register_scheduler("scheduler-one"));
    ASSERT_TRUE(redis_->register_scheduler("scheduler-two"));

    auto first_waiter = boost::asio::co_spawn(
        ioc_,
        redis_->blocking_dequeue_job_for_execution_async(first_identity, first_dequeued_job, "scheduler-one"),
        boost::asio::use_future);
    auto second_waiter = boost::asio::co_spawn(
        ioc_,
        redis_->blocking_dequeue_job_for_execution_async(second_identity, second_dequeued_job, "scheduler-two"),
        boost::asio::use_future);
    boost::asio::co_spawn(
        ioc_,
        queue_ready_jobs_after_delay(redis_, workflow_id, ready_jobs, std::chrono::milliseconds(50)),
        boost::asio::detached);

    ASSERT_TRUE(run_until_future_ready(ioc_, first_waiter, std::chrono::seconds(2)));
    ASSERT_TRUE(run_until_future_ready(ioc_, second_waiter, std::chrono::seconds(2)));
    ASSERT_TRUE(first_waiter.get());
    ASSERT_TRUE(second_waiter.get());

    EXPECT_EQ(first_identity.client_id, workflow_id.client_id);
    EXPECT_EQ(first_identity.workflow_id, workflow_id.workflow_id);
    EXPECT_EQ(second_identity.client_id, workflow_id.client_id);
    EXPECT_EQ(second_identity.workflow_id, workflow_id.workflow_id);
    EXPECT_NE(first_dequeued_job, second_dequeued_job);

    long long queue_size = 0;
    ASSERT_TRUE(run_async(ioc_, redis_->get_execution_queue_size_async(queue_size)));
    EXPECT_EQ(queue_size, 0);

    redis_->deregister_scheduler("scheduler-one");
    redis_->deregister_scheduler("scheduler-two");
    ASSERT_TRUE(run_async(ioc_, redis_->delete_all_workflow_jobs_async(workflow_id, {first_job_data, second_job_data})));
}

TEST_F(RedisDatabaseAsyncValidatorTest, BlockingDequeueReturnsImmediatelyWhenQueueAlreadyHasReadyJob)
{
    run_async(ioc_, redis_->clear_execution_queue_async());

    const WorkflowIdentity workflow_id{"client-" + generate_unique_id(), "workflow-" + generate_unique_id()};
    const std::string job_id = "job-" + generate_unique_id();
    JobRuntimeData job_data = make_job_runtime(job_id, JobStatus::READY, 0, 23);
    ASSERT_TRUE(run_async(ioc_, redis_->set_job_runtime_async(workflow_id, job_data)));

    boost::uuids::time_generator_v7 gen;
    PrioritizedJob ready_job{job_id, gen(), 23};
    ASSERT_TRUE(run_async(ioc_, redis_->enqueue_job_for_execution_async(workflow_id, ready_job)));

    WorkflowIdentity dequeued_identity;
    std::string dequeued_job_id;
    ASSERT_TRUE(redis_->register_scheduler("scheduler-existing-work"));
    auto wait_future = boost::asio::co_spawn(
        ioc_,
        redis_->blocking_dequeue_job_for_execution_async(dequeued_identity, dequeued_job_id, "scheduler-existing-work"),
        boost::asio::use_future);

    ASSERT_TRUE(run_until_future_ready(ioc_, wait_future, std::chrono::seconds(2)));
    ASSERT_TRUE(wait_future.get());

    EXPECT_EQ(dequeued_identity.client_id, workflow_id.client_id);
    EXPECT_EQ(dequeued_identity.workflow_id, workflow_id.workflow_id);
    EXPECT_EQ(dequeued_job_id, job_id);

    redis_->deregister_scheduler("scheduler-existing-work");
    ASSERT_TRUE(run_async(ioc_, redis_->delete_all_workflow_jobs_async(workflow_id, {job_data})));
}

TEST_F(RedisDatabaseAsyncValidatorTest, BlockingDequeueSkipsInvalidEntriesUntilValidJobArrives)
{
    run_async(ioc_, redis_->clear_execution_queue_async());

    const std::unordered_map<std::string, unsigned int> invalid_member{{"not-a-job-key", 99}};
    ASSERT_TRUE(run_async(ioc_,
                          redis_->command_executor().execute_zset_enqueue_command_async(
                              "fp:execution_queue",
                              invalid_member)));

    const WorkflowIdentity workflow_id{"client-" + generate_unique_id(), "workflow-" + generate_unique_id()};
    const std::string job_id = "job-" + generate_unique_id();
    JobRuntimeData job_data = make_job_runtime(job_id, JobStatus::READY, 0, 23);
    ASSERT_TRUE(run_async(ioc_, redis_->set_job_runtime_async(workflow_id, job_data)));

    boost::uuids::time_generator_v7 gen;
    PrioritizedJob ready_job{job_id, gen(), 23};
    WorkflowIdentity dequeued_identity;
    std::string dequeued_job_id;
    ASSERT_TRUE(redis_->register_scheduler("scheduler-skip-invalid"));

    auto wait_future = boost::asio::co_spawn(
        ioc_,
        redis_->blocking_dequeue_job_for_execution_async(dequeued_identity, dequeued_job_id, "scheduler-skip-invalid"),
        boost::asio::use_future);
    boost::asio::co_spawn(
        ioc_,
        enqueue_ready_job_after_delay(redis_, workflow_id, ready_job, std::chrono::milliseconds(50)),
        boost::asio::detached);

    ASSERT_TRUE(run_until_future_ready(ioc_, wait_future, std::chrono::seconds(2)));
    ASSERT_TRUE(wait_future.get());

    EXPECT_EQ(dequeued_identity.client_id, workflow_id.client_id);
    EXPECT_EQ(dequeued_identity.workflow_id, workflow_id.workflow_id);
    EXPECT_EQ(dequeued_job_id, job_id);

    redis_->deregister_scheduler("scheduler-skip-invalid");
    ASSERT_TRUE(run_async(ioc_, redis_->delete_all_workflow_jobs_async(workflow_id, {job_data})));
}

TEST_F(RedisDatabaseAsyncValidatorTest, BlockingDequeueFailsForUnregisteredScheduler)
{
    run_async(ioc_, redis_->clear_execution_queue_async());

    WorkflowIdentity dequeued_identity;
    std::string dequeued_job_id;

    EXPECT_FALSE(run_async(ioc_,
                           redis_->blocking_dequeue_job_for_execution_async(
                               dequeued_identity,
                               dequeued_job_id,
                               "scheduler-not-registered-" + generate_unique_id())));
}

TEST_F(RedisDatabaseAsyncValidatorTest, SchedulerWithSingleWorkerDequeuesReadyJobAndStopsCleanly)
{
    run_async(ioc_, redis_->clear_execution_queue_async());

    const WorkflowIdentity workflow_id{"client-" + generate_unique_id(), "workflow-" + generate_unique_id()};
    const std::string job_id = "job-" + generate_unique_id();
    JobRuntimeData job_data = make_job_runtime(job_id, JobStatus::QUEUED, 0, 25);
    ASSERT_TRUE(run_async(ioc_, redis_->set_job_runtime_async(workflow_id, job_data)));
    ASSERT_TRUE(run_async(ioc_, redis_->set_job_payload_async(workflow_id, job_id, {1, 2, 3})));
    ASSERT_TRUE(run_async(ioc_, redis_->enqueue_job_for_execution_async(
        workflow_id,
        PrioritizedJob{job_id, job_data.job_uuid, job_data.priority})));

    auto work_guard = boost::asio::make_work_guard(ioc_);
    ioc_.restart();
    std::thread io_thread([this]() {
        ioc_.run();
    });

    {
        scheduler test_scheduler(1, 1);

        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(3);
        bool dequeued = false;
        while (std::chrono::steady_clock::now() < deadline) {
            long long queue_size = -1;
            auto queue_size_future = boost::asio::co_spawn(
                ioc_,
                redis_->get_execution_queue_size_async(queue_size),
                boost::asio::use_future);

            if (queue_size_future.get() && queue_size == 0) {
                dequeued = true;
                break;
            }

            std::this_thread::sleep_for(std::chrono::milliseconds(25));
        }

        EXPECT_TRUE(dequeued);
    }

    work_guard.reset();
    io_thread.join();

    run_async(ioc_, redis_->clear_execution_queue_async());
    ASSERT_TRUE(run_async(ioc_, redis_->delete_job_payload_async(workflow_id, job_id)));
    ASSERT_TRUE(run_async(ioc_, redis_->delete_all_workflow_jobs_async(workflow_id, {job_data})));
}

// TEST_F(RedisDatabaseAsyncValidatorTest, TryAcquireJobSlotIncrementsQueuedCountUntilLimit)
// {
//     const WorkflowIdentity workflow_id{"client-" + generate_unique_id(), "workflow-" + generate_unique_id()};
//     WorkflowRuntimeData workflow;
//     workflow.workflow_id = workflow_id.workflow_id;
//     workflow.status = to_string(WorkflowStatus::READY);
//     workflow.max_concurrent_jobs = 2;
//     workflow.reserved_execution_slots = 1;
//     workflow.max_runtime_sec = 60;
//     workflow.total_jobs = 3;
//     workflow.pending_jobs = 0;
//     workflow.completed_jobs = 0;
//     workflow.failed_jobs = 0;

//     ASSERT_TRUE(run_async(ioc_, redis_->set_workflow_runtime_async(workflow_id, workflow)));

//     std::string workflow_status;
//     EXPECT_TRUE(run_async(ioc_, redis_->try_acquire_job_slot_async(workflow_id, workflow_status)));
//     EXPECT_EQ(workflow_status, to_string(WorkflowStatus::READY));

//     std::unordered_map<std::string, std::string> fields;
//     ASSERT_TRUE(run_async(ioc_, redis_->fetch_workflow_runtime_async(workflow_id, fields)));
//     EXPECT_EQ(fields["curr_queued_jobs"], "2");

//     EXPECT_FALSE(run_async(ioc_, redis_->try_acquire_job_slot_async(workflow_id, workflow_status)));
//     EXPECT_EQ(workflow_status, to_string(WorkflowStatus::READY));

//     ASSERT_TRUE(run_async(ioc_, redis_->delete_workflow_runtime_async(workflow_id)));
// }

TEST_F(RedisDatabaseAsyncValidatorTest, DeleteWorkflowRuntimeDataCleansWaitingReadyQueue)
{
    const WorkflowIdentity workflow_id{"client-" + generate_unique_id(), "workflow-" + generate_unique_id()};

    WorkflowRuntimeInfo runtime;
    runtime.identity = workflow_id;
    runtime.workflow.workflow_id = workflow_id.workflow_id;
    runtime.workflow.status = to_string(WorkflowStatus::READY);
    runtime.workflow.max_concurrent_jobs = 1;
    runtime.workflow.reserved_execution_slots = 1;
    runtime.workflow.max_runtime_sec = 60;
    runtime.workflow.total_jobs = 3;
    runtime.workflow.pending_jobs = 0;
    runtime.workflow.completed_jobs = 0;
    runtime.workflow.failed_jobs = 0;
    runtime.jobs = {
        make_job_runtime("queued", JobStatus::QUEUED, 0, 10),
        make_job_runtime("ready-a", JobStatus::READY, 0, 5),
        make_job_runtime("ready-b", JobStatus::READY, 0, 5)
    };

    boost::uuids::time_generator_v7 gen;
    runtime.ready_job_list = {
        {"ready-a", gen(), 5},
        {"ready-b", gen(), 5}
    };
    runtime.jobs_queued_for_execution = {{"queued", gen(), 10}};

    ASSERT_TRUE(run_async(ioc_, redis_->create_workflow_runtime_data_async(runtime)));

    long long waiting_count = 0;
    std::vector<std::string> zcard_args{"ZCARD", RedisKeys::workflow_waiting_jobs_key(workflow_id)};
    ASSERT_TRUE(run_async(ioc_, redis_->command_executor().execute_integer_command_async(zcard_args, waiting_count)));
    ASSERT_EQ(waiting_count, 2);

    ASSERT_TRUE(run_async(ioc_, redis_->delete_workflow_runtime_data_async(runtime)));

    waiting_count = -1;
    ASSERT_TRUE(run_async(ioc_, redis_->command_executor().execute_integer_command_async(zcard_args, waiting_count)));
    EXPECT_EQ(waiting_count, 0);
}

TEST_F(RedisDatabaseAsyncValidatorTest, CreateWorkflowRuntimeDataFailsWhenWorkflowHashCannotBeCreated)
{
    const WorkflowIdentity workflow_id{"client-" + generate_unique_id(), "workflow-" + generate_unique_id()};
    const std::string workflow_key = RedisKeys::workflow_key(workflow_id);

    ASSERT_TRUE(run_async(ioc_, redis_->command_executor().execute_set_command_async(workflow_key, "not-a-hash", 0, false)));

    WorkflowRuntimeInfo runtime;
    runtime.identity = workflow_id;
    runtime.workflow.workflow_id = workflow_id.workflow_id;
    runtime.workflow.status = to_string(WorkflowStatus::READY);
    runtime.workflow.max_concurrent_jobs = 1;
    runtime.workflow.reserved_execution_slots = 0;
    runtime.workflow.max_runtime_sec = 60;
    runtime.workflow.total_jobs = 1;
    runtime.workflow.pending_jobs = 0;
    runtime.workflow.completed_jobs = 0;
    runtime.workflow.failed_jobs = 0;
    runtime.jobs = {make_job_runtime("job-a", JobStatus::READY, 0, 5)};

    EXPECT_FALSE(run_async(ioc_, redis_->create_workflow_runtime_data_async(runtime)));

    long long exists = 0;
    std::vector<std::string> exists_args{"EXISTS", RedisKeys::job_key(workflow_id, "job-a")};
    ASSERT_TRUE(run_async(ioc_, redis_->command_executor().execute_integer_command_async(exists_args, exists)));
    EXPECT_EQ(exists, 0);

    long long deleted = 0;
    std::vector<std::string> del_args{"DEL", workflow_key};
    ASSERT_TRUE(run_async(ioc_, redis_->command_executor().execute_integer_command_async(del_args, deleted)));
}

TEST_F(RedisDatabaseAsyncValidatorTest, CreateWorkflowRuntimeDataRollsBackWhenWaitingReadyZsetCannotBeCreated)
{
    const WorkflowIdentity workflow_id{"client-" + generate_unique_id(), "workflow-" + generate_unique_id()};
    const std::string waiting_key = RedisKeys::workflow_waiting_jobs_key(workflow_id);
    ASSERT_TRUE(run_async(ioc_, redis_->command_executor().execute_set_command_async(waiting_key, "not-a-zset", 0, false)));

    WorkflowRuntimeInfo runtime;
    runtime.identity = workflow_id;
    runtime.workflow.workflow_id = workflow_id.workflow_id;
    runtime.workflow.status = to_string(WorkflowStatus::READY);
    runtime.workflow.max_concurrent_jobs = 1;
    runtime.workflow.reserved_execution_slots = 1;
    runtime.workflow.max_runtime_sec = 60;
    runtime.workflow.total_jobs = 2;
    runtime.workflow.pending_jobs = 0;
    runtime.workflow.completed_jobs = 0;
    runtime.workflow.failed_jobs = 0;
    runtime.jobs = {
        make_job_runtime("queued", JobStatus::QUEUED, 0, 9),
        make_job_runtime("ready", JobStatus::READY, 0, 5)
    };
    boost::uuids::time_generator_v7 gen;
    runtime.ready_job_list = {{"ready", gen(), 5}};
    runtime.jobs_queued_for_execution = {{"queued", gen(), 9}};

    EXPECT_FALSE(run_async(ioc_, redis_->create_workflow_runtime_data_async(runtime)));

    long long exists = 0;
    std::vector<std::string> exists_args{"EXISTS", RedisKeys::workflow_key(workflow_id)};
    ASSERT_TRUE(run_async(ioc_, redis_->command_executor().execute_integer_command_async(exists_args, exists)));
    EXPECT_EQ(exists, 0);

    exists_args = {"EXISTS", RedisKeys::job_key(workflow_id, "queued")};
    ASSERT_TRUE(run_async(ioc_, redis_->command_executor().execute_integer_command_async(exists_args, exists)));
    EXPECT_EQ(exists, 0);

    long long deleted = 0;
    std::vector<std::string> del_args{"DEL", waiting_key};
    ASSERT_TRUE(run_async(ioc_, redis_->command_executor().execute_integer_command_async(del_args, deleted)));
}

TEST_F(RedisDatabaseAsyncValidatorTest, TrySetJobToRunningStartsReadyWorkflow)
{
    const WorkflowIdentity workflow_id{"client-" + generate_unique_id(), "workflow-" + generate_unique_id()};
    auto runtime = make_single_job_runtime(workflow_id, WorkflowStatus::READY, JobStatus::QUEUED);
    ASSERT_TRUE(run_async(ioc_, redis_->create_workflow_runtime_data_async(runtime)));

    StartJobResult result = StartJobResult::INTERNAL_ERROR;
    ASSERT_TRUE(run_async(ioc_, redis_->try_set_job_to_running_async(workflow_id, "job-a", result)));
    EXPECT_EQ(result, StartJobResult::FIRST_TO_START);

    std::unordered_map<std::string, std::string> workflow_fields;
    ASSERT_TRUE(run_async(ioc_, redis_->command_executor().execute_hgetall_command_async(
        RedisKeys::workflow_key(workflow_id), workflow_fields)));
    EXPECT_EQ(workflow_fields["status"], std::string(to_string(WorkflowStatus::RUNNING)));
    EXPECT_FALSE(workflow_fields["start_run_time"].empty());

    std::unordered_map<std::string, std::string> job_fields;
    ASSERT_TRUE(run_async(ioc_, redis_->command_executor().execute_hgetall_command_async(
        RedisKeys::job_key(workflow_id, "job-a"), job_fields)));
    EXPECT_EQ(job_fields["status"], std::string(to_string(JobStatus::RUNNING)));

    ASSERT_TRUE(run_async(ioc_, redis_->delete_workflow_runtime_data_async(runtime)));
}

TEST_F(RedisDatabaseAsyncValidatorTest, TrySetJobToRunningRejectsReadyWorkflowWithExistingStartTime)
{
    const WorkflowIdentity workflow_id{"client-" + generate_unique_id(), "workflow-" + generate_unique_id()};
    auto runtime = make_single_job_runtime(workflow_id, WorkflowStatus::READY, JobStatus::QUEUED);
    ASSERT_TRUE(run_async(ioc_, redis_->create_workflow_runtime_data_async(runtime)));
    ASSERT_TRUE(run_async(ioc_, redis_->update_workflow_runtime_async(workflow_id, {{"start_run_time", "123"}})));

    StartJobResult result = StartJobResult::STARTED;
    ASSERT_FALSE(run_async(ioc_, redis_->try_set_job_to_running_async(workflow_id, "job-a", result)));
    EXPECT_EQ(result, StartJobResult::INVARIANT_VIOLATION_WORKFLOW_TIME);

    std::unordered_map<std::string, std::string> job_fields;
    ASSERT_TRUE(run_async(ioc_, redis_->command_executor().execute_hgetall_command_async(
        RedisKeys::job_key(workflow_id, "job-a"), job_fields)));
    EXPECT_EQ(job_fields["status"], std::string(to_string(JobStatus::CANCELED)));

    ASSERT_TRUE(run_async(ioc_, redis_->delete_workflow_runtime_data_async(runtime)));
}

TEST_F(RedisDatabaseAsyncValidatorTest, TrySetJobToRunningCancelsQueuedJobWhenWorkflowIsTerminal)
{
    const WorkflowIdentity workflow_id{"client-" + generate_unique_id(), "workflow-" + generate_unique_id()};
    auto runtime = make_single_job_runtime(workflow_id, WorkflowStatus::FAILED, JobStatus::QUEUED);
    ASSERT_TRUE(run_async(ioc_, redis_->create_workflow_runtime_data_async(runtime)));

    StartJobResult result = StartJobResult::INTERNAL_ERROR;
    ASSERT_FALSE(run_async(ioc_, redis_->try_set_job_to_running_async(workflow_id, "job-a", result)));
    EXPECT_EQ(result, StartJobResult::CANCELED_BY_WORKFLOW_STATE);

    std::unordered_map<std::string, std::string> job_fields;
    ASSERT_TRUE(run_async(ioc_, redis_->command_executor().execute_hgetall_command_async(
        RedisKeys::job_key(workflow_id, "job-a"), job_fields)));
    EXPECT_EQ(job_fields["status"], std::string(to_string(JobStatus::CANCELED)));

    ASSERT_TRUE(run_async(ioc_, redis_->delete_workflow_runtime_data_async(runtime)));
}

TEST_F(RedisDatabaseAsyncValidatorTest, TrySetJobToRunningTreatsCanceledJobAsBenign)
{
    const WorkflowIdentity workflow_id{"client-" + generate_unique_id(), "workflow-" + generate_unique_id()};
    auto runtime = make_single_job_runtime(workflow_id, WorkflowStatus::RUNNING, JobStatus::CANCELED);
    ASSERT_TRUE(run_async(ioc_, redis_->create_workflow_runtime_data_async(runtime)));

    StartJobResult result = StartJobResult::INTERNAL_ERROR;
    ASSERT_FALSE(run_async(ioc_, redis_->try_set_job_to_running_async(workflow_id, "job-a", result)));
    EXPECT_EQ(result, StartJobResult::ALREADY_CANCELED);

    std::unordered_map<std::string, std::string> job_fields;
    ASSERT_TRUE(run_async(ioc_, redis_->command_executor().execute_hgetall_command_async(
        RedisKeys::job_key(workflow_id, "job-a"), job_fields)));
    EXPECT_EQ(job_fields["status"], std::string(to_string(JobStatus::CANCELED)));

    ASSERT_TRUE(run_async(ioc_, redis_->delete_workflow_runtime_data_async(runtime)));
}

TEST_F(RedisDatabaseAsyncValidatorTest, TrySetJobToRunningRejectsInvalidJobStatusesAndFailsWorkflow)
{
    struct StatusCase {
        std::string name;
        std::string value;
        bool present;
    };

    const std::vector<StatusCase> cases{
        {"PENDING", std::string(to_string(JobStatus::PENDING)), true},
        {"READY", std::string(to_string(JobStatus::READY)), true},
        {"RUNNING", std::string(to_string(JobStatus::RUNNING)), true},
        {"COMPLETED", std::string(to_string(JobStatus::COMPLETED)), true},
        {"FAILED", std::string(to_string(JobStatus::FAILED)), true},
        {"UNKNOWN", std::string(to_string(JobStatus::UNKNOWN)), true},
        {"nil", "", false},
    };

    for (const auto& job_case : cases) {
        SCOPED_TRACE("job=" + job_case.name);

        const WorkflowIdentity workflow_id{"client-" + generate_unique_id(), "workflow-" + generate_unique_id()};
        auto runtime = make_single_job_runtime(workflow_id, WorkflowStatus::READY, JobStatus::QUEUED);
        ASSERT_TRUE(run_async(ioc_, redis_->create_workflow_runtime_data_async(runtime)));

        const auto workflow_key = RedisKeys::workflow_key(workflow_id);
        const auto job_key = RedisKeys::job_key(workflow_id, "job-a");

        if (job_case.present) {
            ASSERT_TRUE(run_async(ioc_, redis_->update_job_runtime_async(
                workflow_id,
                "job-a",
                {{"status", job_case.value}})));
        } else {
            long long removed = 0;
            ASSERT_TRUE(run_async(ioc_, redis_->command_executor().execute_integer_command_async(
                {"HDEL", job_key, "status"}, removed)));
        }

        StartJobResult result = StartJobResult::INTERNAL_ERROR;
        ASSERT_FALSE(run_async(ioc_, redis_->try_set_job_to_running_async(workflow_id, "job-a", result)));
        EXPECT_EQ(result, StartJobResult::INVARIANT_VIOLATION_JOB_STATUS);

        std::unordered_map<std::string, std::string> workflow_fields;
        ASSERT_TRUE(run_async(ioc_, redis_->command_executor().execute_hgetall_command_async(
            workflow_key, workflow_fields)));
        EXPECT_EQ(workflow_fields["status"], std::string(to_string(WorkflowStatus::FAILED)));

        std::unordered_map<std::string, std::string> job_fields;
        ASSERT_TRUE(run_async(ioc_, redis_->command_executor().execute_hgetall_command_async(
            job_key, job_fields)));
        const auto job_status_it = job_fields.find("status");
        EXPECT_EQ(job_status_it != job_fields.end(), job_case.present);
        if (job_case.present) {
            EXPECT_EQ(job_status_it->second, job_case.value);
        }

        ASSERT_TRUE(run_async(ioc_, redis_->delete_workflow_runtime_data_async(runtime)));
    }
}

TEST_F(RedisDatabaseAsyncValidatorTest, TrySetJobToRunningCancelsQueuedJobForInvalidWorkflowStatusesAndFailsWorkflow)
{
    struct StatusCase {
        std::string name;
        std::string value;
        bool present;
    };

    const std::vector<StatusCase> cases{
        {"ADMITTED", std::string(to_string(WorkflowStatus::ADMITTED)), true},
        {"COMPLETED", std::string(to_string(WorkflowStatus::COMPLETED)), true},
        {"UNKNOWN", std::string(to_string(WorkflowStatus::UNKNOWN)), true},
        {"nil", "", false},
    };

    for (const auto& workflow_case : cases) {
        SCOPED_TRACE("workflow=" + workflow_case.name);

        const WorkflowIdentity workflow_id{"client-" + generate_unique_id(), "workflow-" + generate_unique_id()};
        auto runtime = make_single_job_runtime(workflow_id, WorkflowStatus::READY, JobStatus::QUEUED);
        ASSERT_TRUE(run_async(ioc_, redis_->create_workflow_runtime_data_async(runtime)));

        const auto workflow_key = RedisKeys::workflow_key(workflow_id);
        const auto job_key = RedisKeys::job_key(workflow_id, "job-a");

        if (workflow_case.present) {
            ASSERT_TRUE(run_async(ioc_, redis_->update_workflow_runtime_async(
                workflow_id,
                {{"status", workflow_case.value}, {"start_run_time", ""}})));
        } else {
            long long removed = 0;
            ASSERT_TRUE(run_async(ioc_, redis_->command_executor().execute_integer_command_async(
                {"HDEL", workflow_key, "status"}, removed)));
        }

        StartJobResult result = StartJobResult::INTERNAL_ERROR;
        ASSERT_FALSE(run_async(ioc_, redis_->try_set_job_to_running_async(workflow_id, "job-a", result)));
        EXPECT_EQ(result, StartJobResult::INVARIANT_VIOLATION_WORKFLOW_STATUS);

        std::unordered_map<std::string, std::string> workflow_fields;
        ASSERT_TRUE(run_async(ioc_, redis_->command_executor().execute_hgetall_command_async(
            workflow_key, workflow_fields)));
        EXPECT_EQ(workflow_fields["status"], std::string(to_string(WorkflowStatus::FAILED)));

        std::unordered_map<std::string, std::string> job_fields;
        ASSERT_TRUE(run_async(ioc_, redis_->command_executor().execute_hgetall_command_async(
            job_key, job_fields)));
        EXPECT_EQ(job_fields["status"], std::string(to_string(JobStatus::CANCELED)));

        ASSERT_TRUE(run_async(ioc_, redis_->delete_workflow_runtime_data_async(runtime)));
    }
}

TEST_F(RedisDatabaseAsyncValidatorTest, TrySetJobToRunningHandlesQueuedJobWorkflowStateCases)
{
    struct StatusCase {
        std::string name;
        WorkflowStatus workflow_status;
        bool succeeds;
        StartJobResult result;
        WorkflowStatus expected_workflow_status;
        JobStatus expected_job_status;
    };

    const std::vector<StatusCase> cases{
        {"READY", WorkflowStatus::READY, true, StartJobResult::FIRST_TO_START, WorkflowStatus::RUNNING, JobStatus::RUNNING},
        {"RUNNING", WorkflowStatus::RUNNING, true, StartJobResult::STARTED, WorkflowStatus::RUNNING, JobStatus::RUNNING},
        {"FAILED", WorkflowStatus::FAILED, false, StartJobResult::CANCELED_BY_WORKFLOW_STATE, WorkflowStatus::FAILED, JobStatus::CANCELED},
        {"CANCELED", WorkflowStatus::CANCELED, false, StartJobResult::CANCELED_BY_WORKFLOW_STATE, WorkflowStatus::CANCELED, JobStatus::CANCELED},
    };

    for (const auto& workflow_case : cases) {
        SCOPED_TRACE("workflow=" + workflow_case.name);

        const WorkflowIdentity workflow_id{"client-" + generate_unique_id(), "workflow-" + generate_unique_id()};
        auto runtime = make_single_job_runtime(workflow_id, workflow_case.workflow_status, JobStatus::QUEUED);
        ASSERT_TRUE(run_async(ioc_, redis_->create_workflow_runtime_data_async(runtime)));

        StartJobResult result = StartJobResult::INTERNAL_ERROR;
        EXPECT_EQ(run_async(ioc_, redis_->try_set_job_to_running_async(workflow_id, "job-a", result)),
                  workflow_case.succeeds);
        EXPECT_EQ(result, workflow_case.result);

        std::unordered_map<std::string, std::string> workflow_fields;
        ASSERT_TRUE(run_async(ioc_, redis_->command_executor().execute_hgetall_command_async(
            RedisKeys::workflow_key(workflow_id), workflow_fields)));
        EXPECT_EQ(workflow_fields["status"], std::string(to_string(workflow_case.expected_workflow_status)));
        if (workflow_case.workflow_status == WorkflowStatus::READY) {
            EXPECT_FALSE(workflow_fields["start_run_time"].empty());
        }

        std::unordered_map<std::string, std::string> job_fields;
        ASSERT_TRUE(run_async(ioc_, redis_->command_executor().execute_hgetall_command_async(
            RedisKeys::job_key(workflow_id, "job-a"), job_fields)));
        EXPECT_EQ(job_fields["status"], std::string(to_string(workflow_case.expected_job_status)));

        ASSERT_TRUE(run_async(ioc_, redis_->delete_workflow_runtime_data_async(runtime)));
    }
}

TEST_F(RedisDatabaseAsyncValidatorTest, SchedulerCanBeRemovedWhileAnotherSchedulerContinuesRunning)
{
    run_async(ioc_, redis_->clear_execution_queue_async());

    auto work_guard = boost::asio::make_work_guard(ioc_);
    ioc_.restart();
    std::thread io_thread([this]() {
        ioc_.run();
    });

    auto removed_scheduler = std::make_unique<scheduler>(1, 1);
    auto active_scheduler = std::make_unique<scheduler>(1, 1);

    removed_scheduler->request_stop();
    removed_scheduler->request_stop();
    removed_scheduler.reset();

    const WorkflowIdentity workflow_id{"client-" + generate_unique_id(), "workflow-" + generate_unique_id()};
    const std::string job_id = "job-" + generate_unique_id();
    JobRuntimeData job_data = make_job_runtime(job_id, JobStatus::QUEUED, 0, 25);

    auto set_runtime_future = boost::asio::co_spawn(
        ioc_,
        redis_->set_job_runtime_async(workflow_id, job_data),
        boost::asio::use_future);
    ASSERT_TRUE(set_runtime_future.get());

    auto set_payload_future = boost::asio::co_spawn(
        ioc_,
        redis_->set_job_payload_async(workflow_id, job_id, {1, 2, 3}),
        boost::asio::use_future);
    ASSERT_TRUE(set_payload_future.get());

    auto enqueue_future = boost::asio::co_spawn(
        ioc_,
        redis_->enqueue_job_for_execution_async(
            workflow_id,
            PrioritizedJob{job_id, job_data.job_uuid, job_data.priority}),
        boost::asio::use_future);
    ASSERT_TRUE(enqueue_future.get());

    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(3);
    bool dequeued = false;
    while (std::chrono::steady_clock::now() < deadline) {
        long long queue_size = -1;
        auto queue_size_future = boost::asio::co_spawn(
            ioc_,
            redis_->get_execution_queue_size_async(queue_size),
            boost::asio::use_future);

        if (queue_size_future.get() && queue_size == 0) {
            dequeued = true;
            break;
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(25));
    }

    EXPECT_TRUE(dequeued);

    active_scheduler->request_stop();
    active_scheduler->request_stop();
    active_scheduler.reset();

    work_guard.reset();
    io_thread.join();

    run_async(ioc_, redis_->clear_execution_queue_async());
    ASSERT_TRUE(run_async(ioc_, redis_->delete_job_payload_async(workflow_id, job_id)));
    ASSERT_TRUE(run_async(ioc_, redis_->delete_all_workflow_jobs_async(workflow_id, {job_data})));
}
