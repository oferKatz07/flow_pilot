#include <gtest/gtest.h>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/use_future.hpp>
#include <boost/uuid/time_generator_v7.hpp>
#include <chrono>
#include <memory>
#include <random>
#include <string>
#include <unordered_map>
#include <vector>

#include "config.h"
#include "database_models.h"
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

void run_async(boost::asio::io_context& ioc, boost::asio::awaitable<void> awaitable)
{
    auto future = boost::asio::co_spawn(ioc, std::move(awaitable), boost::asio::use_future);
    ioc.restart();
    ioc.run();
    future.get();
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

TEST_F(RedisDatabaseAsyncValidatorTest, EnqueueReadyJobStoresFullRedisJobKeyAndDequeueParsesIdentity)
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

    WorkflowIdentity dequeued_identity;
    std::string dequeued_job_id;
    ASSERT_TRUE(run_async(ioc_, redis_->dequeue_job_for_execution_async(dequeued_identity, dequeued_job_id, "scheduler-a")));

    EXPECT_EQ(dequeued_identity.client_id, workflow_id.client_id);
    EXPECT_EQ(dequeued_identity.workflow_id, workflow_id.workflow_id);
    EXPECT_EQ(dequeued_job_id, job_data.job_id);

    std::unordered_map<std::string, std::string> fields;
    const std::string job_key = RedisKeys::job_key(workflow_id, job_data.job_id);
    ASSERT_TRUE(run_async(ioc_, redis_->command_executor().execute_hgetall_command_async(job_key, fields)));
    EXPECT_EQ(fields["owned_by"], "scheduler-a");

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

    WorkflowIdentity dequeued_workflow_id;
    std::string ready_job;
    ASSERT_TRUE(run_async(ioc_, redis_->dequeue_job_for_execution_async(dequeued_workflow_id, ready_job, "scheduler-a")));
    EXPECT_EQ(dequeued_workflow_id.client_id, workflow_id.client_id);
    EXPECT_EQ(dequeued_workflow_id.workflow_id, workflow_id.workflow_id);
    EXPECT_EQ(ready_job, "job-b");

    ASSERT_TRUE(run_async(ioc_, redis_->dequeue_job_for_execution_async(dequeued_workflow_id, ready_job, "scheduler-a")));
    EXPECT_TRUE(ready_job.empty());

    ASSERT_TRUE(run_async(ioc_, redis_->delete_all_workflow_jobs_async(workflow_id, {job_b_runtime})));
}

TEST_F(RedisDatabaseAsyncValidatorTest, DequeueRemovesStaleQueueEntryWithoutCreatingJobHash)
{
    run_async(ioc_, redis_->clear_execution_queue_async());

    const WorkflowIdentity workflow_id{"client-" + generate_unique_id(), "workflow-" + generate_unique_id()};
    const std::string job_id = "job-" + generate_unique_id();

    boost::uuids::time_generator_v7 gen;
    PrioritizedJob ready_job{job_id, gen(), 9};
    ASSERT_TRUE(run_async(ioc_, redis_->enqueue_job_for_execution_async(workflow_id, ready_job)));

    WorkflowIdentity dequeued_identity;
    std::string dequeued_job_id = "stale";
    ASSERT_TRUE(run_async(ioc_,
                          redis_->dequeue_job_for_execution_async(dequeued_identity, dequeued_job_id, "scheduler-stale")));
    EXPECT_TRUE(dequeued_job_id.empty());

    long long queue_size = -1;
    ASSERT_TRUE(run_async(ioc_, redis_->get_execution_queue_size_async(queue_size)));
    EXPECT_EQ(queue_size, 0);

    long long exists = 0;
    std::vector<std::string> exists_args{"EXISTS", RedisKeys::job_key(workflow_id, job_id)};
    ASSERT_TRUE(run_async(ioc_, redis_->command_executor().execute_integer_command_async(exists_args, exists)));
    EXPECT_EQ(exists, 0);
}

TEST_F(RedisDatabaseAsyncValidatorTest, DequeueDoesNotPopJobWhenOwnershipCannotBeAssigned)
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

    WorkflowIdentity dequeued_identity;
    std::string dequeued_job_id;
    EXPECT_FALSE(run_async(ioc_,
                           redis_->dequeue_job_for_execution_async(dequeued_identity, dequeued_job_id, "scheduler-fail")));

    long long queue_size = 0;
    ASSERT_TRUE(run_async(ioc_, redis_->get_execution_queue_size_async(queue_size)));
    EXPECT_EQ(queue_size, 1);

    run_async(ioc_, redis_->clear_execution_queue_async());
    ASSERT_TRUE(run_async(ioc_, redis_->command_executor().execute_integer_command_async(del_args, deleted)));
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
