#include <gtest/gtest.h>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/use_future.hpp>
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

TEST_F(RedisDatabaseAsyncValidatorTest, EnqueueReadyJobStoresFullRedisJobKeyAndDequeueParsesIdentity)
{
    run_async(ioc_, redis_->clear_ready_job_async());

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

    std::string ready_job = job_data.job_id;
    ASSERT_TRUE(run_async(ioc_, redis_->enqueue_ready_job_async(workflow_id, ready_job)));

    WorkflowIdentity dequeued_identity;
    std::string dequeued_job_id;
    size_t list_size = 1;
    ASSERT_TRUE(run_async(ioc_, redis_->dequeue_ready_job_async(dequeued_identity, dequeued_job_id, "scheduler-a", list_size)));

    EXPECT_EQ(dequeued_identity.client_id, workflow_id.client_id);
    EXPECT_EQ(dequeued_identity.workflow_id, workflow_id.workflow_id);
    EXPECT_EQ(dequeued_job_id, job_data.job_id);
    EXPECT_EQ(list_size, 0u);

    std::unordered_map<std::string, std::string> fields;
    const std::string job_key = RedisKeys::job_key(workflow_id, job_data.job_id);
    ASSERT_TRUE(run_async(ioc_, redis_->command_executor().execute_hgetall_command_async(job_key, fields)));
    EXPECT_EQ(fields["owned_by"], "scheduler-a");

    ASSERT_TRUE(run_async(ioc_, redis_->delete_all_workflow_jobs_async(workflow_id, {job_data})));
}
