#include <gtest/gtest.h>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/use_future.hpp>
#include <nlohmann/json.hpp>
#include <chrono>
#include <random>
#include <memory>

#include "flow_pilot_error_msgs.h"
#include "workflow_service.h"
#include "config.h"
#include "redis_db_async.h"

using namespace flow_pilot;
using json = nlohmann::json;

static boost::asio::io_context shared_redis_ioc;

static std::string generate_unique_id()
{
    static std::mt19937_64 rng(static_cast<unsigned long long>(std::chrono::high_resolution_clock::now().time_since_epoch().count()));
    std::uniform_int_distribution<unsigned long long> dist;
    return std::to_string(dist(rng));
}

template <typename T>
static T run_async(boost::asio::io_context& ioc, boost::asio::awaitable<T> awaitable)
{
    auto future = boost::asio::co_spawn(ioc, std::move(awaitable), boost::asio::use_future);
    ioc.restart();
    ioc.run();
    return future.get();
}

static ValidationResult submit_workflow_sync(boost::asio::io_context& ioc, WorkflowService& service, const std::string& body) {
    auto fut = boost::asio::co_spawn(ioc,
        [&]( ) -> boost::asio::awaitable<ValidationResult> {
            co_return co_await service.submit_workflow(body);
        },
        boost::asio::use_future);
    ioc.restart();
    ioc.run();
    return fut.get();
}

static json make_valid_workflow()
{
    json workflow;
    workflow["request_id"] = "req-123";
    workflow["client_id"] = "client-123";
    workflow["workflow_id"] = "test-001";
    workflow["workflow_type"] = "order_processing";

    json job;
    job["job_id"] = "job-1";
    job["type"] = "reserve_inventory";
    job["payload"] = json::object({{"item_id", "123"}});
    workflow["jobs"] = json::array({job});

    return workflow;
}

class WorkflowServiceTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Use an in-memory SQLite DB for test isolation and deterministic defaults
        Config::get().db_config().db_path = ":memory:";
        Config::get().redis().host = "127.0.0.1";
        Config::get().redis().port = 6379;

        try {
            RedisDatabaseAsync::init(shared_redis_ioc, Config::get().redis());
        } catch (const std::exception& ex) {
            GTEST_SKIP() << "Redis is not available for WorkflowService tests: " << ex.what();
        }
        std::string schema_path = "../" + Config::get().workflow().workflow_schema_path;
        service = std::make_unique<WorkflowService>(schema_path);
    }

    boost::asio::io_context& ioc_ = shared_redis_ioc;
    std::unique_ptr<WorkflowService> service;
};

// Test invalid JSON parsing
TEST_F(WorkflowServiceTest, InvalidJson) {
    std::string invalid_json = "{ invalid json }";
    ValidationResult result = submit_workflow_sync(ioc_, *service, invalid_json);

    EXPECT_FALSE(result.valid);
    EXPECT_EQ(result.status_str, error_msgs::INVALID_JSON_FORMAT);
    EXPECT_FALSE(result.errors_msg.empty());
    EXPECT_TRUE(result.errors_msg.find("parse error") != std::string::npos);
}

// Test missing required fields
TEST_F(WorkflowServiceTest, MissingRequiredFields) {
    json workflow;
    workflow["workflow_id"] = "test-001";
    // Missing request_id, client_id, workflow_type and jobs

    ValidationResult result = submit_workflow_sync(ioc_, *service, workflow.dump());

    EXPECT_FALSE(result.valid);
    EXPECT_EQ(result.status_str, error_msgs::SCHEMA_VALIDATION_FAILED);
    EXPECT_FALSE(result.errors_msg.empty());
}

// Test invalid field types
TEST_F(WorkflowServiceTest, InvalidFieldTypes) {
    json workflow;
    workflow["request_id"] = "req-123";
    workflow["client_id"] = "client-123";
    workflow["workflow_id"] = 123;  // Should be string
    workflow["workflow_type"] = "order_processing";
    workflow["jobs"] = "not_an_array";  // Should be array

    ValidationResult result = submit_workflow_sync(ioc_, *service, workflow.dump());

    EXPECT_FALSE(result.valid);
    EXPECT_EQ(result.status_str, error_msgs::SCHEMA_VALIDATION_FAILED);
    EXPECT_FALSE(result.errors_msg.empty());
}

// Test empty jobs array
TEST_F(WorkflowServiceTest, EmptyJobsArray) {
    json workflow;
    workflow["request_id"] = "req-123";
    workflow["client_id"] = "client-123";
    workflow["workflow_id"] = "test-001";
    workflow["workflow_type"] = "order_processing";
    workflow["jobs"] = json::array();  // Empty array

    ValidationResult result = submit_workflow_sync(ioc_, *service, workflow.dump());
    std::cout << "Returned ValidationResult: " << result.errors_msg << ", " << result.status_str << std::endl;

    EXPECT_FALSE(result.valid);
    EXPECT_EQ(result.status_str, error_msgs::SCHEMA_VALIDATION_FAILED);
    EXPECT_FALSE(result.errors_msg.empty());
}

// Test invalid job structure - missing required fields
TEST_F(WorkflowServiceTest, InvalidJobStructure) {
    json workflow;
    workflow["request_id"] = "req-123";
    workflow["client_id"] = "client-123";
    workflow["workflow_id"] = "test-001";
    workflow["workflow_type"] = "order_processing";

    json job1;
    job1["job_id"] = "job-1";
    // Missing type and payload

    json job2;
    job2["type"] = "reserve_inventory";
    job2["payload"] = json::object();
    // Missing job_id

    workflow["jobs"] = json::array({job1, job2});

    ValidationResult result = submit_workflow_sync(ioc_, *service, workflow.dump());
    std::cout << "Returned ValidationResult: " << result.errors_msg << ", " << result.status_str << std::endl;

    EXPECT_FALSE(result.valid);
    EXPECT_EQ(result.status_str, error_msgs::SCHEMA_VALIDATION_FAILED);
    EXPECT_FALSE(result.errors_msg.empty());
}

// Test invalid job types
TEST_F(WorkflowServiceTest, InvalidJobTypes) {
    json workflow;
    workflow["request_id"] = "req-123";
    workflow["client_id"] = "client-123";
    workflow["workflow_id"] = "test-001";
    workflow["workflow_type"] = "order_processing";

    json job;
    job["job_id"] = 123;  // Should be string
    job["type"] = 456;    // Should be string
    job["payload"] = "not_an_object";  // Should be object

    workflow["jobs"] = json::array({job});

    ValidationResult result = submit_workflow_sync(ioc_, *service, workflow.dump());
    std::cout << "Returned ValidationResult: " << result.errors_msg << ", " << result.status_str << std::endl;

    EXPECT_FALSE(result.valid);
    EXPECT_EQ(result.status_str, error_msgs::SCHEMA_VALIDATION_FAILED);
    EXPECT_FALSE(result.errors_msg.empty());
}

// Test valid minimal workflow
TEST_F(WorkflowServiceTest, ValidMinimalWorkflow) {
    json workflow;
    workflow["request_id"] = "req-" + generate_unique_id();
    workflow["client_id"] = "client_1";
    workflow["workflow_id"] = "test-" + generate_unique_id();
    workflow["workflow_type"] = "order_processing";

    json job;
    job["job_id"] = "job-" + generate_unique_id();
    job["type"] = "reserve_inventory";
    job["payload"] = json::object({{"item_id", "123"}});

    workflow["jobs"] = json::array({job});

    ValidationResult result = submit_workflow_sync(ioc_, *service, workflow.dump());
    std::cout << "Returned ValidationResult: " << result.errors_msg << ", " << result.status_str << std::endl;

    EXPECT_TRUE(result.valid);
    EXPECT_EQ(result.status_str, error_msgs::WORKFLOW_ADMITTED);
    EXPECT_TRUE(result.errors_msg.empty());
}

// Test valid complex workflow with dependencies
TEST_F(WorkflowServiceTest, ValidComplexWorkflow) {
    json workflow;
    std::string client_id = "client-" + generate_unique_id();
    workflow["request_id"] = "req-" + generate_unique_id();
    workflow["client_id"] = client_id;
    workflow["workflow_id"] = "order-" + generate_unique_id();
    workflow["workflow_type"] = "order_processing";
    workflow["created_by"] = "test-user";
    workflow["priority"] = 5;
    workflow["idempotency_key"] = "unique-key-" + generate_unique_id();
    workflow["failure_policy"] = "FAIL_FAST";

    json retry_policy;
    retry_policy["max_retries"] = 3;
    retry_policy["backoff"] = "EXPONENTIAL";
    retry_policy["initial_delay_ms"] = 1000;
    retry_policy["max_delay_ms"] = 30000;
    workflow["retry_policy"] = retry_policy;

    json job1;
    job1["job_id"] = "reserve-inventory";
    job1["type"] = "reserve_inventory";
    job1["payload"] = json::object({{"item_id", "123"}, {"quantity", 2}});

    json job2;
    job2["job_id"] = "charge-payment";
    job2["type"] = "charge_payment";
    job2["depends_on"] = json::array({"reserve-inventory"});
    job2["payload"] = json::object({{"amount", 99.99}, {"currency", "USD"}});
    job2["timeout_ms"] = 5000;

    json compensation;
    compensation["job_id"] = "refund-payment";
    compensation["type"] = "refund_payment";
    compensation["payload"] = json::object({{"reason", "workflow_failed"}});
    job2["compensation"] = compensation;

    workflow["jobs"] = json::array({job1, job2});

    ValidationResult result = submit_workflow_sync(ioc_, *service, workflow.dump());
    std::cout << "Returned ValidationResult: " << result.errors_msg << ", " << result.status_str << std::endl;
    EXPECT_TRUE(result.valid);
    EXPECT_EQ(result.status_str, error_msgs::WORKFLOW_ADMITTED);
    EXPECT_TRUE(result.errors_msg.empty());
}

// Test workflow with optional fields
TEST_F(WorkflowServiceTest, WorkflowWithOptionalFields) {
    json workflow;
    std::string client_id = "client-" + generate_unique_id();
    workflow["request_id"] = "req-" + generate_unique_id();
    workflow["client_id"] = client_id;
    workflow["workflow_id"] = "test-" + generate_unique_id();
    workflow["workflow_type"] = "order_processing";

    json job;
    job["job_id"] = "job-" + generate_unique_id();
    job["type"] = "reserve_inventory";
    job["payload"] = json::object({{"item_id", "123"}});
    job["priority"] = 8;
    job["timeout_ms"] = 10000;

    json job_retry_policy;
    job_retry_policy["max_retries"] = 2;
    job_retry_policy["backoff"] = "FIXED";
    job["retry_policy"] = job_retry_policy;

    workflow["jobs"] = json::array({job});

    ValidationResult result = submit_workflow_sync(ioc_, *service, workflow.dump());
    std::cout << "Returned ValidationResult: " << result.errors_msg << ", " << result.status_str << std::endl;
    EXPECT_TRUE(result.valid);
    EXPECT_EQ(result.status_str, error_msgs::WORKFLOW_ADMITTED);
    EXPECT_TRUE(result.errors_msg.empty());
}


TEST(RedisDatabaseTest, InvalidConnectionStringFails) {
    std::shared_ptr<RedisDatabaseAsync> redis;
    try {
        redis = RedisDatabaseAsync::init(shared_redis_ioc);
    } catch (const std::exception& ex) {
        GTEST_SKIP() << "Redis unavailable for RedisDatabaseTest: " << ex.what();
    }

    EXPECT_FALSE(redis->connect("redis://localhost"));
    EXPECT_FALSE(redis->connect("redis://"));
}

class ActualRedisDatabaseTest : public ::testing::Test {
protected:
    void SetUp() override {
        try {
            redis_ = RedisDatabaseAsync::init(shared_redis_ioc);
        } catch (const std::exception& ex) {
            GTEST_SKIP() << "Redis server is not available: " << ex.what();
        }
    }

    std::shared_ptr<RedisDatabaseAsync> redis_;
};

TEST_F(ActualRedisDatabaseTest, AdmitRequestLuaCreatesValidatingEntry) {
    std::string client_id = "test-client-" + generate_unique_id();
    std::string request_id = "test-request-" + generate_unique_id();
    std::string workflow_id = "workflow-" + generate_unique_id();
    std::string rejection_reason;

    EXPECT_TRUE(run_async(shared_redis_ioc,
                          redis_->admit_request_async(client_id,
                                                      request_id,
                                                      workflow_id,
                                                      5,
                                                      5,
                                                      10,
                                                      rejection_reason)));
    EXPECT_TRUE(rejection_reason.empty());

    std::string status;
    ASSERT_TRUE(run_async(shared_redis_ioc,
                          redis_->fetch_request_status_async(client_id, request_id, status)));
    EXPECT_EQ(status, "RECEIVED");
}

TEST_F(ActualRedisDatabaseTest, DuplicateRequestRejectedWithoutChangingExistingStatus) {
    std::string client_id = "test-client-" + generate_unique_id();
    std::string request_id = "test-request-" + generate_unique_id();
    std::string workflow_id = "workflow-" + generate_unique_id();
    std::string rejection_reason;

    EXPECT_TRUE(run_async(shared_redis_ioc,
                          redis_->admit_request_async(client_id,
                                                      request_id,
                                                      workflow_id,
                                                      5,
                                                      5,
                                                      10,
                                                      rejection_reason)));
    std::string status_before;
    ASSERT_TRUE(run_async(shared_redis_ioc,
                          redis_->fetch_request_status_async(client_id, request_id, status_before)));
    EXPECT_EQ(status_before, "RECEIVED");

    EXPECT_FALSE(run_async(shared_redis_ioc,
                           redis_->admit_request_async(client_id,
                                                       request_id,
                                                       workflow_id,
                                                       5,
                                                       5,
                                                       10,
                                                       rejection_reason)));
    EXPECT_EQ(rejection_reason, "Duplicate request detected");

    std::string status_after;
    ASSERT_TRUE(run_async(shared_redis_ioc,
                          redis_->fetch_request_status_async(client_id, request_id, status_after)));
    EXPECT_EQ(status_after, status_before);
}

TEST_F(ActualRedisDatabaseTest, RejectedRequestDoesNotAffectPreExistingRequestStatus) {
    std::string client_id = "test-client-" + generate_unique_id();
    std::string accepted_request_id = "accepted-request-" + generate_unique_id();
    std::string accepted_workflow_id = "workflow-" + generate_unique_id();
    std::string rejected_request_id = "rejected-request-" + generate_unique_id();
    std::string rejected_workflow_id = "workflow-" + generate_unique_id();
    std::string rejection_reason;

    EXPECT_TRUE(run_async(shared_redis_ioc,
                          redis_->admit_request_async(client_id,
                                                      accepted_request_id,
                                                      accepted_workflow_id,
                                                      5,
                                                      1,
                                                      10,
                                                      rejection_reason)));
    std::string accepted_status_before;
    ASSERT_TRUE(run_async(shared_redis_ioc,
                          redis_->fetch_request_status_async(client_id, accepted_request_id, accepted_status_before)));
    EXPECT_EQ(accepted_status_before, "RECEIVED");

    EXPECT_FALSE(run_async(shared_redis_ioc,
                           redis_->admit_request_async(client_id,
                                                       rejected_request_id,
                                                       rejected_workflow_id,
                                                       5,
                                                       1,
                                                       10,
                                                       rejection_reason)));
    EXPECT_EQ(rejection_reason, "Rate limit exceeded: too many workflow requests in the recent time window.");

    std::string accepted_status_after;
    ASSERT_TRUE(run_async(shared_redis_ioc,
                          redis_->fetch_request_status_async(client_id, accepted_request_id, accepted_status_after)));
    EXPECT_EQ(accepted_status_after, accepted_status_before);
}

TEST_F(ActualRedisDatabaseTest, AdmitRequest_RateLimitRejected_DoesNotAddWorkflow) {
    std::string client_id = "test-client-" + generate_unique_id();
    std::string req1 = "req-" + generate_unique_id();
    std::string wf1 = "wf-" + generate_unique_id();
    std::string req2 = "req-" + generate_unique_id();
    std::string wf2 = "wf-" + generate_unique_id();
    std::string rejection_reason;

    // First request should succeed (max_requests = 1)
    EXPECT_TRUE(run_async(shared_redis_ioc,
                          redis_->admit_request_async(client_id, req1, wf1, 5, 1, 10, rejection_reason)));
    EXPECT_TRUE(rejection_reason.empty());

    // Second request within same window should be rate-limited and not add workflow
    EXPECT_FALSE(run_async(shared_redis_ioc,
                           redis_->admit_request_async(client_id, req2, wf2, 5, 1, 10, rejection_reason)));
    EXPECT_EQ(rejection_reason, "Rate limit exceeded: too many workflow requests in the recent time window.");

    // Ensure wf2 was not left in the active set
    EXPECT_FALSE(run_async(shared_redis_ioc,
                           redis_->remove_active_workflow_async(client_id, wf2)));

    // Cleanup
    run_async(shared_redis_ioc, redis_->remove_active_workflow_async(client_id, wf1));
    run_async(shared_redis_ioc, redis_->release_request_id_async(client_id, req1));
    run_async(shared_redis_ioc, redis_->release_request_id_async(client_id, req2));
}

TEST_F(ActualRedisDatabaseTest, AdmitRequest_ActiveLimitRejected_RollsBack) {
    std::string client_id = "test-client-" + generate_unique_id();
    std::string req1 = "req-" + generate_unique_id();
    std::string wf1 = "wf-" + generate_unique_id();
    std::string req2 = "req-" + generate_unique_id();
    std::string wf2 = "wf-" + generate_unique_id();
    std::string rejection_reason;

    // First request should occupy the single active slot
    EXPECT_TRUE(run_async(shared_redis_ioc,
                          redis_->admit_request_async(client_id, req1, wf1, 1, 100, 10, rejection_reason)));
    EXPECT_TRUE(rejection_reason.empty());

    // Second request should be rejected due to active limit and should not be added
    EXPECT_FALSE(run_async(shared_redis_ioc,
                           redis_->admit_request_async(client_id, req2, wf2, 1, 100, 10, rejection_reason)));
    EXPECT_EQ(rejection_reason, "Client exceeded the maximum allowed concurrent workflows.");

    // Ensure wf2 was not left in the active set
    EXPECT_FALSE(run_async(shared_redis_ioc,
                           redis_->remove_active_workflow_async(client_id, wf2)));

    // Cleanup
    run_async(shared_redis_ioc, redis_->remove_active_workflow_async(client_id, wf1));
    run_async(shared_redis_ioc, redis_->release_request_id_async(client_id, req1));
    run_async(shared_redis_ioc, redis_->release_request_id_async(client_id, req2));
}

TEST_F(ActualRedisDatabaseTest, AdmitRequest_ExistingWorkflowIdIsIdempotent) {
    std::string client_id = "test-client-" + generate_unique_id();
    std::string req1 = "req-" + generate_unique_id();
    std::string req2 = "req-" + generate_unique_id();
    std::string wf = "wf-" + generate_unique_id();
    std::string rejection_reason;

    // First admission should succeed
    EXPECT_TRUE(run_async(shared_redis_ioc,
                          redis_->admit_request_async(client_id, req1, wf, 5, 100, 10, rejection_reason)));
    EXPECT_TRUE(rejection_reason.empty());

    // Second admission with a different request_id but same workflow_id should also succeed
    EXPECT_TRUE(run_async(shared_redis_ioc,
                          redis_->admit_request_async(client_id, req2, wf, 5, 100, 10, rejection_reason)));
    EXPECT_TRUE(rejection_reason.empty());

    // Both request statuses should exist
    std::string status1;
    ASSERT_TRUE(run_async(shared_redis_ioc,
                          redis_->fetch_request_status_async(client_id, req1, status1)));
    EXPECT_EQ(status1, "RECEIVED");

    std::string status2;
    ASSERT_TRUE(run_async(shared_redis_ioc,
                          redis_->fetch_request_status_async(client_id, req2, status2)));
    EXPECT_EQ(status2, "RECEIVED");

    // Ensure the workflow ID was not duplicated in the active set by removing it once
    EXPECT_TRUE(run_async(shared_redis_ioc,
                          redis_->remove_active_workflow_async(client_id, wf)));
    EXPECT_FALSE(run_async(shared_redis_ioc,
                           redis_->remove_active_workflow_async(client_id, wf)));

    // Cleanup request entries
    run_async(shared_redis_ioc, redis_->release_request_id_async(client_id, req1));
    run_async(shared_redis_ioc, redis_->release_request_id_async(client_id, req2));
}

int main(int argc, char **argv) {

    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
