#include <gtest/gtest.h>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/use_future.hpp>
#include <nlohmann/json.hpp>
#include <chrono>
#include <random>
#include <memory>
#include <unordered_map>

#include "flow_pilot_error_msgs.h"
#include "workflow_admission_service.h"
#include "config.h"
#include "redis_db_async.h"
#include "sqlite_db.h"

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

static ValidationResult submit_workflow_sync(boost::asio::io_context& ioc, WorkflowAdmissionService& service, const std::string& body) {
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

class WorkflowAdmissionServiceTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Use an in-memory SQLite DB for test isolation and deterministic defaults
        Config::get().db_config().db_path = ":memory:";
        Config::get().redis().host = "127.0.0.1";
        Config::get().redis().port = 6379;
        // Use the test client config manager for testing
        Config::get().client_config().config_type = ClientDataConfig::ConfigManagerTypes::TEST_MANAGER;

        try {
            RedisDatabaseAsync::init(shared_redis_ioc, Config::get().redis());
        } catch (const std::exception& ex) {
            GTEST_SKIP() << "Redis is not available for WorkflowService tests: " << ex.what();
        }
        std::string schema_path = "../" + Config::get().workflow().workflow_schema_path;
        service = std::make_unique<WorkflowAdmissionService>(schema_path);
    }

    boost::asio::io_context& ioc_ = shared_redis_ioc;
    std::unique_ptr<WorkflowAdmissionService> service;
};

// Test invalid JSON parsing
TEST_F(WorkflowAdmissionServiceTest, InvalidJson) {
    std::string invalid_json = "{ invalid json }";
    ValidationResult result = submit_workflow_sync(ioc_, *service, invalid_json);
    EXPECT_FALSE(result.valid);
    EXPECT_EQ(result.status_code, StatusCodes::INVALID_JSON_FORMAT);
    EXPECT_FALSE(result.errors_msg.empty());
    EXPECT_TRUE(result.errors_msg.find("parse error") != std::string::npos);
}

// Test missing required fields
TEST_F(WorkflowAdmissionServiceTest, MissingRequiredFields) {
    json workflow;
    workflow["workflow_id"] = "test-001";
    // Missing request_id, client_id, workflow_type and jobs

    ValidationResult result = submit_workflow_sync(ioc_, *service, workflow.dump());
    EXPECT_FALSE(result.valid);
    EXPECT_EQ(result.status_code, StatusCodes::SCHEMA_VALIDATION_FAILED);
    EXPECT_FALSE(result.errors_msg.empty());
}

// Test invalid field types
TEST_F(WorkflowAdmissionServiceTest, InvalidFieldTypes) {
    json workflow;
    workflow["request_id"] = "req-123";
    workflow["client_id"] = "client-123";
    workflow["workflow_id"] = 123;  // Should be string
    workflow["workflow_type"] = "order_processing";
    workflow["jobs"] = "not_an_array";  // Should be array

    ValidationResult result = submit_workflow_sync(ioc_, *service, workflow.dump());
    EXPECT_FALSE(result.valid);
    EXPECT_EQ(result.status_code, StatusCodes::SCHEMA_VALIDATION_FAILED);
    EXPECT_FALSE(result.errors_msg.empty());
}

// Test empty jobs array
TEST_F(WorkflowAdmissionServiceTest, EmptyJobsArray) {
    json workflow;
    workflow["request_id"] = "req-123";
    workflow["client_id"] = "client-123";
    workflow["workflow_id"] = "test-001";
    workflow["workflow_type"] = "order_processing";
    workflow["jobs"] = json::array();  // Empty array

    ValidationResult result = submit_workflow_sync(ioc_, *service, workflow.dump());
    EXPECT_FALSE(result.valid);
    EXPECT_EQ(result.status_code, StatusCodes::SCHEMA_VALIDATION_FAILED);
    EXPECT_FALSE(result.errors_msg.empty());
}

// Test invalid job structure - missing required fields
TEST_F(WorkflowAdmissionServiceTest, InvalidJobStructure) {
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
    EXPECT_FALSE(result.valid);
    EXPECT_EQ(result.status_code, StatusCodes::SCHEMA_VALIDATION_FAILED);
    EXPECT_FALSE(result.errors_msg.empty());
}

// Test invalid job types
TEST_F(WorkflowAdmissionServiceTest, InvalidJobTypes) {
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
    EXPECT_FALSE(result.valid);
    EXPECT_EQ(result.status_code, StatusCodes::SCHEMA_VALIDATION_FAILED);
    EXPECT_FALSE(result.errors_msg.empty());
}

// Test valid minimal workflow
TEST_F(WorkflowAdmissionServiceTest, ValidMinimalWorkflow) {
    json workflow;
    const std::string client_id = "client_1";
    const std::string workflow_id = "test-" + generate_unique_id();
    workflow["request_id"] = "req-" + generate_unique_id();
    workflow["client_id"] = client_id;
    workflow["workflow_id"] = workflow_id;
    workflow["workflow_type"] = "order_processing";

    json job;
    job["job_id"] = "job-" + generate_unique_id();
    job["type"] = "reserve_inventory";
    
    std::string payload = "{\"item_id\", \"123\"}";
    job["payload"] = std::vector<uint8_t>({payload.begin(), payload.end()});
    workflow["jobs"] = json::array({job});

    ValidationResult result = submit_workflow_sync(ioc_, *service, workflow.dump());
    EXPECT_TRUE(result.valid);
    EXPECT_EQ(result.status_code, StatusCodes::WORKFLOW_ADMITTED);
    EXPECT_TRUE(result.errors_msg.empty());

    // Cleanup 
    std::shared_ptr<RedisDatabaseAsync> redis_ =  RedisDatabaseAsync::get_instance();
    EXPECT_TRUE(run_async(shared_redis_ioc,
                          redis_->remove_active_workflow_async(client_id, workflow_id)));


}

// Test valid complex workflow with dependencies
TEST_F(WorkflowAdmissionServiceTest, ValidComplexWorkflow) {
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

    std::string payload;
    json job1;
    job1["job_id"] = "reserve-inventory";
    job1["type"] = "reserve_inventory";
    payload = "{\"item_id\", \"123\"}, {\"quantity\", 2}";
    job1["payload"] = std::vector<uint8_t>({payload.begin(), payload.end()});

    json job2;
    job2["job_id"] = "charge-payment";
    job2["type"] = "charge_payment";
    job2["depends_on"] = json::array({"reserve-inventory"});
    payload = "{\"amount\", 99.99}, {\"currency\", \"USD\"}";
    job2["payload"] = std::vector<uint8_t>({payload.begin(), payload.end()});
    job2["timeout_ms"] = 5000;

    json compensation;
    compensation["job_id"] = "refund-payment";
    compensation["type"] = "refund_payment";
    payload = "{\"reason\", \"workflow_failed\"}";
    compensation["payload"] = std::vector<uint8_t>({payload.begin(), payload.end()});
    job2["compensation"] = compensation;

    workflow["jobs"] = json::array({job1, job2});

    ValidationResult result = submit_workflow_sync(ioc_, *service, workflow.dump());
    std::cout << "Received result with error " << result.errors_msg << "\n\n";
    EXPECT_TRUE(result.valid);
    EXPECT_EQ(result.status_code, StatusCodes::WORKFLOW_ADMITTED);
    EXPECT_TRUE(result.errors_msg.empty());
}

// Test workflow with optional fields
TEST_F(WorkflowAdmissionServiceTest, WorkflowWithOptionalFields) {
    json workflow;
    std::string client_id = "client-" + generate_unique_id();
    workflow["request_id"] = "req-" + generate_unique_id();
    workflow["client_id"] = client_id;
    workflow["workflow_id"] = "test-" + generate_unique_id();
    workflow["workflow_type"] = "order_processing";

    json job;
    job["job_id"] = "job-" + generate_unique_id();
    job["type"] = "reserve_inventory";
    std::string payload = "{\"item_id\", \"123\"}";
    job["payload"] = std::vector<uint8_t>({payload.begin(), payload.end()});
    job["priority"] = 4;
    job["timeout_ms"] = 10000;

    json job_retry_policy;
    job_retry_policy["max_retries"] = 2;
    job_retry_policy["backoff"] = "FIXED";
    job["retry_policy"] = job_retry_policy;

    workflow["jobs"] = json::array({job});

    ValidationResult result = submit_workflow_sync(ioc_, *service, workflow.dump());
    EXPECT_TRUE(result.valid);
    EXPECT_EQ(result.status_code, StatusCodes::WORKFLOW_ADMITTED);
    EXPECT_TRUE(result.errors_msg.empty());
}

TEST_F(WorkflowAdmissionServiceTest, AdmittedWorkflowPersistsSqliteAndRedisState) {
    const std::string client_id = "client-rdbms-" + generate_unique_id();
    const std::string request_id = "req-" + generate_unique_id();
    const std::string workflow_id = "wf-" + generate_unique_id();

    json workflow;
    workflow["request_id"] = request_id;
    workflow["client_id"] = client_id;
    workflow["workflow_id"] = workflow_id;
    workflow["workflow_type"] = "order_processing";

    json first_job;
    first_job["job_id"] = "job-1";
    first_job["type"] = "reserve_inventory";
    std::vector<uint8_t> first_payload = {0x10, 0x20, 0x30, 0x40};
    first_job["payload"] = first_payload;
    workflow["jobs"] = json::array({first_job});

    // Make sure the ready job list is empty
    auto redis = RedisDatabaseAsync::get_instance();
    run_async(shared_redis_ioc,
              redis->clear_ready_job_async());

    const ValidationResult result = submit_workflow_sync(ioc_, *service, workflow.dump());
    EXPECT_TRUE(result.valid);
    EXPECT_EQ(result.status_code, StatusCodes::WORKFLOW_ADMITTED);
    EXPECT_TRUE(result.errors_msg.empty());

    auto& db = SQLiteDatabase::get_instance();

    std::vector<RequestData> requests;
    ASSERT_TRUE(db.get_all_requests_for_client(client_id, requests));
    ASSERT_EQ(requests.size(), 1u);
    EXPECT_EQ(requests[0].client_id, client_id);
    EXPECT_EQ(requests[0].request_id, request_id);
    EXPECT_EQ(requests[0].workflow_id, workflow_id);
    EXPECT_EQ(requests[0].status, RequestStatus::ADMITTED);

    std::vector<WorkflowfullData> workflows;
    ASSERT_TRUE(db.get_all_workflows_for_client(client_id, workflows));
    ASSERT_EQ(workflows.size(), 1u);
    EXPECT_EQ(workflows[0].info.workflow_id, workflow_id);
    EXPECT_EQ(workflows[0].status, WorkflowStatus::READY);
    EXPECT_EQ(workflows[0].total_jobs, 1);

    std::vector<WorkflowJob> persisted_jobs;
    ASSERT_TRUE(db.get_all_jobs_for_workflow(client_id, workflow_id, persisted_jobs));
    ASSERT_EQ(persisted_jobs.size(), 1u);
    EXPECT_EQ(persisted_jobs[0].job_id, "job-1");
    EXPECT_EQ(persisted_jobs[0].status, JobStatus::READY);

    workflow_identity identity{client_id, workflow_id};

    std::unordered_map<std::string, std::string> workflow_runtime;
    ASSERT_TRUE(run_async(shared_redis_ioc,
                          redis->fetch_workflow_runtime_async(identity, workflow_runtime)));
    EXPECT_EQ(workflow_runtime["status"], std::string(to_string(WorkflowStatus::RUNNING)));
    EXPECT_EQ(workflow_runtime["pending_jobs"], "0");

    std::unordered_map<std::string, std::string> job_runtime;
    ASSERT_TRUE(run_async(shared_redis_ioc,
                          redis->fetch_job_runtime_async(identity, "job-1", job_runtime)));
    EXPECT_EQ(job_runtime["status"], std::string(to_string(JobStatus::READY)));
    EXPECT_EQ(job_runtime["remaining_dependencies"], "0");

    std::vector<uint8_t> payload;
    ASSERT_TRUE(run_async(shared_redis_ioc,
                          redis->fetch_job_payload_async(identity, "job-1", payload)));
    EXPECT_EQ(payload, first_payload);

    std::string ready_job_key;
    ASSERT_TRUE(run_async(shared_redis_ioc,
                          redis->dequeue_ready_job_async(ready_job_key)));
    EXPECT_EQ(ready_job_key, "job-1");

    std::shared_ptr<RedisDatabaseAsync> redis_cleanup = RedisDatabaseAsync::get_instance();
    EXPECT_TRUE(run_async(shared_redis_ioc,
                          redis_cleanup->remove_active_workflow_async(client_id, workflow_id)));
    EXPECT_TRUE(run_async(shared_redis_ioc,
                          redis_cleanup->release_request_id_async(client_id, request_id)));
    EXPECT_TRUE(run_async(shared_redis_ioc,
                          redis_cleanup->delete_workflow_runtime_async(identity)));
    EXPECT_TRUE(run_async(shared_redis_ioc,
                          redis_cleanup->delete_job_payload_async(identity, "job-1")));
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
    StatusCodes rejection_code;

    EXPECT_TRUE(run_async(shared_redis_ioc,
                          redis_->admit_request_async(client_id,
                                                      request_id,
                                                      workflow_id,
                                                      5,
                                                      5,
                                                      10,
                                                      rejection_code)));

    std::string status;
    ASSERT_TRUE(run_async(shared_redis_ioc,
                          redis_->fetch_request_status_async(client_id, request_id, status)));
    EXPECT_EQ(status, std::string(to_string(RequestStatus::RECEIVED)));
}

TEST_F(ActualRedisDatabaseTest, DuplicateRequestRejectedWithoutChangingExistingStatus) {
    std::string client_id = "test-client-" + generate_unique_id();
    std::string request_id = "test-request-" + generate_unique_id();
    std::string workflow_id = "workflow-" + generate_unique_id();
    StatusCodes rejection_code;

    EXPECT_TRUE(run_async(shared_redis_ioc,
                          redis_->admit_request_async(client_id,
                                                      request_id,
                                                      workflow_id,
                                                      5,
                                                      5,
                                                      10,
                                                      rejection_code)));
    std::string status_before;
    ASSERT_TRUE(run_async(shared_redis_ioc,
                          redis_->fetch_request_status_async(client_id, request_id, status_before)));
    EXPECT_EQ(status_before, std::string(to_string(RequestStatus::RECEIVED)));

    EXPECT_FALSE(run_async(shared_redis_ioc,
                           redis_->admit_request_async(client_id,
                                                       request_id,
                                                       workflow_id,
                                                       5,
                                                       5,
                                                       10,
                                                       rejection_code)));
    EXPECT_EQ(rejection_code, StatusCodes::DUPLICATE_REQUEST);

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
    StatusCodes rejection_code;

    EXPECT_TRUE(run_async(shared_redis_ioc,
                          redis_->admit_request_async(client_id,
                                                      accepted_request_id,
                                                      accepted_workflow_id,
                                                      5,
                                                      1,
                                                      10,
                                                      rejection_code)));
    std::string accepted_status_before;
    ASSERT_TRUE(run_async(shared_redis_ioc,
                          redis_->fetch_request_status_async(client_id, accepted_request_id, accepted_status_before)));
    EXPECT_EQ(accepted_status_before, std::string(to_string(RequestStatus::RECEIVED)));

    EXPECT_FALSE(run_async(shared_redis_ioc,
                           redis_->admit_request_async(client_id,
                                                       rejected_request_id,
                                                       rejected_workflow_id,
                                                       5,
                                                       1,
                                                       10,
                                                       rejection_code)));
    EXPECT_EQ(rejection_code, StatusCodes::RATE_LIMIT_EXCEEDED);

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
    StatusCodes rejection_code;

    // First request should succeed (max_requests = 1)
    EXPECT_TRUE(run_async(shared_redis_ioc,
                          redis_->admit_request_async(client_id, req1, wf1, 5, 1, 10, rejection_code)));

    // Second request within same window should be rate-limited and not add workflow
    EXPECT_FALSE(run_async(shared_redis_ioc,
                           redis_->admit_request_async(client_id, req2, wf2, 5, 1, 10, rejection_code)));
    EXPECT_EQ(rejection_code, StatusCodes::RATE_LIMIT_EXCEEDED);

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
    StatusCodes rejection_code;

    // First request should occupy the single active slot
    EXPECT_TRUE(run_async(shared_redis_ioc,
                          redis_->admit_request_async(client_id, req1, wf1, 1, 100, 10, rejection_code)));
    // Second request should be rejected due to active limit and should not be added
    EXPECT_FALSE(run_async(shared_redis_ioc,
                           redis_->admit_request_async(client_id, req2, wf2, 1, 100, 10, rejection_code)));
    EXPECT_EQ(rejection_code, StatusCodes::CONCURRENT_WORKFLOW_LIMIT_EXCEEDED);

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
    StatusCodes rejection_code;

    // First admission should succeed
    EXPECT_TRUE(run_async(shared_redis_ioc,
                          redis_->admit_request_async(client_id, req1, wf, 5, 100, 10, rejection_code)));

    // Second admission with a different request_id but same workflow_id should nos succeed
    EXPECT_FALSE(run_async(shared_redis_ioc,
                           redis_->admit_request_async(client_id, req2, wf, 5, 100, 10, rejection_code)));

    // Both request statuses should exist
    std::string status1;
    ASSERT_TRUE(run_async(shared_redis_ioc,
                          redis_->fetch_request_status_async(client_id, req1, status1)));
    EXPECT_EQ(status1, to_string(RequestStatus::RECEIVED));

    std::string status2;
    ASSERT_TRUE(run_async(shared_redis_ioc,
                          redis_->fetch_request_status_async(client_id, req2, status2)));
    EXPECT_EQ(status2, to_string(RequestStatus::REJECTED));

    // Ensure the workflow ID was not duplicated in the active set by removing it once
    EXPECT_TRUE(run_async(shared_redis_ioc,
                          redis_->remove_active_workflow_async(client_id, wf)));
    EXPECT_FALSE(run_async(shared_redis_ioc,
                           redis_->remove_active_workflow_async(client_id, wf)));

    // Cleanup request entries
    run_async(shared_redis_ioc, redis_->release_request_id_async(client_id, req1));
    run_async(shared_redis_ioc, redis_->release_request_id_async(client_id, req2));
}

TEST_F(ActualRedisDatabaseTest, WorkflowRuntimeCreateFetchAndUpdate) {
    struct workflow_identity identiity;
    identiity.client_id = "runtime-client-" + generate_unique_id();
    identiity.workflow_id = "workflow-" + generate_unique_id();

    struct workflow_runtime_data workflow_info;
    workflow_info.workflow_id = identiity.workflow_id;
    workflow_info.status = "READY";
    workflow_info.pending_jobs = 1;
    workflow_info.completed_jobs = 0;
    workflow_info.failed_jobs = 0;

    EXPECT_TRUE(run_async(shared_redis_ioc,
                          redis_->set_workflow_runtime_async(identiity, workflow_info)));

    std::unordered_map<std::string, std::string> workflow_data;
    EXPECT_TRUE(run_async(shared_redis_ioc,
                          redis_->fetch_workflow_runtime_async(identiity,
                                                               workflow_data)));

    EXPECT_EQ(workflow_data["pending_jobs"], "1");
    EXPECT_EQ(workflow_data["completed_jobs"], "0");
    EXPECT_EQ(workflow_data["failed_jobs"], "0");
    EXPECT_EQ(workflow_data["status"], "READY");

    std::unordered_map<std::string, std::string> update_fields;
    update_fields["status"] = "RUNNING";
    update_fields["ready_jobs"] = "0";
    
    EXPECT_TRUE(run_async(shared_redis_ioc,
                          redis_->update_workflow_runtime_async(identiity,
                                                               update_fields)));

    workflow_data.clear();
    EXPECT_TRUE(run_async(shared_redis_ioc,
                          redis_->fetch_workflow_runtime_async(identiity,
                                                               workflow_data)));
    EXPECT_EQ(workflow_data["status"], "RUNNING");
    EXPECT_EQ(workflow_data["ready_jobs"], "0");
}

TEST_F(ActualRedisDatabaseTest, CreateWorkflowRuntimeDataBootstrapsWorkflowState) {
    struct workflow_runtime_info workflow_info;
    // Setting identity data
    workflow_info.identity.client_id = "bootstrap-client-" + generate_unique_id();
    workflow_info.identity.workflow_id = "workflow-" + generate_unique_id();
    // Setting worrkflow data
    workflow_info.workflow.workflow_id = workflow_info.identity.workflow_id;
    workflow_info.workflow.status = to_string(WorkflowStatus::READY);
    workflow_info.workflow.pending_jobs = 1;
    workflow_info.workflow.completed_jobs = 0;
    workflow_info.workflow.failed_jobs = 0;

    // Setting jobs data
    job_runtime_data job1;
    job1.job_id = "job-a";
    job1.status = to_string(JobStatus::READY);
    job1.remaining_dependencies = 0;
    job1.max_retries = 3;
    job1.current_retry_count = 0;
    job1.priority = 2;
    job1.retry_delay_sec = 0;
    job1.timeout_sec = 5;
    job1.retry_backoff_policy = "IMMEDIATE";
    job1.successors.push_back("job-b");

    job_runtime_data job2;
    job2.job_id = "job-b";
    job2.status = to_string(JobStatus::PENDING);
    job2.remaining_dependencies = 1;
    job1.max_retries = 3;
    job1.current_retry_count = 0;
    job1.priority = 3;
    job1.retry_delay_sec = 1;
    job1.timeout_sec = 4;
    job1.retry_backoff_policy = "IMMEDIATE";

   
    workflow_info.jobs = {job1, job2};

    EXPECT_TRUE(run_async(shared_redis_ioc,
                          redis_->create_workflow_runtime_data_async(workflow_info)));

    std::unordered_map<std::string, std::string> workflow_data;
    ASSERT_TRUE(run_async(shared_redis_ioc,
                          redis_->fetch_workflow_runtime_async(workflow_info.identity,
                                                               workflow_data)));
    EXPECT_EQ(workflow_data["status"], workflow_info.workflow.status);
    EXPECT_EQ(workflow_data["pending_jobs"], "1");

    std::unordered_map<std::string, std::string> job_data;
    ASSERT_TRUE(run_async(shared_redis_ioc,
                          redis_->fetch_job_runtime_async(workflow_info.identity,
                                                         "job-a",
                                                         job_data)));
    EXPECT_EQ(job_data["status"], job1.status);
    EXPECT_EQ(job_data["remaining_dependencies"], std::to_string(job1.remaining_dependencies));
    EXPECT_EQ(job_data["max_retries"], std::to_string(job1.max_retries));
    EXPECT_EQ(job_data["current_retry_count"], std::to_string(job1.current_retry_count));
    EXPECT_EQ(job_data["priority"], std::to_string(job1.priority));
    EXPECT_EQ(job_data["retry_delay_sec"], std::to_string(job1.retry_delay_sec));
    EXPECT_EQ(job_data["timeout_sec"], std::to_string(job1.timeout_sec));
    EXPECT_EQ(job_data["retry_backoff_policy"], job1.retry_backoff_policy);
}

TEST_F(ActualRedisDatabaseTest, JobRuntimePayloadAndDependenciesRoundtrip) {
    struct workflow_identity workflow_id;
    workflow_id.client_id = "job-client-" + generate_unique_id();
    workflow_id.workflow_id = "workflow-" + generate_unique_id();
    
    std::string job_id = "job-" + generate_unique_id();
    struct job_runtime_data job_info;
    job_info.job_id = job_id;
    job_info.status = to_string(JobStatus::PENDING);
    job_info.remaining_dependencies = 1;
    job_info.max_retries = 2;
    job_info.current_retry_count = 0;
    job_info.priority = 1;
    job_info.retry_delay_sec = 3;
    job_info.timeout_sec = 2;
    job_info.retry_backoff_policy = "IMMEDIATE";

    EXPECT_TRUE(run_async(shared_redis_ioc,
                          redis_->set_job_runtime_async(workflow_id, job_info)));

    std::unordered_map<std::string, std::string> job_data;
    EXPECT_TRUE(run_async(shared_redis_ioc,
                          redis_->fetch_job_runtime_async(workflow_id,
                                                         job_id,
                                                         job_data)));
    EXPECT_EQ(job_data["status"], job_info.status);
    EXPECT_EQ(job_data["remaining_dependencies"], std::to_string(job_info.remaining_dependencies));
    EXPECT_EQ(job_data["max_retries"], std::to_string(job_info.max_retries));
    EXPECT_EQ(job_data["current_retry_count"], std::to_string(job_info.current_retry_count));
    EXPECT_EQ(job_data["priority"], std::to_string(job_info.priority));
    EXPECT_EQ(job_data["retry_delay_sec"], std::to_string(job_info.retry_delay_sec));
    EXPECT_EQ(job_data["timeout_sec"], std::to_string(job_info.timeout_sec));
    EXPECT_EQ(job_data["retry_backoff_policy"], job_info.retry_backoff_policy);
}

int main(int argc, char **argv) {

    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
