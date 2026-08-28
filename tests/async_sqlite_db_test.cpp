
// async_sqlite_db_test_cpp

#include <gtest/gtest.h>
#include <boost/asio.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/use_future.hpp>

#include "flow_pilot_error_msgs.h"
#include "config.h"
#include "async_db.h"
#include "db_factory.h"
#include "sqlite_db.h"

using namespace flow_pilot;

TEST(AsyncSQLiteDBTest, DuplicateRequestFails) {
    // same in-memory DB instance used for this process; reuse config
    boost::asio::io_context ioc;
    auto& async_db = AsyncDatabase::get_instance();

    RequestData rd;
    rd.client_id = "dup-client";
    rd.request_id = "req-dup";
    rd.workflow_id = "wf-dup";
    rd.workflow_payload_size_bytes = 5;
    rd.status = RequestStatus::RECEIVED;

    StatusCodes err1;
    StatusCodes err2;

    // First add should succeed
    auto f1 = boost::asio::co_spawn(ioc,
        [&]() -> boost::asio::awaitable<bool> {
            co_return co_await async_db.add_request_async(rd, err1);
        },
        boost::asio::use_future);

    ioc.run();
    bool r1 = f1.get();
    ASSERT_TRUE(r1);

    // Second add (same client/request) should fail due to constraint
    boost::asio::io_context ioc2; // new ioc for second call, wrapper reuses instance
    auto& async_db2 = AsyncDatabase::get_instance();
    auto f2 = boost::asio::co_spawn(ioc2,
        [&]() -> boost::asio::awaitable<bool> {
            co_return co_await async_db2.add_request_async(rd, err2);
        },
        boost::asio::use_future);

    ioc2.run();
    bool r2 = f2.get();
    EXPECT_FALSE(r2);
    EXPECT_EQ(err2, StatusCodes::DUPLICATE_REQUEST); // should be duplicate request error
}

TEST(AsyncSQLiteDBTest, UpdateReqStatusAndQueryUserRequests) {
    Config::get().db_config().db_path = ":memory:";
    boost::asio::io_context ioc;
    auto& async_db = AsyncDatabase::get_instance();

    RequestData rd;
    rd.client_id = "active-client";
    rd.request_id = "req-active";
    rd.workflow_id = "wf-active";
    rd.workflow_payload_size_bytes = 20;
    rd.status = RequestStatus::RECEIVED;

    StatusCodes err;

    // Add workflow
    auto fadd = boost::asio::co_spawn(ioc,
        [&]() -> boost::asio::awaitable<bool> {
            co_return co_await async_db.add_request_async(rd, err);
        }, boost::asio::use_future);
    ioc.run();
    ASSERT_TRUE(fadd.get());

    // Update status to ADMITTED via async API
    boost::asio::io_context ioc2;
    auto& async_db2 = AsyncDatabase::get_instance();
    auto fupd = boost::asio::co_spawn(ioc2,
        [&]() -> boost::asio::awaitable<bool> {
            rd.status = RequestStatus::ADMITTED;
            co_return co_await async_db2.update_request_status_async(rd);
        }, boost::asio::use_future);
    ioc2.run();
    ASSERT_TRUE(fupd.get());

    // Query active workflows via async API
    boost::asio::io_context ioc3;
    auto& async_db3 = AsyncDatabase::get_instance();
    std::vector<RequestData> list;
    auto flist = boost::asio::co_spawn(ioc3,
        [&]() -> boost::asio::awaitable<bool> {
            co_return co_await async_db3.get_all_requests_for_client_async(rd.client_id, list);
        }, boost::asio::use_future);
    ioc3.run();
    bool got = flist.get();
    ASSERT_TRUE(got);
    ASSERT_GE(list.size(), 1);
    bool found = false;
    for (auto &r : list) {
        if (r.workflow_id == rd.workflow_id && r.client_id == rd.client_id) {
            found = true;
            EXPECT_EQ(r.status, RequestStatus::ADMITTED);
        }
    }
    EXPECT_TRUE(found);
}

TEST(AsyncSQLiteDBTest, AddAndQueryWorkflow) {
    // Use an in-memory DB for isolation
    Config::get().db_config().db_path = ":memory:";

    boost::asio::io_context ioc;
    auto& async_db = AsyncDatabase::get_instance();

    WorkflowData wf;
    wf.info.client_id = "test-client";
    wf.info.request_id = "req-1";
    wf.info.workflow_id = "wf-1";
    wf.workflow_type = "type-A";
    wf.workflow_version = "v1";
    wf.status = WorkflowStatus::ADMITTED;
    wf.total_jobs = 1;
    wf.info.workflow_payload_size_bytes = 10;

    StatusCodes err;

    auto fut = boost::asio::co_spawn(ioc,
        [&]() -> boost::asio::awaitable<bool> {
            co_return co_await async_db.add_workflow_async(wf, err);
        },
        boost::asio::use_future);

    // Run the io_context; add_workflow_async will offload blocking work to thread pool
    ioc.run();

    bool added = fut.get();
    ASSERT_TRUE(added) << "add_workflow_async failed: " << status_code_to_string(err);

    // Verify via async DB API
    std::vector<WorkflowData> results;
    auto fut2 = boost::asio::co_spawn(ioc,
        [&]() -> boost::asio::awaitable<bool> {
            co_return co_await async_db.get_all_workflows_for_client_async(wf.info.client_id, results);
        },
        boost::asio::use_future);
    ioc.restart();
    ioc.run();
    bool got = fut2.get();
    ASSERT_TRUE(got);
    ASSERT_EQ(results.size(), 1);
    EXPECT_EQ(results[0].info.workflow_id, wf.info.workflow_id);
    EXPECT_EQ(results[0].info.client_id, wf.info.client_id);
}

TEST(AsyncSQLiteDBTest, UpdateWfStatusAndQueryActive) {
    Config::get().db_config().db_path = ":memory:";
    boost::asio::io_context ioc;
    auto& async_db = AsyncDatabase::get_instance();

    WorkflowData wf;
    wf.info.client_id = "active-client";
    wf.info.request_id = "req-active";
    wf.info.workflow_id = "wf-active";
    wf.info.workflow_payload_size_bytes = 20;
    wf.workflow_type = "type-C";
    wf.workflow_version = "v1";
    wf.total_jobs = 2;
    wf.status = WorkflowStatus::ADMITTED;

    StatusCodes err;

    // Add workflow
    auto fadd = boost::asio::co_spawn(ioc,
        [&]() -> boost::asio::awaitable<bool> {
            co_return co_await async_db.add_workflow_async(wf, err);
        }, boost::asio::use_future);
    ioc.run();
    ASSERT_TRUE(fadd.get());

    // Update status to RUNNING via async API
    boost::asio::io_context ioc2;

    auto& async_db2 = AsyncDatabase::get_instance();
    auto fupd = boost::asio::co_spawn(ioc2,
        [&]() -> boost::asio::awaitable<bool> {
            co_return co_await async_db2.update_workflow_status_async(wf.info.client_id, wf.info.workflow_id, WorkflowStatus::RUNNING);
        }, boost::asio::use_future);
    ioc2.run();
    ASSERT_TRUE(fupd.get());

    // Query active workflows via async API
    boost::asio::io_context ioc3;
    auto& async_db3 = AsyncDatabase::get_instance();
    std::vector<WorkflowData> list;
    auto flist = boost::asio::co_spawn(ioc3,
        [&]() -> boost::asio::awaitable<bool> {
            co_return co_await async_db3.get_all_workflows_for_client_async(wf.info.client_id, list);
        }, boost::asio::use_future);
    ioc3.run();
    bool got = flist.get();
    ASSERT_TRUE(got);
    ASSERT_GE(list.size(), 1);
    bool found = false;
    for (auto &w : list) {
        if (w.info.workflow_id == wf.info.workflow_id && w.info.client_id == wf.info.client_id) {
            found = true;
            EXPECT_EQ(w.status, WorkflowStatus::RUNNING);
        }
    }
    EXPECT_TRUE(found);
}

TEST(SQLiteDatabaseTest, ReadyQueuedRunningTransitionsSetExpectedTimestamps) {
    auto& db = SQLiteDatabase::get_instance();
    const std::string client_id = "transition-client";
    const std::string workflow_id = "transition-wf";

    WorkflowData wf;
    wf.info.client_id = client_id;
    wf.info.request_id = "transition-req";
    wf.info.workflow_id = workflow_id;
    wf.workflow_type = "type-transition";
    wf.workflow_version = "v1";
    wf.status = WorkflowStatus::ADMITTED;
    wf.total_jobs = 1;

    StatusCodes err;
    ASSERT_TRUE(db.add_workflow(wf, err));

    WorkflowJobList job_list;
    job_list.client_id = client_id;
    job_list.workflow_id = workflow_id;
    job_list.retry_count = 1;
    job_list.jobs.push_back({"transition-job-uuid", "transition-job", JobStatus::PENDING});
    ASSERT_TRUE(db.add_workflow_jobs(job_list, err));

    WorkflowJob job;
    ASSERT_TRUE(db.get_job_data(client_id, workflow_id, "transition-job", job));
    EXPECT_EQ(job.status, JobStatus::PENDING);
    EXPECT_GT(job.submitted_at, 0);
    EXPECT_EQ(job.ready_at, 0);
    EXPECT_EQ(job.queued_at, 0);
    EXPECT_EQ(job.started_at, 0);

    ASSERT_TRUE(db.update_ready_jobs(client_id, workflow_id, {}, {"transition-job"}));
    ASSERT_TRUE(db.get_job_data(client_id, workflow_id, "transition-job", job));
    const std::time_t ready_at = job.ready_at;
    EXPECT_EQ(job.status, JobStatus::READY);
    EXPECT_GT(ready_at, 0);
    EXPECT_EQ(job.queued_at, 0);

    ASSERT_TRUE(db.update_ready_jobs(client_id, workflow_id, {"transition-job"}, {}));
    ASSERT_TRUE(db.get_job_data(client_id, workflow_id, "transition-job", job));
    EXPECT_EQ(job.status, JobStatus::QUEUED);
    EXPECT_EQ(job.ready_at, ready_at);
    EXPECT_GT(job.queued_at, 0);

    ASSERT_TRUE(db.update_job_status(client_id, workflow_id, "transition-job", JobStatus::RUNNING));
    ASSERT_TRUE(db.get_job_data(client_id, workflow_id, "transition-job", job));
    EXPECT_EQ(job.status, JobStatus::RUNNING);
    EXPECT_GT(job.started_at, 0);

    ASSERT_TRUE(db.update_workflow_status(client_id, workflow_id, WorkflowStatus::READY));
    std::vector<WorkflowData> workflows;
    ASSERT_TRUE(db.get_all_workflows_for_client(client_id, workflows));
    ASSERT_EQ(workflows.size(), 1u);
    EXPECT_EQ(workflows[0].status, WorkflowStatus::READY);
    EXPECT_GT(workflows[0].ready_at, 0);
}
