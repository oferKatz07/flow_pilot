#include <gtest/gtest.h>
#include <boost/asio.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/use_future.hpp>

#include "flow_pilot_error_msgs.h"
#include "config.h"
#include "async_db.h"
#include "db_factory.h"

using namespace flow_pilot;

TEST(AsyncSQLiteDBTest, DuplicateRequestFails) {
    // same in-memory DB instance used for this process; reuse config
    boost::asio::io_context ioc;
    auto& async_db = AsyncDatabase::get_instance();

    RequestData rd;
    rd.client_id = "dup-client";
    rd.request_id = "req-dup";
    rd.workflow_id = "wf-dup";
    to_string(RequestStatus::RECEIVED, rd.status);
    rd.workflow_payload_size_bytes = 5;

    std::string err1;
    std::string err2;

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
    EXPECT_FALSE(err2.empty());
    EXPECT_EQ(err2, error_msg::DUPLICATE_REQUEST); // should be duplicate request error
}

TEST(AsyncSQLiteDBTest, UpdateReqStatusAndQueryUserRequests) {
    Config::get_config().db_config().db_path = ":memory:";
    boost::asio::io_context ioc;
    auto& async_db = AsyncDatabase::get_instance();

    RequestData rd;
    rd.client_id = "active-client";
    rd.request_id = "req-active";
    rd.workflow_id = "wf-active";
    to_string(RequestStatus::RECEIVED, rd.status);
    rd.workflow_payload_size_bytes = 20;

    std::string err;

    // Add workflow
    auto fadd = boost::asio::co_spawn(ioc,
        [&]() -> boost::asio::awaitable<bool> {
            co_return co_await async_db.add_request_async(rd, err);
        }, boost::asio::use_future);
    ioc.run();
    ASSERT_TRUE(fadd.get());

    // Update status to COMPLETED via async API
    std::string complete_status;
    to_string(RequestStatus::COMPLETED, complete_status);
    boost::asio::io_context ioc2;
    auto& async_db2 = AsyncDatabase::get_instance();
    auto fupd = boost::asio::co_spawn(ioc2,
        [&]() -> boost::asio::awaitable<bool> {
            rd.status = complete_status;
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
            EXPECT_EQ(r.status, complete_status);
        }
    }
    EXPECT_TRUE(found);
}

TEST(AsyncSQLiteDBTest, AddAndQueryWorkflow) {
    // Use an in-memory DB for isolation
    Config::get_config().db_config().db_path = ":memory:";

    boost::asio::io_context ioc;
    auto& async_db = AsyncDatabase::get_instance();

    WorkflowfullData wf;
    wf.info.client_id = "test-client";
    wf.info.request_id = "req-1";
    wf.info.workflow_id = "wf-1";
    wf.workflow_type = "type-A";
    wf.workflow_version = "v1";
    to_string(WorkflowStatus::ADMITTED, wf.status);
    wf.total_jobs = 1;
    wf.info.workflow_payload_size_bytes = 10;

    std::string err;

    auto fut = boost::asio::co_spawn(ioc,
        [&]() -> boost::asio::awaitable<bool> {
            co_return co_await async_db.add_workflow_async(wf, err);
        },
        boost::asio::use_future);

    // Run the io_context; add_workflow_async will offload blocking work to thread pool
    ioc.run();

    bool added = fut.get();
    ASSERT_TRUE(added) << "add_workflow_async failed: " << err;

    // Verify via async DB API
    std::vector<WorkflowfullData> results;
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
    Config::get_config().db_config().db_path = ":memory:";
    boost::asio::io_context ioc;
    auto& async_db = AsyncDatabase::get_instance();

    WorkflowfullData wf;
    wf.info.client_id = "active-client";
    wf.info.request_id = "req-active";
    wf.info.workflow_id = "wf-active";
    wf.info.workflow_payload_size_bytes = 20;
    wf.workflow_type = "type-C";
    wf.workflow_version = "v1";
    to_string(WorkflowStatus::ADMITTED, wf.status);
    wf.total_jobs = 2;

    std::string err;

    // Add workflow
    auto fadd = boost::asio::co_spawn(ioc,
        [&]() -> boost::asio::awaitable<bool> {
            co_return co_await async_db.add_workflow_async(wf, err);
        }, boost::asio::use_future);
    ioc.run();
    ASSERT_TRUE(fadd.get());

    // Update status to RUNNING via async API
    boost::asio::io_context ioc2;
    std::string running_status;

    auto& async_db2 = AsyncDatabase::get_instance();
    auto fupd = boost::asio::co_spawn(ioc2,
        [&]() -> boost::asio::awaitable<bool> {
            to_string(WorkflowStatus::RUNNING, running_status);
            co_return co_await async_db2.update_workflow_status_async(wf.info.client_id, wf.info.workflow_id, running_status);
        }, boost::asio::use_future);
    ioc2.run();
    ASSERT_TRUE(fupd.get());

    // Query active workflows via async API
    boost::asio::io_context ioc3;
    auto& async_db3 = AsyncDatabase::get_instance();
    std::vector<WorkflowfullData> list;
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
            EXPECT_EQ(w.status, running_status);
        }
    }
    EXPECT_TRUE(found);
}
