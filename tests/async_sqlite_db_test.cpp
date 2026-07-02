#include <gtest/gtest.h>
#include <boost/asio.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/use_future.hpp>

#include "config.h"
#include "async_db.h"
#include "db_factory.h"

using namespace flow_pilot;

TEST(AsyncSQLiteDBTest, AddAndQueryWorkflow) {
    // Use an in-memory DB for isolation
    Config::get_config().db_config().db_path = ":memory:";

    boost::asio::io_context ioc;
    auto& async_db = AsyncDatabase::get_instance();

    RequestData wd;
    wd.client_id = "test-client";
    wd.request_id = "req-1";
    wd.workflow_id = "wf-1";
    wd.workflow_type = "type-A";
    wd.workflow_version = "v1";
    wd.status = "RECEIVED";
    wd.job_count = 1;
    wd.workflow_size_bytes = 10;

    std::string payload = "{}";
    std::string err;

    auto fut = boost::asio::co_spawn(ioc,
        [&]() -> boost::asio::awaitable<bool> {
            co_return co_await async_db.add_workflow_async(wd, payload, err);
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
            co_return co_await async_db.get_all_workflows_for_client_async(wd.client_id, results);
        },
        boost::asio::use_future);
    ioc.restart();
    ioc.run();
    bool got = fut2.get();
    ASSERT_TRUE(got);
    ASSERT_EQ(results.size(), 1);
    EXPECT_EQ(results[0].info.workflow_id, wd.workflow_id);
    EXPECT_EQ(results[0].info.client_id, wd.client_id);
}

TEST(AsyncSQLiteDBTest, DuplicateRequestFails) {
    // same in-memory DB instance used for this process; reuse config
    boost::asio::io_context ioc;
    auto& async_db = AsyncDatabase::get_instance();

    RequestData wd;
    wd.client_id = "dup-client";
    wd.request_id = "req-dup";
    wd.workflow_id = "wf-dup";
    wd.workflow_type = "type-B";
    wd.workflow_version = "v1";
    wd.status = "RECEIVED";
    wd.job_count = 1;
    wd.workflow_size_bytes = 5;

    std::string payload = "{}";
    std::string err1;
    std::string err2;

    // First add should succeed
    auto f1 = boost::asio::co_spawn(ioc,
        [&]() -> boost::asio::awaitable<bool> {
            co_return co_await async_db.add_workflow_async(wd, payload, err1);
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
            co_return co_await async_db2.add_workflow_async(wd, payload, err2);
        },
        boost::asio::use_future);

    ioc2.run();
    bool r2 = f2.get();
    EXPECT_FALSE(r2);
    EXPECT_FALSE(err2.empty());
    EXPECT_NE(err2.find("Duplicate request"), std::string::npos);
}

TEST(AsyncSQLiteDBTest, UpdateStatusAndQueryActive) {
    Config::get_config().db_config().db_path = ":memory:";
    boost::asio::io_context ioc;
    auto& async_db = AsyncDatabase::get_instance();

    RequestData wd;
    wd.client_id = "active-client";
    wd.request_id = "req-active";
    wd.workflow_id = "wf-active";
    wd.workflow_type = "type-C";
    wd.workflow_version = "v1";
    wd.status = "RECEIVED";
    wd.job_count = 2;
    wd.workflow_size_bytes = 20;

    std::string payload = "{}";
    std::string err;

    // Add workflow
    auto fadd = boost::asio::co_spawn(ioc,
        [&]() -> boost::asio::awaitable<bool> {
            co_return co_await async_db.add_workflow_async(wd, payload, err);
        }, boost::asio::use_future);
    ioc.run();
    ASSERT_TRUE(fadd.get());

    // Update status to RUNNING via async API
    boost::asio::io_context ioc2;
    auto& async_db2 = AsyncDatabase::get_instance();
    auto fupd = boost::asio::co_spawn(ioc2,
        [&]() -> boost::asio::awaitable<bool> {
            co_return co_await async_db2.update_workflow_status_async(wd.client_id, wd.workflow_id, "RUNNING");
        }, boost::asio::use_future);
    ioc2.run();
    ASSERT_TRUE(fupd.get());

    // Query active workflows via async API
    boost::asio::io_context ioc3;
    auto& async_db3 = AsyncDatabase::get_instance();
    std::vector<WorkflowfullData> list;
    auto flist = boost::asio::co_spawn(ioc3,
        [&]() -> boost::asio::awaitable<bool> {
            co_return co_await async_db3.get_all_active_workflows_async(list);
        }, boost::asio::use_future);
    ioc3.run();
    bool got = flist.get();
    ASSERT_TRUE(got);
    ASSERT_GE(list.size(), 1);
    bool found = false;
    for (auto &w : list) {
        if (w.info.workflow_id == wd.workflow_id && w.info.client_id == wd.client_id) {
            found = true;
            EXPECT_EQ(w.info.status, "RUNNING");
        }
    }
    EXPECT_TRUE(found);
}
