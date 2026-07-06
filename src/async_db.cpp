
// async_db.cpp - Asynchronous database interface implementation for FlowPilot

#include <algorithm>
#include <boost/asio/post.hpp>
#include <boost/asio/thread_pool.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <mutex>
#include <thread>
#include "sqlite_db.h"
#include "config.h"
#include "async_db.h"

namespace flow_pilot {

IAsyncDatabase& AsyncDatabase::get_instance()
{
    // Ensure the instance is initialized before returning it
    static AsyncDatabase instance = AsyncDatabase(); 

    return instance;
}

AsyncDatabase::AsyncDatabase() : db_(get_db_instance())
{
}

boost::asio::awaitable<bool> AsyncDatabase::add_request_async(
    const RequestData& request_data,
    const std::string& workflow_payload,
    std::string& error_message)
{
    auto executor = pool_.get_executor();
    co_await boost::asio::post(executor, boost::asio::use_awaitable);
    co_return db_.add_request(request_data, workflow_payload, error_message);
}

boost::asio::awaitable<bool> AsyncDatabase::add_request_async(
    const RequestData& request_data,
    const std::string& workflow_payload,
    const int max_requests,
    std::string& error_message)
{
    (void)max_requests;
    auto executor = pool_.get_executor();
    co_await boost::asio::post(executor, boost::asio::use_awaitable);
    co_return db_.add_request(request_data, workflow_payload, error_message);
}

boost::asio::awaitable<bool> AsyncDatabase::add_workflow_async(
    const WorkflowfullData& workflow_data,
    std::string& error_message)
{
    auto executor = pool_.get_executor();
    co_await boost::asio::post(executor, boost::asio::use_awaitable);
    co_return db_.add_workflow(workflow_data, error_message);
}

boost::asio::awaitable<bool> AsyncDatabase::update_workflow_status_async(
    const std::string& client_id,
    const std::string& workflow_id,
    const std::string& status)
{
    auto executor = pool_.get_executor();
    co_await boost::asio::post(executor, boost::asio::use_awaitable);
    co_return db_.update_workflow_status(client_id, workflow_id, status);
}

boost::asio::awaitable<bool> AsyncDatabase::get_all_active_workflows_async(
    std::vector<WorkflowfullData>& workflows) const
{
    auto executor = pool_.get_executor();
    co_await boost::asio::post(executor, boost::asio::use_awaitable);
    co_return db_.get_all_active_workflows(workflows);
}

boost::asio::awaitable<bool> AsyncDatabase::get_all_workflows_for_client_async(
    const std::string& client_id,
    std::vector<WorkflowfullData>& workflows) const
{
    auto executor = pool_.get_executor();
    co_await boost::asio::post(executor, boost::asio::use_awaitable);
    co_return db_.get_all_workflows_for_client(client_id, workflows);
}

IDatabase& AsyncDatabase::get_db_instance()
{
    if (Config::get_config().db_config().db_type == DBConfig::DBType::ASYNC_SQLITE) {
        return SQLiteDatabase::get_instance();
    } else {
        throw std::runtime_error("Unsupported database type! not implemented yet");
    }
}

} // namespace flow_pilot
