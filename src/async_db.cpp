
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
    std::string& error_message)
{
    auto executor = pool_.get_executor();
    co_await boost::asio::post(executor, boost::asio::use_awaitable);
    co_return db_.add_request(request_data, error_message);
}

boost::asio::awaitable<bool> AsyncDatabase::add_request_async(
    const RequestData& request_data,
    const std::string& workflow_payload,
    const ClientConfig& client_config,
    std::string& error_message)
{
    auto executor = pool_.get_executor();
    co_await boost::asio::post(executor, boost::asio::use_awaitable);
    co_return db_.add_request(request_data, workflow_payload, client_config, error_message);
}

boost::asio::awaitable<bool> AsyncDatabase::update_request_status_async(
    const RequestData& request_data)
{
    auto executor = pool_.get_executor();
    co_await boost::asio::post(executor, boost::asio::use_awaitable);
    co_return db_.update_request_status(request_data);
}

boost::asio::awaitable<bool> AsyncDatabase::get_all_requests_for_client_async(
    const std::string& client_id,
    std::vector<RequestData>& requests) const
{
    auto executor = pool_.get_executor();
    co_await boost::asio::post(executor, boost::asio::use_awaitable);
    co_return db_.get_all_requests_for_client(client_id, requests);
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
    if (Config::get().db_config().db_type == DBConfig::DBTypes::ASYNC_SQLITE) {
        return SQLiteDatabase::get_instance();
    } else {
        throw std::runtime_error("Unsupported database type! not implemented yet");
    }
}

} // namespace flow_pilot
