// async_db.h - Asynchronous database API wrapper to provide async IO support to databases 
//              which do not have native support to async IO. 
//              This is done by using a thread pool to run the synchronous database operations 
//              in a separate thread, and then returning the result to the caller via a future.

#pragma once

#include <boost/asio/thread_pool.hpp>
#include "async_db_interface.h"

namespace flow_pilot {

class AsyncDatabase : public IAsyncDatabase {
public:
    ~AsyncDatabase() = default;

    // Delete copy/move
    AsyncDatabase(const AsyncDatabase&) = delete;
    AsyncDatabase& operator=(const AsyncDatabase&) = delete;
    AsyncDatabase(AsyncDatabase&&) = delete;
    AsyncDatabase& operator=(AsyncDatabase&&) = delete;

    // Get the singleton instance. Throws if not initialized.
    static IAsyncDatabase& get_instance();

    boost::asio::awaitable<bool> add_request_async(
        const RequestData& request_data,
        const std::string& workflow_payload,
        std::string& error_message) override;

    boost::asio::awaitable<bool> add_request_async(
        const RequestData& request_data,
        const std::string& workflow_payload,
        const int max_requests,
        std::string& error_message) override;

    boost::asio::awaitable<bool> add_workflow_async(
        const WorkflowfullData& workflow_data,
        std::string& error_message) override;

    boost::asio::awaitable<bool> update_workflow_status_async(
        const std::string& client_id,
        const std::string& workflow_id,
        const std::string& status) override;

    boost::asio::awaitable<bool> get_all_active_workflows_async(
        std::vector<WorkflowfullData>& workflows) const override;

    boost::asio::awaitable<bool> get_all_workflows_for_client_async(
        const std::string& client_id,
        std::vector<WorkflowfullData>& workflows) const override;

private:
    explicit AsyncDatabase();
    static IDatabase& get_db_instance();

    IDatabase& db_;
    mutable boost::asio::thread_pool pool_;
};

} // namespace flow_pilot
