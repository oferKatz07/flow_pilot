// async_db_interface.h - Asynchronous database interface for FlowPilot

#pragma once

#include <boost/asio/awaitable.hpp>
#include <string>
#include <vector>

#include "client_config.h"
#include "db_interface.h"

namespace flow_pilot {

class IAsyncDatabase {
public:
    IAsyncDatabase() = default;
    virtual ~IAsyncDatabase() = default;

    virtual boost::asio::awaitable<bool> add_request_async(
        const RequestData& request_data,
        StatusCodes& error_status) = 0;

    virtual boost::asio::awaitable<bool> add_request_async(
        const RequestData& request_data,
        const std::string& workflow_payload,
        const ClientConfig& client_config,
        StatusCodes& error_status) = 0;

    virtual boost::asio::awaitable<bool> update_request_status_async(
        const RequestData& request_data) = 0;

    virtual boost::asio::awaitable<bool> get_all_requests_for_client_async(
        const std::string& client_id,
        std::vector<RequestData>& requests) const = 0;

    virtual boost::asio::awaitable<bool> add_workflow_async(
        const WorkflowfullData& workflow_data,
        StatusCodes& error_status) = 0;

    virtual boost::asio::awaitable<bool> update_workflow_status_async(
        const std::string& client_id,
        const std::string& workflow_id,
        const WorkflowStatus status) = 0;

    virtual boost::asio::awaitable<bool> get_all_active_workflows_async(
        std::vector<WorkflowfullData>& workflows) const = 0;

    virtual boost::asio::awaitable<bool> get_all_workflows_for_client_async(
        const std::string& client_id,
        std::vector<WorkflowfullData>& workflows) const = 0;
};

} // namespace flow_pilot

