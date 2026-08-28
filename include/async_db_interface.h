// async_db_interface.h - Asynchronous database interface for FlowPilot

#pragma once

#include <boost/asio/awaitable.hpp>
#include <string>
#include <vector>

#include "database_models.h"
#include "flow_pilot_error_msgs.h"
#include "client_config.h"

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
        const WorkflowData& workflow_data,
        StatusCodes& error_status) = 0;

    virtual boost::asio::awaitable<bool> update_workflow_status_async(
        const std::string& client_id,
        const std::string& workflow_id,
        const WorkflowStatus status) = 0;

    virtual boost::asio::awaitable<bool> get_all_active_workflows_async(
        std::vector<WorkflowData>& workflows) const = 0;

    virtual boost::asio::awaitable<bool> get_all_workflows_for_client_async(
        const std::string& client_id,
        std::vector<WorkflowData>& workflows) const = 0;

    virtual boost::asio::awaitable<bool> fail_workflow_async(
        const std::string& client_id,
        const std::string& workflow_id) = 0;
        
    virtual boost::asio::awaitable<bool> add_workflow_jobs_async(
        const WorkflowJobList& job_list,
        StatusCodes& error_status) = 0;

    virtual boost::asio::awaitable<bool> update_ready_jobs_async(
        const std::string& client_id,
        const std::string& workflow_id,
        const std::vector<std::string>& queud_job_ids,
        const std::vector<std::string>& ready_job_ids) = 0;
};

} // namespace flow_pilot

