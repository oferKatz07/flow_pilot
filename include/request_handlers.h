
// request_handlers.h - Handler definitions for handling the FlowPilot received HTTP requests

#pragma once

#include <string>
#include <string_view>
#include <boost/beast/http.hpp>

#include "workflow_service.h"

namespace flow_pilot {
namespace beast = boost::beast;
namespace http = beast::http;

struct HandlerCtxData {
    std::string_view workflow_id;
    std::string request_body;
    http::status error_status_;
    bool keep_alive;
};


class IHandler {
public:
    virtual ~IHandler() = default;
    virtual boost::asio::awaitable<http::response<http::string_body>> handle(const HandlerCtxData& ctx) = 0;
};

class BaseHandler : public IHandler {
public:
    BaseHandler() = default;
    virtual ~BaseHandler() = default;
};

class WorkflowValidationHandler final : public BaseHandler {
public:
    WorkflowValidationHandler();
    boost::asio::awaitable<http::response<http::string_body>> handle(const HandlerCtxData& ctx) override;
private:
    WorkflowService workflow_service_;
};

class GetWorkflowListHandler final : public BaseHandler {
public:
    GetWorkflowListHandler() = default;
    boost::asio::awaitable<http::response<http::string_body>> handle(const HandlerCtxData& ctx) override;
};

class GetWorkflowHandler final : public BaseHandler {
public:
    GetWorkflowHandler() = default;
    boost::asio::awaitable<http::response<http::string_body>> handle(const HandlerCtxData& ctx) override;
};

class DeleteWorkflowHandler final : public BaseHandler {
public:
    DeleteWorkflowHandler() = default;
    boost::asio::awaitable<http::response<http::string_body>> handle(const HandlerCtxData& ctx) override;
};

class CancelWorkflowHandler final : public BaseHandler {
public:
    CancelWorkflowHandler() = default;
    boost::asio::awaitable<http::response<http::string_body>> handle(const HandlerCtxData& ctx) override;
};

class ErrorHandler final : public BaseHandler {
public:
    ErrorHandler() = default;
    boost::asio::awaitable<http::response<http::string_body>> handle(const HandlerCtxData& ctx) override;
};

class HandlerFactory {
public:
    static HandlerFactory& get_instance();

    IHandler& get_handler(const http::request<http::string_body>& req,
                          HandlerCtxData& ctx);

private:
    HandlerFactory() = default;
    WorkflowValidationHandler workflow_validation_handler_;
    GetWorkflowListHandler get_workflow_list_handler_;
    GetWorkflowHandler get_workflow_handler_;
    DeleteWorkflowHandler delete_workflow_handler_;
    CancelWorkflowHandler cancel_workflow_handler_;
    ErrorHandler error_handler_;
};

} // namespace flow_pilot
