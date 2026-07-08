
// request_handlers.cpp - Implementation of request handlers for handling the FlowPilot received HTTP requests

#include "request_handlers.h"
#include "workflow_service.h"
#include "logger.h"
#include "config.h"

namespace flow_pilot {
using namespace boost::asio;

static inline std::vector<std::string_view> split_path(std::string_view target)
{
    if (target.empty() || target[0] != '/') {
        return {};
    }

    auto query_pos = target.find('?');
    if (query_pos != std::string_view::npos) {
        target.remove_suffix(target.size() - query_pos);
    }

    std::vector<std::string_view> segments;
    size_t start = 1;

    while (start <= target.size()) {
        auto next = target.find('/', start);
        if (next == std::string_view::npos) {
            next = target.size();
        }

        if (next > start) {
            segments.emplace_back(target.substr(start, next - start));
        }
        start = next + 1;
    }

    return segments;
}

static http::response<http::string_body> make_json_response(
    http::status status,
    bool keep_alive,
    std::string body) {
    http::response<http::string_body> res{status, 11};
    res.set(http::field::server, "FlowPilot");
    res.set(http::field::content_type, "application/json; charset=utf-8");
    res.keep_alive(keep_alive);
    res.body() = std::move(body);
    res.prepare_payload();
    return res;
}

static http::response<http::string_body> make_header_only_response(
    http::status status,
    bool keep_alive) {
    http::response<http::string_body> res{status, 11};
    res.set(http::field::server, "FlowPilot");
    res.keep_alive(keep_alive);
    res.prepare_payload();
    return res;
}

static std::string build_validation_error_response(const ValidationResult& result){
    json response;
    response["status"] = "validation_failed";
    response["message"] = result.status_str;
    response["errors"].push_back(result.errors_msg);
    return response.dump();
}

static std::string build_validation_success_response(const std::string_view& workflow_id) {
    json response;
    response["status"] = "accepted";
    response["message"] = "workflow validation successful";
    response["workflow_id"] = workflow_id;
    return response.dump();
}

WorkflowValidationHandler::WorkflowValidationHandler()
    : workflow_service_(Config::get_config().workflow().workflow_schema_path) {
}

// Handler for POST /api/v1/workflows - validate and submit workflow
awaitable<http::response<http::string_body>>
WorkflowValidationHandler::handle(const HandlerCtxData& ctx) {
    Logger::get_instance()->info("Creating new workflow");
    ValidationResult validation = co_await workflow_service_.submit_workflow(ctx.request_body);
    if (!validation.valid) {
        get_logger()->warn("Workflow validation failed: {}", validation.status_str);
        if (validation.status_str == "Client not found") {
            co_return make_json_response(http::status::forbidden, ctx.keep_alive,
                build_validation_error_response(validation));
        } else if (validation.status_str == "Duplicate request") {
            co_return make_json_response(http::status::conflict, ctx.keep_alive,
                build_validation_error_response(validation));
        } else if (validation.status_str == "Concurrent workflow limit exceeded" ||
                   validation.status_str == "Rate limit exceeded") {
            co_return make_json_response(http::status::too_many_requests, ctx.keep_alive,
                build_validation_error_response(validation));
        } else if (validation.status_str == "Internal DB failure") {
            co_return make_json_response(http::status::internal_server_error, ctx.keep_alive,
                build_validation_error_response(validation));
        }
        co_return make_json_response(http::status::bad_request, ctx.keep_alive,
            build_validation_error_response(validation));
    }

    co_return make_json_response(http::status::accepted, ctx.keep_alive,
        build_validation_success_response(validation.status_str));
}

// Handler for GET /api/v1/workflows
awaitable<http::response<http::string_body>>
GetWorkflowListHandler::handle(const HandlerCtxData& ctx) {
    Logger::get_instance()->info("List workflows requested - Returning feature not implemented response");
    // return make_json_response(http::status::not_implemented, keep_alive,
    //     "{\"workflows\":[],\"message\":\"workflow list handler not implemented\"}");

    co_return make_header_only_response(http::status::not_implemented, ctx.keep_alive);
}

// Handler for GET /api/v1/workflows/{workflow_id}
awaitable<http::response<http::string_body>>
GetWorkflowHandler::handle(const HandlerCtxData& ctx) {
    Logger::get_instance()->info("Getting workflow: {} - Returning feature not implemented response", ctx.workflow_id);
    // json response;
    // response["workflow_id"] = workflow_id_;
    // response["status"] = "unknown";
    // response["message"] = "workflow fetch handler not implemented";    
    // return make_json_response(http::status::not_implemented, keep_alive, response.dump());

    co_return make_header_only_response(http::status::not_implemented, ctx.keep_alive);
}

// Handler for DELETE /api/v1/workflows/{workflow_id}
awaitable<http::response<http::string_body>>
DeleteWorkflowHandler::handle(const HandlerCtxData& ctx) {
    Logger::get_instance()->info("Deleting workflow: {} - Returning feature not implemented response", ctx.workflow_id);
    // json response;
    // response["workflow_id"] = workflow_id;
    // response["status"] = "deleted";
    // response["message"] = "workflow delete handler not implemented";
    // return make_json_response(http::status::not_implemented, keep_alive, response.dump());

    co_return make_header_only_response(http::status::not_implemented, ctx.keep_alive);
}

// Handler for POST /api/v1/workflows/{workflow_id}/cancel
awaitable<http::response<http::string_body>>
CancelWorkflowHandler::handle(const HandlerCtxData& ctx) {
    Logger::get_instance()->info("Canceling workflow: {} - Returning feature not implemented response", ctx.workflow_id);
    // json response;
    // response["workflow_id"] = workflow_id;
    // response["status"] = "cancel_requested";
    // response["message"] = "workflow cancel handler not implemented";
    // return make_json_response(http::status::not_implemented, keep_alive, response.dump());

    co_return make_header_only_response(http::status::not_implemented, ctx.keep_alive);
}

awaitable<http::response<http::string_body>>
ErrorHandler::handle(const HandlerCtxData& ctx) {
    switch (ctx.error_status_) {
        case http::status::payload_too_large:
            co_return make_json_response(http::status::payload_too_large, ctx.keep_alive,
                "{\"error\":\"request body too large\"}");
        case http::status::method_not_allowed:
            co_return make_json_response(http::status::method_not_allowed, ctx.keep_alive,
                "{\"error\":\"method not allowed\"}");
        case http::status::not_found:
            co_return make_json_response(http::status::not_found, ctx.keep_alive,
                "{\"error\":\"not found\"}");
        default:
            co_return make_json_response(http::status::internal_server_error, ctx.keep_alive,
                "{\"error\":\"internal server error\"}");
    }
}

HandlerFactory& HandlerFactory::get_instance() {
    static HandlerFactory instance;
    return instance;
}

IHandler& HandlerFactory::get_handler(const http::request<http::string_body>& req,
                                      HandlerCtxData& ctx) {
    const auto keep_alive = req.keep_alive();
    const auto segments = split_path(req.target());
    auto body = req.body();
    auto method = req.method();

    if (body.size() > Config::get_config().server().max_request_body_size) {
        ctx.error_status_ = http::status::payload_too_large;
        return error_handler_;
    }

    // Workflows endpoints
    if (segments.size() >= 3 && segments[0] == "api" && segments[1] == "v1" && 
        segments[2] == "workflows") {
        
        // List all workflows
        if (segments.size() == 3 && method == http::verb::get) {
            ctx.keep_alive = keep_alive;
            return get_workflow_list_handler_;
        }
        
        // Submit new workflow
        if (segments.size() == 3 && method == http::verb::post) {
            ctx.request_body = std::move(body);
            ctx.keep_alive = keep_alive;
            return workflow_validation_handler_;
        }
        
        // Get specific workflow
        if (segments.size() == 4 && method == http::verb::get) {
            ctx.workflow_id = segments[3];
            ctx.keep_alive = keep_alive;
            return get_workflow_handler_;
        }
        
        // Delete specific workflow
        if (segments.size() == 4 && method == http::verb::delete_) {
            ctx.workflow_id = segments[3];
            ctx.keep_alive = keep_alive;
            return delete_workflow_handler_;
        }
        
        // Cancel workflow
        if (segments.size() == 5 && segments[4] == "cancel" && method == http::verb::post) {
            ctx.workflow_id = segments[3];
            ctx.keep_alive = keep_alive;
            return cancel_workflow_handler_;
        }
    }

    // Method not allowed
    if (method != http::verb::get && method != http::verb::post && method != http::verb::delete_) {
        ctx.error_status_ = http::status::method_not_allowed;
        return error_handler_;
    }

    // Not found
    ctx.error_status_ = http::status::not_found;
    return error_handler_;
}

} // namespace flow_pilot
