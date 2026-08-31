// admission_failure_handler.cpp - Admission failure response and cleanup handling for FlowPilot

#include "admission_failure_handler.h"

#include "db_factory.h"
#include "flow_pilot_error_msgs.h"
#include "logger.h"
#include "redis_db_async.h"

namespace flow_pilot {
using namespace boost::asio;

awaitable<ValidationResult> AdmissionFailureHandler::handle_rejection(ValidationResult& result,
                                                                      RequestData& request_info,
                                                                      AdmissionStage admission_stage,
                                                                      const StatusCodes& rejection_reason) const
{
    const std::string_view errors_msg = status_code_to_string(rejection_reason);

    result.valid = false;
    result.status_code = rejection_reason;
    result.errors_msg = errors_msg;

    request_info.status = RequestStatus::REJECTED;
    request_info.reject_reason = errors_msg;

    if (admission_stage >= AdmissionStage::PERSIST_REQUEST) {
        co_await update_redis_request_status(request_info);
    }

    if (admission_stage > AdmissionStage::PERSIST_REQUEST) {
        co_await DBFactory::get().update_request_status_async(request_info);
        co_await DBFactory::get().fail_workflow_async(request_info.client_id, request_info.workflow_id);
    }

    co_return result;
}

awaitable<void> AdmissionFailureHandler::update_redis_request_status(const RequestData& request_info) const
{
    if (!co_await RedisDatabaseAsync::get_instance()->update_request_status_async(
            request_info.client_id,
            request_info.request_id,
            std::string(to_string(request_info.status)))) {
        Logger::get_logger()->warn("Failed to update Redis request status for {}/{}", request_info.client_id,
                                   request_info.request_id);
    }

    if (request_info.status != RequestStatus::ADMITTED) {
        if (!co_await RedisDatabaseAsync::get_instance()->release_request_id_async(
                request_info.client_id,
                request_info.request_id)) {
            Logger::get_logger()->warn(
                "Failed to release Redis request ID for {}/{}",
                request_info.client_id,
                request_info.request_id);
        }

        if (!co_await RedisDatabaseAsync::get_instance()->remove_active_workflow_async(
                request_info.client_id,
                request_info.workflow_id)) {
            Logger::get_logger()->warn(
                "Failed to remove active workflow from Redis for {}/{}",
                request_info.client_id,
                request_info.workflow_id);
        }
    }
}

} // namespace flow_pilot
