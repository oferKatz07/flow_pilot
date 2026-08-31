// admission_failure_handler.h - Admission failure response and cleanup handling for FlowPilot

#pragma once

#include <boost/asio.hpp>

#include "admission_types.h"
#include "database_models.h"

namespace flow_pilot {

class AdmissionFailureHandler {
public:
    boost::asio::awaitable<ValidationResult> handle_rejection(ValidationResult& result,
                                                              RequestData& request_info,
                                                              AdmissionStage admission_stage,
                                                              const StatusCodes& rejection_reason) const;

    boost::asio::awaitable<void> update_redis_request_status(const RequestData& request_info) const;
};

} // namespace flow_pilot
