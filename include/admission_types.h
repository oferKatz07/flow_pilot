// admission_types.h - Shared admission result and stage types for FlowPilot

#pragma once

#include <string>

#include "flow_pilot_error_msgs.h"

namespace flow_pilot {

struct ValidationResult {
    bool valid;
    StatusCodes status_code;
    std::string errors_msg;
};

enum class AdmissionStage {
    PARSE_REQUEST,
    GET_CLIENT_CONFIG,
    ADMIT_REQUEST,
    PERSIST_REQUEST,
    VALIDATE_WORKFLOW,
    PERSIST_WORKFLOW,
    GENERATE_RUNTIME_DATA
};

} // namespace flow_pilot
