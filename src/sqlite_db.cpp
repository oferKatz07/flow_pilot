
// sqlite_db.cpp - SQLite persistence implementation for FlowPilot

#include <sqlite3.h>
#include <chrono>
#include <filesystem>
#include <iomanip>
#include <sstream>
#include <ctime>
#include <stdexcept>

#include "flow_pilot_error_msgs.h"
#include "config.h"
#include "http_server.h"
#include "logger.h"
#include "sqlite_db.h"



namespace flow_pilot {

namespace {

std::time_t nullable_time_from_sqlite(sqlite3_stmt* stmt, int column)
{
    if (sqlite3_column_type(stmt, column) == SQLITE_NULL) {
        return 0;
    }

    return static_cast<std::time_t>(sqlite3_column_int64(stmt, column));
}

} // namespace

SQLiteDatabase::~SQLiteDatabase() {
    if (db_) {
        sqlite3_close(db_);
        db_ = nullptr;
    }
}

IDatabase& SQLiteDatabase::get_instance() {
    static SQLiteDatabase instance(Config::get().db_config().db_path);
    return dynamic_cast<IDatabase&>(instance);
}

bool SQLiteDatabase::get_rate_limit_plans(std::unordered_map<std::string, RateLimitConfig> &rate_limit_plans) const {
    const char* sql = "SELECT plan_name, max_concurrent_workflows, max_requests, window_sec FROM rate_limit_plans;";
    sqlite3_stmt* stmt = nullptr;
    int rc = sqlite3_prepare_v2(db_, sql, -1, &stmt, nullptr);
    if (rc != SQLITE_OK) {
        Logger::get_logger()->error("sqlite prepare failed: {}", sqlite3_errmsg(db_));
        if (stmt) sqlite3_finalize(stmt);
        return false;
    }

    while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
        std::string plan_name = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 0));
        RateLimitConfig cfg;
        cfg.plan_name = plan_name;
        cfg.max_concurrent_workflows = sqlite3_column_int(stmt, 1);
        cfg.max_requests = sqlite3_column_int(stmt, 2);
        cfg.window_sec = sqlite3_column_int(stmt, 3);
        rate_limit_plans.emplace(plan_name, cfg);
    }

    sqlite3_finalize(stmt);
    return true;
}

bool SQLiteDatabase::get_rate_limit_plan(RateLimitConfig& rate_limit_plan) const {
    bool ret_val = true;                                            
    const char* sql = "SELECT max_concurrent_workflows, max_requests, window_sec FROM rate_limit_plans WHERE plan_name = ? LIMIT 1;";
    sqlite3_stmt* stmt = nullptr;

    int rc = sqlite3_prepare_v2(db_, sql, -1, &stmt, nullptr);
    if (rc != SQLITE_OK) {
        Logger::get_logger()->error("sqlite prepare failed: {}", sqlite3_errmsg(db_));
        if (stmt) {
            sqlite3_finalize(stmt);
        }
        return false;
    }
    
    sqlite3_bind_text(stmt, 1, rate_limit_plan.plan_name.c_str(), -1, SQLITE_STATIC);
    rc = sqlite3_step(stmt);
    if (rc == SQLITE_ROW) {
        rate_limit_plan.max_concurrent_workflows = sqlite3_column_int(stmt, 0);
        rate_limit_plan.max_requests = sqlite3_column_int(stmt, 1);
        rate_limit_plan.window_sec = sqlite3_column_int(stmt, 2);
    } else {
        Logger::get_logger()->error("Failed to fined rate limit plan '{}' in DB", rate_limit_plan.plan_name);
        ret_val = false;
    }   

    sqlite3_finalize(stmt);

    return ret_val;
}

bool SQLiteDatabase::upsert_rate_limit_plan(const RateLimitConfig& rate_limit_plan) {
    bool ret_val = true;                                            
    const char* sql = "INSERT OR REPLACE INTO rate_limit_plans (plan_name, max_concurrent_workflows, max_requests, window_sec, update_time) VALUES (?, ?, ?, ?, ?);";
    sqlite3_stmt* stmt = nullptr;
    
    int rc = sqlite3_prepare_v2(db_, sql, -1, &stmt, nullptr);
    if (rc != SQLITE_OK) {
        Logger::get_logger()->error("sqlite prepare failed: {}", sqlite3_errmsg(db_));
        if (stmt) {
            sqlite3_finalize(stmt);
        }
        return false;
    }

    sqlite3_bind_text(stmt,  1, rate_limit_plan.plan_name.c_str(), -1, SQLITE_STATIC);
    sqlite3_bind_int(stmt,   2, rate_limit_plan.max_concurrent_workflows);
    sqlite3_bind_int(stmt,   3, rate_limit_plan.max_requests);
    sqlite3_bind_int(stmt,   4, rate_limit_plan.window_sec);
    sqlite3_bind_int64(stmt, 5, static_cast<sqlite3_int64>(std::time(nullptr)));

    rc = sqlite3_step(stmt);
    if (rc != SQLITE_DONE) {
        Logger::get_logger()->error("sqlite step failed: {}", sqlite3_errmsg(db_));
        ret_val = false;
    }

    sqlite3_finalize(stmt);
    return ret_val;
}

bool SQLiteDatabase::get_policy_plans(std::unordered_map<std::string, PolicyPlan> &policy_plans) const {
    const char* sql = "SELECT plan_name, max_workflow_size_kb, max_jobs_in_workflow, "
                      "max_job_size_bytes, max_workflow_runtime_sec, max_workflow_total_retries, max_job_runtime_sec, "
                      "max_job_retries, max_concurrent_jobs, max_pending_jobs, workflow_retention_days, request_retention_days, "
                      "payload_retention_days FROM policy_plans;";
    sqlite3_stmt* stmt = nullptr;
    int rc = sqlite3_prepare_v2(db_, sql, -1, &stmt, nullptr);
    if (rc != SQLITE_OK) {
        Logger::get_logger()->error("sqlite prepare failed: {}", sqlite3_errmsg(db_));
        if (stmt) sqlite3_finalize(stmt);
        return false;
    }

    while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
        std::string name = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 0));
        PolicyPlan p;
        p.plan_name = name;
        p.max_workflow_size_kb = sqlite3_column_int(stmt, 1);
        p.max_jobs_in_workflow = sqlite3_column_int(stmt, 2);
        p.max_job_size_bytes = sqlite3_column_int(stmt, 3);
        p.max_workflow_runtime_sec = sqlite3_column_int(stmt, 4);
        p.max_workflow_total_retries = sqlite3_column_int(stmt, 5);
        p.max_job_runtime_sec = sqlite3_column_int(stmt, 6);
        p.max_job_retries = sqlite3_column_int(stmt, 7);
        p.max_concurrent_jobs = sqlite3_column_int(stmt, 8);
        p.max_pending_jobs = sqlite3_column_int(stmt, 9);
        p.workflow_retention_days = sqlite3_column_int(stmt, 10);
        p.request_retention_days = sqlite3_column_int(stmt, 11);
        p.payload_retention_days = sqlite3_column_int(stmt, 12);
        policy_plans.emplace(name, p);
    }
    sqlite3_finalize(stmt);
    return true;
}

bool SQLiteDatabase::get_policy_plan(PolicyPlan& plan_policy) const {
    bool ret_val = true;
    const char* sql = "SELECT plan_name, max_workflow_size_kb, max_jobs_in_workflow, "
                      "max_job_size_bytes, max_workflow_runtime_sec, max_workflow_total_retries, max_job_runtime_sec, "
                      "max_job_retries, max_concurrent_jobs, max_pending_jobs, workflow_retention_days, request_retention_days, "
                      "payload_retention_days FROM policy_plans WHERE plan_name = ? LIMIT 1;";
    sqlite3_stmt* stmt = nullptr;

    int rc = sqlite3_prepare_v2(db_, sql, -1, &stmt, nullptr);
    if (rc != SQLITE_OK) {
        Logger::get_logger()->error("sqlite prepare failed: {}", sqlite3_errmsg(db_));
        if (stmt) sqlite3_finalize(stmt);
        return false;
    }
    sqlite3_bind_text(stmt, 1, plan_policy.plan_name.c_str(), -1, SQLITE_STATIC);
    rc = sqlite3_step(stmt);
    if (rc == SQLITE_ROW) {
        plan_policy.max_workflow_size_kb = sqlite3_column_int(stmt, 1);
        plan_policy.max_jobs_in_workflow = sqlite3_column_int(stmt, 2);
        plan_policy.max_job_size_bytes = sqlite3_column_int(stmt, 3);
        plan_policy.max_workflow_runtime_sec = sqlite3_column_int(stmt, 4);
        plan_policy.max_workflow_total_retries = sqlite3_column_int(stmt, 5);
        plan_policy.max_job_runtime_sec = sqlite3_column_int(stmt, 6);
        plan_policy.max_job_retries = sqlite3_column_int(stmt, 7);
        plan_policy.max_concurrent_jobs = sqlite3_column_int(stmt, 8);
        plan_policy.max_pending_jobs = sqlite3_column_int(stmt, 9);
        plan_policy.workflow_retention_days = sqlite3_column_int(stmt, 10);
        plan_policy.request_retention_days = sqlite3_column_int(stmt, 11);
        plan_policy.payload_retention_days = sqlite3_column_int(stmt, 12);
    } else {
        Logger::get_logger()->error("Failed to find policy plan '{}' in DB", plan_policy.plan_name);
        ret_val = false;
    }

    sqlite3_finalize(stmt);
    return ret_val;
}

bool SQLiteDatabase::upsert_policy_plan(const PolicyPlan& policy_plan) {
    bool ret_val = true;
    const char* sql = "INSERT OR REPLACE INTO policy_plans (plan_name, max_workflow_size_kb, max_jobs_in_workflow, "
                      "max_job_size_bytes, max_workflow_runtime_sec, max_workflow_total_retries, max_job_runtime_sec, "
                      "max_job_retries, max_concurrent_jobs, max_pending_jobs, workflow_retention_days, request_retention_days, "
                      "payload_retention_days, update_time) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);";
    sqlite3_stmt* stmt = nullptr;

    int rc = sqlite3_prepare_v2(db_, sql, -1, &stmt, nullptr);
    if (rc != SQLITE_OK) {
        Logger::get_logger()->error("sqlite prepare failed: {}", sqlite3_errmsg(db_));
        if (stmt) {
            sqlite3_finalize(stmt);
        }

        return false;
    }

    sqlite3_bind_text(stmt,  1, policy_plan.plan_name.c_str(), -1, SQLITE_STATIC);
    sqlite3_bind_int(stmt,   2, policy_plan.max_workflow_size_kb);
    sqlite3_bind_int(stmt,   3, policy_plan.max_jobs_in_workflow);
    sqlite3_bind_int(stmt,   4, policy_plan.max_job_size_bytes);
    sqlite3_bind_int(stmt,   5, policy_plan.max_workflow_runtime_sec);
    sqlite3_bind_int(stmt,   6, policy_plan.max_workflow_total_retries);
    sqlite3_bind_int(stmt,   7, policy_plan.max_job_runtime_sec);
    sqlite3_bind_int(stmt,   8, policy_plan.max_job_retries);
    sqlite3_bind_int(stmt,   9, policy_plan.max_concurrent_jobs);
    sqlite3_bind_int(stmt,   10, policy_plan.max_pending_jobs); 
    sqlite3_bind_int(stmt,   11, policy_plan.workflow_retention_days); 
    sqlite3_bind_int(stmt,   12, policy_plan.request_retention_days); 
    sqlite3_bind_int(stmt,   13, policy_plan.payload_retention_days);
    sqlite3_bind_int64(stmt, 14, static_cast<sqlite3_int64>(std::time(nullptr)));

    rc = sqlite3_step(stmt);
    if (rc != SQLITE_DONE) {
        Logger::get_logger()->error("sqlite step failed: {}", sqlite3_errmsg(db_));
        ret_val = false;
    }
    sqlite3_finalize(stmt);
    return ret_val;
}

bool SQLiteDatabase::get_all_users(
    std::unordered_map<std::string, ClientData> &clients
) const {
    clients.clear();
    const char* sql = "SELECT client_id, rate_limit_plan_name, policy_plan_name FROM clients;";
    sqlite3_stmt* stmt = nullptr;

    int rc = sqlite3_prepare_v2(db_, sql, -1, &stmt, nullptr);
    if (rc != SQLITE_OK) {
        Logger::get_logger()->error("sqlite prepare failed: {}", sqlite3_errmsg(db_));
        if (stmt) sqlite3_finalize(stmt);
        return false;
    }

    while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
        const unsigned char* cid = sqlite3_column_text(stmt, 0);
        const unsigned char* rl = sqlite3_column_text(stmt, 1);
        const unsigned char* pp = sqlite3_column_text(stmt, 2);
        ClientData cd;
        cd.client_id = cid ? reinterpret_cast<const char*>(cid) : std::string();
        cd.rate_limit_config_plan_name = rl ? reinterpret_cast<const char*>(rl) : std::string();
        cd.policy_plan_name = pp ? reinterpret_cast<const char*>(pp) : std::string();
        clients.emplace(cd.client_id, cd);
    }

    sqlite3_finalize(stmt);
    return true;
}

bool SQLiteDatabase::get_user_config(ClientData& user_data) const {
    bool ret_val = true;
    const char* sql = "SELECT rate_limit_plan_name, policy_plan_name FROM clients WHERE client_id = ? LIMIT 1;";
    sqlite3_stmt* stmt = nullptr;

    int rc = sqlite3_prepare_v2(db_, sql, -1, &stmt, nullptr);
    if (rc != SQLITE_OK) {
        Logger::get_logger()->error("sqlite prepare failed: {}", sqlite3_errmsg(db_));
        if (stmt) sqlite3_finalize(stmt);
        return false;
    }

    sqlite3_bind_text(stmt, 1, user_data.client_id.c_str(), -1, SQLITE_STATIC);
    rc = sqlite3_step(stmt);
    if (rc == SQLITE_ROW) {
        const unsigned char* rl = sqlite3_column_text(stmt, 0);
        const unsigned char* pp = sqlite3_column_text(stmt, 1);
        user_data.rate_limit_config_plan_name = rl ? reinterpret_cast<const char*>(rl) : std::string();
        user_data.policy_plan_name = pp ? reinterpret_cast<const char*>(pp) : std::string();
    } else {
        Logger::get_logger()->error("Failed to find client_id '{}' in DB", user_data.client_id);
        ret_val = false;
    }
    
    sqlite3_finalize(stmt);
    return ret_val;
}

bool SQLiteDatabase::upsert_user_config(
    const std::string& user_id,
    const std::string& rate_limit_plan_name,
    const std::string& policy_plan_name
) {
    bool ret_val = true;
    const char* sql = "INSERT OR REPLACE INTO clients (client_id, rate_limit_plan_name, policy_plan_name, status, update_time) VALUES (?, ?, ?, ?, ?);";
    sqlite3_stmt* stmt = nullptr;

    int rc = sqlite3_prepare_v2(db_, sql, -1, &stmt, nullptr);
    if (rc != SQLITE_OK) {
        Logger::get_logger()->error("sqlite prepare failed: {}", sqlite3_errmsg(db_));
        if (stmt) sqlite3_finalize(stmt);
        return false;
    }

    sqlite3_bind_text(stmt,  1, user_id.c_str(), -1, SQLITE_STATIC);
    sqlite3_bind_text(stmt,  2, rate_limit_plan_name.c_str(), -1, SQLITE_STATIC);
    sqlite3_bind_text(stmt,  3, policy_plan_name.c_str(), -1, SQLITE_STATIC);
    sqlite3_bind_text(stmt,  4, "ACTIVE", -1, SQLITE_STATIC);
    sqlite3_bind_int64(stmt, 5, static_cast<sqlite3_int64>(std::time(nullptr)));

    rc = sqlite3_step(stmt);
    if (rc != SQLITE_DONE) {
        Logger::get_logger()->error("sqlite step failed: {}", sqlite3_errmsg(db_));
        ret_val = false;
    }

    sqlite3_finalize(stmt);
    return ret_val;
}

bool SQLiteDatabase::add_request(const RequestData& request_data, StatusCodes& error_status) {                                 
    const sqlite3_int64 now = static_cast<sqlite3_int64>(std::time(nullptr));
    const char* sql = "INSERT INTO workflow_requests (client_id, request_id, workflow_id, payload_size, operation_type, \
                                                      status, reject_reason, received_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?);";
    sqlite3_stmt* stmt = nullptr;
    bool ret_val = true;
    std::string msg;

    int rc = sqlite3_prepare_v2(db_, sql, -1, &stmt, nullptr);
    if (rc != SQLITE_OK) {
        msg = std::string("add_request - Failed to prepare SQLite statement: ") + sqlite3_errmsg(db_);
        Logger::get_logger()->error(msg);
        if (stmt) sqlite3_finalize(stmt);
        error_status = StatusCodes::INTERNAL_DB_FAILURE;
        return false;
    }

    sqlite3_bind_text(stmt, 1, request_data.client_id.c_str(), -1, SQLITE_STATIC);
    sqlite3_bind_text(stmt, 2, request_data.request_id.c_str(), -1, SQLITE_STATIC);
    sqlite3_bind_text(stmt, 3, request_data.workflow_id.c_str(), -1, SQLITE_STATIC);
    sqlite3_bind_int(stmt, 4, request_data.workflow_payload_size_bytes);
    sqlite3_bind_text(stmt, 5, request_data.operation.c_str(), -1, SQLITE_STATIC);
    sqlite3_bind_int(stmt, 6, to_int(request_data.status));
    sqlite3_bind_text(stmt, 7, request_data.reject_reason.c_str(), -1, SQLITE_STATIC);
    sqlite3_bind_int64(stmt, 8, now);

    rc = sqlite3_step(stmt);
    if (rc != SQLITE_DONE) {
        if (rc == SQLITE_CONSTRAINT) {
            msg = std::string("Duplicate request: request id=") + request_data.request_id + " client_id=" + request_data.client_id;
            error_status = StatusCodes::DUPLICATE_REQUEST;
        } else {
            msg = std::string("Failed to insert request: ") + sqlite3_errmsg(db_);
            error_status = StatusCodes::INTERNAL_DB_FAILURE;
        }
        Logger::get_logger()->error(msg);
        ret_val = false;
    }

    sqlite3_finalize(stmt);
    return ret_val;
}

bool SQLiteDatabase::add_request(const RequestData& request_data,
                                  const std::string& workflow_payload,
                                  const ClientConfig& client_config,
                                  StatusCodes& error_status) {
    // Add the request to the database
    if (!add_request(request_data, error_status)) {
        return false;
    }

    int concurrent_workflows;
    if (!get_client_active_workflows_count(request_data.client_id, concurrent_workflows)) {
        error_status = StatusCodes::INTERNAL_DB_FAILURE;
        return false;
    }

    if (concurrent_workflows >= client_config.rate_limit_config.max_concurrent_workflows) {
        error_status = StatusCodes::RATE_LIMIT_EXCEEDED;
        return false;
    }

    if (request_data.workflow_payload_size_bytes > client_config.policy_config.max_workflow_size_kb * 1024) {
        error_status = StatusCodes::WORKFLOW_SIZE_EXCEEDED;
        return false;
    }

    if (!add_request_payload(request_data, workflow_payload)) {
        Logger::Logger::get_logger()->error("Failed to add workflow payload for request_id={} client_id={}", request_data.request_id, request_data.client_id);
    }

    return true;
}

bool SQLiteDatabase::update_request_status(const RequestData& request_data) {
    const char* sql = "UPDATE workflow_requests SET status = ?, reject_reason = ? WHERE client_id = ? AND request_id = ?;";
    bool ret_val = true;
    sqlite3_stmt* stmt = nullptr;

    int rc = sqlite3_prepare_v2(db_, sql, -1, &stmt, nullptr);
    if (rc != SQLITE_OK) {
        Logger::get_logger()->error("update_request_status - sqlite prepare failed: {}", sqlite3_errmsg(db_));
        if (stmt) {
            sqlite3_finalize(stmt);
        }
        return false;
    }
    
    sqlite3_bind_int(stmt, 1, to_int(request_data.status));
    sqlite3_bind_text(stmt, 2, request_data.reject_reason.c_str(), -1, SQLITE_STATIC);
    sqlite3_bind_text(stmt, 3, request_data.client_id.c_str(), -1, SQLITE_STATIC);
    sqlite3_bind_text(stmt, 4, request_data.request_id.c_str(), -1, SQLITE_STATIC);
    rc = sqlite3_step(stmt);
    if (rc != SQLITE_DONE) {
        Logger::get_logger()->error("update_request_status - sqlite step failed: {}", sqlite3_errmsg(db_));
        ret_val = false;
    }

    sqlite3_finalize(stmt);

    return ret_val;
}

bool SQLiteDatabase::get_all_requests_for_client(const std::string& client_id, std::vector<RequestData>& workflows) const {
    const char* sql = "SELECT client_id, request_id, workflow_id, payload_size, operation_type, \
                       status, reject_reason FROM workflow_requests WHERE client_id = ?;";
    sqlite3_stmt* stmt = nullptr;
    int rc = sqlite3_prepare_v2(db_, sql, -1, &stmt, nullptr);
    if (rc != SQLITE_OK) {
        Logger::get_logger()->error("get_all_requests_for_client - sqlite prepare failed: {}", sqlite3_errmsg(db_));
        if (stmt) {
            sqlite3_finalize(stmt);
        }
        return false;
    }
    sqlite3_bind_text(stmt, 1, client_id.c_str(), -1, SQLITE_STATIC);
    while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
        RequestData rd;
        const unsigned char* cid = sqlite3_column_text(stmt, 0);
        const unsigned char* rid = sqlite3_column_text(stmt, 1);
        const unsigned char* wid = sqlite3_column_text(stmt, 2);
        const unsigned char* wfs = sqlite3_column_text(stmt, 3);
        const unsigned char* op = sqlite3_column_text(stmt, 4);
        const unsigned char* rr = sqlite3_column_text(stmt, 6);
        rd.client_id = cid ? reinterpret_cast<const char*>(cid) : std::string();
        rd.request_id = rid ? reinterpret_cast<const char*>(rid) : std::string();
        rd.workflow_id = wid ? reinterpret_cast<const char*>(wid) : std::string();
        rd.workflow_payload_size_bytes = wfs ? std::stoi(reinterpret_cast<const char*>(wfs)) : 0;
        rd.operation = op ? reinterpret_cast<const char*>(op) : std::string();
        rd.status = from_int_to_RequestStatus(sqlite3_column_int(stmt, 5));
        rd.reject_reason = rr ? reinterpret_cast<const char*>(rr) : std::string();
        workflows.push_back(rd);
    }

    sqlite3_finalize(stmt);
    return true;
}

bool SQLiteDatabase:: add_workflow(const WorkflowData& workflow_data,
                                  StatusCodes& error_status) {
    const char* sql = "INSERT INTO workflows (client_id, workflow_id, workflow_type, version, status, total_jobs, \
                                              received_at) VALUES (?, ?, ?, ?, ?, ?, ?);";
    const sqlite3_int64 now = static_cast<sqlite3_int64>(std::time(nullptr));
    sqlite3_stmt* stmt = nullptr;
    bool ret_val = true;

    int rc = sqlite3_prepare_v2(db_, sql, -1, &stmt, nullptr);
    if (rc != SQLITE_OK) {
        std::string error_message = std::string("add_workflow - Failed to prepare SQLite statement: ") + sqlite3_errmsg(db_);
        Logger::get_logger()->error(error_message);
        error_status = StatusCodes::INTERNAL_DB_FAILURE;
        if (stmt) sqlite3_finalize(stmt);
        return false;
    }

    // Bind all parameters for the workflow metadata row.
    // This creates the workflow record linked to the request above.
    sqlite3_bind_text(stmt,  1, workflow_data.info.client_id.c_str(), -1, SQLITE_STATIC);
    sqlite3_bind_text(stmt,  2, workflow_data.info.workflow_id.c_str(), -1, SQLITE_STATIC);
    sqlite3_bind_text(stmt,  3, workflow_data.workflow_type.c_str(), -1, SQLITE_STATIC);
    sqlite3_bind_text(stmt,  4, workflow_data.workflow_version.c_str(), -1, SQLITE_STATIC);
    sqlite3_bind_int(stmt,   5, to_int(workflow_data.status));
    sqlite3_bind_int(stmt,   6, workflow_data.total_jobs);
    sqlite3_bind_int64(stmt, 7, now);

    rc = sqlite3_step(stmt);
    if (rc != SQLITE_DONE) {
        std::string msg;
        if (rc == SQLITE_CONSTRAINT) {
            msg = std::string("add_workflow - Workflow ID already exists: workflow id=") + workflow_data.info.workflow_id + " client_id=" + workflow_data.info.client_id;
            error_status = StatusCodes::WORKFLOW_ID_EXISTS;
        } else {
            msg = std::string("add_workflow - Failed to insert workflow (workflow id=") + workflow_data.info.workflow_id + " client_id=" + workflow_data.info.client_id + "): " + sqlite3_errmsg(db_);
            error_status = StatusCodes::INTERNAL_DB_FAILURE;
        }
        Logger::get_logger()->error(msg);
        ret_val = false;
    }

    sqlite3_finalize(stmt);

    return ret_val;
}

bool SQLiteDatabase::update_workflow_status(const std::string& client_id, const std::string& workflow_id, const WorkflowStatus status) {
    std::string sql_cmd;
    switch (status) {
        case WorkflowStatus::COMPLETED:
        case WorkflowStatus::FAILED:
        case WorkflowStatus::CANCELED:
            sql_cmd = "UPDATE workflows SET status = ?, completed_at = ? WHERE client_id = ? AND workflow_id = ?;";
            break;
        case WorkflowStatus::RUNNING:
            sql_cmd = "UPDATE workflows SET status = ?, started_at = ? WHERE client_id = ? AND workflow_id = ?;";
            break;
        case WorkflowStatus::READY:
            sql_cmd = "UPDATE workflows SET status = ?, ready_at = ? WHERE client_id = ? AND workflow_id = ?;";
            break;
        default:
            Logger::get_logger()->error("update_workflow_status - Invalid status for update: {}", to_int(status));
            return false; // Invalid status for update
    }

    const char* sql = sql_cmd.c_str();
    sqlite3_stmt* stmt = nullptr;
    int rc = sqlite3_prepare_v2(db_, sql, -1, &stmt, nullptr);
    if (rc != SQLITE_OK) {
        Logger::get_logger()->error("sqlite prepare failed: {}", sqlite3_errmsg(db_));
        if (stmt) sqlite3_finalize(stmt);
        return false;
    }
    sqlite3_bind_int(stmt,   1, to_int(status));
    sqlite3_bind_int64(stmt, 2, static_cast<sqlite3_int64>(std::time(nullptr)));
    sqlite3_bind_text(stmt,  3, client_id.c_str(), -1, SQLITE_STATIC);
    sqlite3_bind_text(stmt,  4, workflow_id.c_str(), -1, SQLITE_STATIC);
    rc = sqlite3_step(stmt);
    if (rc != SQLITE_DONE) {
        Logger::get_logger()->error("sqlite step failed: {}", sqlite3_errmsg(db_));
        sqlite3_finalize(stmt);
        return false;
    }

    if (sqlite3_changes(db_) == 0) {
        Logger::get_logger()->error(
            "update_workflow_status - no workflow row found for client_id={} workflow_id={}",
            client_id,
            workflow_id);
        sqlite3_finalize(stmt);
        return false;
    }

    sqlite3_finalize(stmt);
    return true;
}

bool SQLiteDatabase::get_all_active_workflows(std::vector<WorkflowData>& workflows) const {
    const char* sql = "SELECT client_id, workflow_id, workflow_type, version, status, total_jobs, received_at, started_at, ready_at, completed_at "
                      "FROM workflows WHERE status IN (?, ?, ?);";
    sqlite3_stmt* stmt = nullptr;
    int rc = sqlite3_prepare_v2(db_, sql, -1, &stmt, nullptr);
    if (rc != SQLITE_OK) {
        Logger::get_logger()->error("sqlite prepare failed: {}", sqlite3_errmsg(db_));
        if (stmt) sqlite3_finalize(stmt);
        return false;
    }

    sqlite3_bind_int(stmt, 1, to_int(WorkflowStatus::ADMITTED));
    sqlite3_bind_int(stmt, 2, to_int(WorkflowStatus::READY));
    sqlite3_bind_int(stmt, 3, to_int(WorkflowStatus::RUNNING));

    while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
        WorkflowData wd;
        const unsigned char* cid = sqlite3_column_text(stmt, 0);
        const unsigned char* wid = sqlite3_column_text(stmt, 1);
        const unsigned char* wt = sqlite3_column_text(stmt, 2);
        const unsigned char* wv = sqlite3_column_text(stmt, 3);
        wd.info.client_id = cid ? reinterpret_cast<const char*>(cid) : std::string();
        wd.info.workflow_id = wid ? reinterpret_cast<const char*>(wid) : std::string();
        wd.workflow_type = wt ? reinterpret_cast<const char*>(wt) : std::string();
        wd.workflow_version = wv ? reinterpret_cast<const char*>(wv) : std::string();
        wd.status = from_int_to_WorkflowStatus(sqlite3_column_int(stmt, 4));
        wd.total_jobs = sqlite3_column_int(stmt, 5);
        wd.received_at = nullable_time_from_sqlite(stmt, 6);
        wd.started_at = nullable_time_from_sqlite(stmt, 7);
        wd.ready_at = nullable_time_from_sqlite(stmt, 8);
        wd.completed_at = nullable_time_from_sqlite(stmt, 9);
        workflows.push_back(std::move(wd));
    }
    
    sqlite3_finalize(stmt);
    return true;
}

bool SQLiteDatabase::get_all_workflows_for_client(const std::string& client_id, std::vector<WorkflowData>& workflows) const {
    const char* sql = "SELECT client_id, workflow_id, workflow_type, version, status, total_jobs, received_at, started_at, ready_at, completed_at "
                      "FROM workflows WHERE client_id = ?;";
    sqlite3_stmt* stmt = nullptr;
    int rc = sqlite3_prepare_v2(db_, sql, -1, &stmt, nullptr);
    if (rc != SQLITE_OK) {
        Logger::get_logger()->error("get_all_workflows_for_client - sqlite prepare failed: {}", sqlite3_errmsg(db_));
        if (stmt) sqlite3_finalize(stmt);
        return false;
    }

    sqlite3_bind_text(stmt, 1, client_id.c_str(), -1, SQLITE_STATIC);
    while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
        WorkflowData wf;
        const unsigned char* cid = sqlite3_column_text(stmt, 0);
        const unsigned char* wid = sqlite3_column_text(stmt, 1);
        const unsigned char* wt = sqlite3_column_text(stmt, 2);
        const unsigned char* wv = sqlite3_column_text(stmt, 3);
        wf.info.client_id = cid ? reinterpret_cast<const char*>(cid) : std::string();
        wf.info.workflow_id = wid ? reinterpret_cast<const char*>(wid) : std::string();
        wf.workflow_type = wt ? reinterpret_cast<const char*>(wt) : std::string();
        wf.workflow_version = wv ? reinterpret_cast<const char*>(wv) : std::string();
        wf.status = from_int_to_WorkflowStatus(sqlite3_column_int(stmt, 4));
        wf.total_jobs = sqlite3_column_int(stmt, 5);
        wf.received_at = nullable_time_from_sqlite(stmt, 6);
        wf.started_at = nullable_time_from_sqlite(stmt, 7);
        wf.ready_at = nullable_time_from_sqlite(stmt, 8);
        wf.completed_at = nullable_time_from_sqlite(stmt, 9);
        workflows.push_back(wf);
    }

    sqlite3_finalize(stmt);
    return true;
}

bool SQLiteDatabase::fail_workflow(const std::string& client_id, const std::string& workflow_id) {
    bool ret_val1;
    bool ret_val2;
    ret_val1 = fail_all_jobs_waiting_to_run(client_id, workflow_id);
    ret_val2 = update_workflow_status(client_id, workflow_id, WorkflowStatus::FAILED);

    return ret_val1 && ret_val2;
}

bool SQLiteDatabase::get_all_jobs_for_workflow(const std::string& client_id, const std::string& workflow_id, 
                                               std::vector<WorkflowJob>& jobs) const {

    const char* sql = "SELECT job_uuid, client_id, workflow_id, job_id, status, retry_count, submitted_at, ready_at, queued_at, started_at, completed_at "
                      "FROM jobs WHERE client_id = ? AND workflow_id = ?;";
    sqlite3_stmt* stmt;
    bool ret_val = true;

    if (sqlite3_prepare_v2(db_, sql, -1, &stmt, nullptr) == SQLITE_OK) {
        sqlite3_bind_text(stmt, 1, client_id.c_str(), -1, SQLITE_STATIC);
        sqlite3_bind_text(stmt, 2, workflow_id.c_str(), -1, SQLITE_STATIC);

        while (sqlite3_step(stmt) == SQLITE_ROW) {
            WorkflowJob job_data;
            job_data.job_uuid = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 0));
            job_data.client_id = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 1));
            job_data.workflow_id = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 2));
            job_data.job_id = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 3));
            job_data.status = from_int_to_JobStatus(sqlite3_column_int(stmt, 4));
            job_data.retry_num = sqlite3_column_int(stmt, 5);
            job_data.submitted_at = nullable_time_from_sqlite(stmt, 6);
            job_data.ready_at = nullable_time_from_sqlite(stmt, 7);
            job_data.queued_at = nullable_time_from_sqlite(stmt, 8);
            job_data.started_at = nullable_time_from_sqlite(stmt, 9);
            job_data.completed_at = nullable_time_from_sqlite(stmt, 10);
            jobs.push_back(job_data);
        }

        sqlite3_finalize(stmt);
    } else {
        Logger::Logger::get_logger()->error(sqlite3_errmsg(db_));
        ret_val = false;
    }

    return ret_val;
}

bool SQLiteDatabase::add_workflow_jobs(const WorkflowJobList& job_list, StatusCodes& error_status) {
    const char* sql = "INSERT INTO jobs (job_uuid, client_id, workflow_id, job_id, status, retry_count, \
                                         submitted_at) VALUES (?, ?, ?, ?, ?, ?, ?);";
    sqlite3_stmt* stmt = nullptr;
    bool ret_val = true;

    int rc = sqlite3_prepare_v2(db_, sql, -1, &stmt, nullptr);
    if (rc != SQLITE_OK) {
        std::string error_message = std::string("Failed to prepare SQLite statement: ") + sqlite3_errmsg(db_);
        Logger::get_logger()->error(error_message);
        error_status = StatusCodes::INTERNAL_DB_FAILURE;
        if (stmt) sqlite3_finalize(stmt);
        return false;
    }

    // 1. Begin Transaction
    char* errMsg = nullptr;
    sqlite3_exec(db_, "BEGIN TRANSACTION;", nullptr, nullptr, &errMsg);
    if (rc != SQLITE_OK) {
        Logger::get_logger()->error(
            "Failed to begin SQLite transaction: {}",
            errMsg ? errMsg : sqlite3_errmsg(db_));

        sqlite3_free(errMsg);
        sqlite3_finalize(stmt);

        error_status = StatusCodes::INTERNAL_DB_FAILURE;
        return false;
    }

    // 2. Insert each job in the list to the jobs table
    sqlite3_int64 now = static_cast<sqlite3_int64>(std::time(nullptr));
    for (const auto& job : job_list.jobs) {
        sqlite3_bind_text(stmt,  1, job.job_uuid.c_str(), -1, SQLITE_STATIC);
        sqlite3_bind_text(stmt,  2, job_list.client_id.c_str(), -1, SQLITE_STATIC);
        sqlite3_bind_text(stmt,  3, job_list.workflow_id.c_str(), -1, SQLITE_STATIC);
        sqlite3_bind_text(stmt,  4, job.job_id.c_str(), -1, SQLITE_STATIC);
        sqlite3_bind_int(stmt,   5, to_int(job.status));
        sqlite3_bind_int(stmt,   6, 0);
        sqlite3_bind_int64(stmt, 7, now);
        // submit the job data to the database
        rc = sqlite3_step(stmt);
        if (rc != SQLITE_DONE) {
            // If any job insertion fails, rollback the transaction and return an error
            Logger::get_logger()->error("Failed to insert workflow job: {}", sqlite3_errmsg(db_));

            // Rollback the transaction
            if (sqlite3_exec(db_, "ROLLBACK;", nullptr, nullptr, &errMsg)) {
                Logger::get_logger()->error("Failed to rollback SQLite add_workflow_jobs transaction: {}", errMsg ? errMsg : sqlite3_errmsg(db_));
                sqlite3_free(errMsg);
            }

            sqlite3_finalize(stmt);
            error_status = StatusCodes::INTERNAL_DB_FAILURE;
            return false;
        }

        // Clear stmt for the next iteration
        sqlite3_reset(stmt);
    }

    // 3. Commit the transaction
    if (sqlite3_exec(db_, "COMMIT;", nullptr, nullptr, &errMsg)) {
        Logger::get_logger()->error("Failed to commit transaction: {}", errMsg ? errMsg : sqlite3_errmsg(db_));
        sqlite3_free(errMsg);
        errMsg = nullptr;
        // Attempt to rollback if COMMIT failed
        if (sqlite3_exec(db_, "ROLLBACK;", nullptr, nullptr, &errMsg)) {
            Logger::get_logger()->error("Failed to rollback SQLite add_workflow_jobs transaction: {} after a COMMIT failure", 
                                        errMsg ? errMsg : sqlite3_errmsg(db_));
            sqlite3_free(errMsg);
        }

        error_status = StatusCodes::INTERNAL_DB_FAILURE;
        ret_val = false;
    }

    // All workflow jobs have been successfully commited to the DB
    sqlite3_finalize(stmt);
    return ret_val;
}

bool SQLiteDatabase::update_ready_jobs(const std::string& client_id, const std::string& workflow_id, 
                                       const std::vector<std::string>& queued_job_ids, 
                                       const std::vector<std::string>& ready_job_ids) {
    const char* sql = "UPDATE jobs SET status = ?, ready_at = COALESCE(ready_at, ?), queued_at = COALESCE(queued_at, ?) WHERE client_id = ? AND workflow_id = ? AND job_id = ?;";
    sqlite3_stmt* stmt = nullptr;
    int rc = sqlite3_prepare_v2(db_, sql, -1, &stmt, nullptr);
    if (rc != SQLITE_OK) {
        Logger::get_logger()->error("sqlite prepare failed: {}", sqlite3_errmsg(db_));
        if (stmt) {
            sqlite3_finalize(stmt);
        }

        return false;
    }

    // Begin a transaction to ensure atomicity of the updates
    char* errMsg = nullptr;
    if (sqlite3_exec(db_, "BEGIN TRANSACTION;", nullptr, nullptr, &errMsg)) {
        Logger::get_logger()->error("Failed to begin SQLite transaction: {}", errMsg ? errMsg : sqlite3_errmsg(db_));
        sqlite3_free(errMsg);
        sqlite3_finalize(stmt);
        return false;
    }

    sqlite3_int64 now = static_cast<sqlite3_int64>(std::time(nullptr));
    for (const std::string& job_id : ready_job_ids) {
        sqlite3_bind_int(stmt,   1, to_int(JobStatus::READY));
        sqlite3_bind_int64(stmt, 2, now);
        sqlite3_bind_null(stmt,  3);
        sqlite3_bind_text(stmt,  4, client_id.c_str(), -1, SQLITE_STATIC);
        sqlite3_bind_text(stmt,  5, workflow_id.c_str(), -1, SQLITE_STATIC);
        sqlite3_bind_text(stmt,  6, job_id.c_str(), -1, SQLITE_STATIC);
        rc = sqlite3_step(stmt);
        if (rc != SQLITE_DONE) {
            Logger::get_logger()->error("sqlite step failed: {}", sqlite3_errmsg(db_));
            if (sqlite3_exec(db_, "ROLLBACK;", nullptr, nullptr, &errMsg)) {
                Logger::get_logger()->error("Failed to rollback SQLite update_ready_jobs transaction: {}", 
                                            errMsg ? errMsg : sqlite3_errmsg(db_));
                sqlite3_free(errMsg);
            }
            
            sqlite3_finalize(stmt);
            return false;
        }

        if (sqlite3_changes(db_) == 0) {
            Logger::get_logger()->error(
                "update_ready_jobs - no job row found for READY update client_id={} workflow_id={} job_id={}",
                client_id,
                workflow_id,
                job_id);
            if (sqlite3_exec(db_, "ROLLBACK;", nullptr, nullptr, &errMsg)) {
                Logger::get_logger()->error("Failed to rollback SQLite update_ready_jobs transaction: {}",
                                            errMsg ? errMsg : sqlite3_errmsg(db_));
                sqlite3_free(errMsg);
            }

            sqlite3_finalize(stmt);
            return false;
        }

        // Reset stmt for the next iteration
        sqlite3_reset(stmt); 
    }
    
    for (const std::string& job_id : queued_job_ids) {
        sqlite3_bind_int(stmt,   1, to_int(JobStatus::QUEUED));
        sqlite3_bind_int64(stmt, 2, now);
        sqlite3_bind_int64(stmt, 3, now);
        sqlite3_bind_text(stmt,  4, client_id.c_str(), -1, SQLITE_STATIC);
        sqlite3_bind_text(stmt,  5, workflow_id.c_str(), -1, SQLITE_STATIC);
        sqlite3_bind_text(stmt,  6, job_id.c_str(), -1, SQLITE_STATIC);
        rc = sqlite3_step(stmt);
        if (rc != SQLITE_DONE) {
            Logger::get_logger()->error("sqlite step failed: {}", sqlite3_errmsg(db_));
            if (sqlite3_exec(db_, "ROLLBACK;", nullptr, nullptr, &errMsg)) {
                Logger::get_logger()->error("Failed to rollback SQLite update_ready_jobs transaction: {}", 
                                            errMsg ? errMsg : sqlite3_errmsg(db_));
                sqlite3_free(errMsg);
            }
            
            sqlite3_finalize(stmt);
            return false;
        }

        if (sqlite3_changes(db_) == 0) {
            Logger::get_logger()->error(
                "update_ready_jobs - no job row found for QUEUED update client_id={} workflow_id={} job_id={}",
                client_id,
                workflow_id,
                job_id);
            if (sqlite3_exec(db_, "ROLLBACK;", nullptr, nullptr, &errMsg)) {
                Logger::get_logger()->error("Failed to rollback SQLite update_ready_jobs transaction: {}",
                                            errMsg ? errMsg : sqlite3_errmsg(db_));
                sqlite3_free(errMsg);
            }

            sqlite3_finalize(stmt);
            return false;
        }

        // Reset stmt for the next iteration
        sqlite3_reset(stmt); 
    }

    bool ret_val = true;
    if (sqlite3_exec(db_, "COMMIT;", nullptr, nullptr, &errMsg)) {
        Logger::get_logger()->error("Failed to commit SQLite transaction: {}", errMsg ? errMsg : sqlite3_errmsg(db_));
        sqlite3_free(errMsg);
        errMsg = nullptr;
        if (sqlite3_exec(db_, "ROLLBACK;", nullptr, nullptr, &errMsg)) {
            Logger::get_logger()->error("Failed to rollback SQLite update_ready_jobs transaction: {} after COMMIT failure", 
                                        errMsg ? errMsg : sqlite3_errmsg(db_));
            sqlite3_free(errMsg);
        }

        ret_val = false;
    }

    sqlite3_finalize(stmt);
    return ret_val;
}

bool SQLiteDatabase::update_job_status(const std::string& client_id, const std::string& workflow_id, 
                                       const std::string& job_id, const JobStatus status) {
    std::string sql_cmd;
    switch (status) {
        case JobStatus::COMPLETED:
        case JobStatus::FAILED:
        case JobStatus::CANCELED:
            sql_cmd = "UPDATE jobs SET status = ?, completed_at = ? WHERE client_id = ? AND workflow_id = ? AND job_id = ?;";
            break;
        case JobStatus::RUNNING:
            sql_cmd = "UPDATE jobs SET status = ?, started_at = COALESCE(started_at, ?) WHERE client_id = ? AND workflow_id = ? AND job_id = ?;";
            break;
        case JobStatus::READY:
            sql_cmd = "UPDATE jobs SET status = ?, ready_at = COALESCE(ready_at, ?) WHERE client_id = ? AND workflow_id = ? AND job_id = ?;";
            break;
        case JobStatus::QUEUED:
            sql_cmd = "UPDATE jobs SET status = ?, queued_at = COALESCE(queued_at, ?) WHERE client_id = ? AND workflow_id = ? AND job_id = ?;";
            break;
        default:
            Logger::get_logger()->error("Invalid status for update: {}", to_int(status));
            return false; // Invalid status for update
    }

    const char* sql = sql_cmd.c_str();
    sqlite3_stmt* stmt = nullptr;
    int rc = sqlite3_prepare_v2(db_, sql, -1, &stmt, nullptr);
    if (rc != SQLITE_OK) {
        Logger::get_logger()->error("sqlite prepare failed: {}", sqlite3_errmsg(db_));
        if (stmt) {
            sqlite3_finalize(stmt);
        }
        return false;
    }

    sqlite3_bind_int(stmt,   1, to_int(status));
    sqlite3_bind_int64(stmt, 2, static_cast<sqlite3_int64>(std::time(nullptr)));
    sqlite3_bind_text(stmt,  3, client_id.c_str(), -1, SQLITE_STATIC);
    sqlite3_bind_text(stmt,  4, workflow_id.c_str(), -1, SQLITE_STATIC);
    sqlite3_bind_text(stmt,  5, job_id.c_str(), -1, SQLITE_STATIC);

    bool ret_val = true;
    rc = sqlite3_step(stmt);
    if (rc != SQLITE_DONE) {
        Logger::get_logger()->error("sqlite step failed: {}", sqlite3_errmsg(db_));
        ret_val = false;
    }

    if (ret_val && sqlite3_changes(db_) == 0) {
        Logger::get_logger()->error(
            "update_job_status - no job row found for client_id={} workflow_id={} job_id={}",
            client_id,
            workflow_id,
            job_id);
        ret_val = false;
    }

    sqlite3_finalize(stmt);
    return ret_val;
}

bool SQLiteDatabase::get_job_data(const std::string& client_id, const std::string& workflow_id, 
                   const std::string& job_id, WorkflowJob& job_data) const {

    const char* sql = "SELECT job_uuid, client_id, workflow_id, job_id, status, retry_count, submitted_at, ready_at, queued_at, started_at, completed_at "
                      "FROM jobs WHERE client_id = ? AND workflow_id = ? AND job_id = ?;";
    sqlite3_stmt* stmt;
    bool ret_val = true;

    if (sqlite3_prepare_v2(db_, sql, -1, &stmt, nullptr) == SQLITE_OK) {
        sqlite3_bind_text(stmt, 1, client_id.c_str(), -1, SQLITE_STATIC);
        sqlite3_bind_text(stmt, 2, workflow_id.c_str(), -1, SQLITE_STATIC);
        sqlite3_bind_text(stmt, 3, job_id.c_str(), -1, SQLITE_STATIC);

        if (sqlite3_step(stmt) == SQLITE_ROW) {
            job_data.job_uuid = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 0));
            job_data.client_id = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 1));
            job_data.workflow_id = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 2));
            job_data.job_id = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 3));
            job_data.status = from_int_to_JobStatus(sqlite3_column_int(stmt, 4));
            job_data.retry_num = sqlite3_column_int(stmt, 5);
            job_data.submitted_at = nullable_time_from_sqlite(stmt, 6);
            job_data.ready_at = nullable_time_from_sqlite(stmt, 7);
            job_data.queued_at = nullable_time_from_sqlite(stmt, 8);
            job_data.started_at = nullable_time_from_sqlite(stmt, 9);
            job_data.completed_at = nullable_time_from_sqlite(stmt, 10);
        }
    } else {
        Logger::Logger::get_logger()->error(sqlite3_errmsg(db_));
        ret_val = false;
    }

    sqlite3_finalize(stmt);
    return ret_val;
}

///////////////////////////////////////////////////////////////////////////////////////
//                        Private Methods Implement<tion Zoon                        //
///////////////////////////////////////////////////////////////////////////////////////


SQLiteDatabase::SQLiteDatabase(const std::string& db_path)
    : db_path_(std::move(db_path)), db_(nullptr) {
    init_db();
    create_schema();
}

bool SQLiteDatabase::create_schema() {
    create_rate_limit_plans_table();
    create_policy_plans_table();
    create_clients_table();
    create_workflow_requests_table();
    create_workflows_table();
    create_workflow_payload_table();
    create_jobs_table();
    return true;
}

void SQLiteDatabase::init_db() {
    const std::filesystem::path db_file(db_path_);
    if (!db_file.parent_path().empty()) {
        std::filesystem::create_directories(db_file.parent_path());
    }
    const int rc = sqlite3_open_v2(
        db_path_.c_str(),
        &db_,
        SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_FULLMUTEX,
        nullptr
    );
    if (rc != SQLITE_OK || db_ == nullptr) {
        throw std::runtime_error("Failed to open sqlite database: " + db_path_);
    }
}

bool SQLiteDatabase::add_request_payload(const RequestData& request_data, const std::string& workflow_payload) {
    const char* sql = "INSERT INTO workflow_payloads (client_id, request_id, workflow_id, payload) VALUES (?, ?, ?, ?);";
    sqlite3_stmt* stmt = nullptr;
    bool ret_val = true;

    int rc = sqlite3_prepare_v2(db_, sql, -1, &stmt, nullptr);
    if (rc != SQLITE_OK) {
        std::string error_message = std::string("Failed to prepare SQLite statement: ") + sqlite3_errmsg(db_);
        Logger::get_logger()->error(error_message);
        if (stmt) sqlite3_finalize(stmt);
        return false;
    }

    sqlite3_bind_text(stmt, 1, request_data.client_id.c_str(), -1, SQLITE_STATIC);
    sqlite3_bind_text(stmt, 2, request_data.request_id.c_str(), -1, SQLITE_STATIC);
    sqlite3_bind_text(stmt, 3, request_data.workflow_id.c_str(), -1, SQLITE_STATIC);
    sqlite3_bind_text(stmt, 4, workflow_payload.c_str(), -1, SQLITE_STATIC); 

    if (sqlite3_step(stmt) != SQLITE_DONE) {
        std::string error_message = std::string("Failed to execute SQLite statement: ") + sqlite3_errmsg(db_);
        Logger::get_logger()->error(error_message);
        ret_val = false;
    }

    sqlite3_finalize(stmt);
    return ret_val;
}

bool SQLiteDatabase::get_client_active_workflows_count(const std::string& client_id, int& active_workflows) {
    const char* sql = "SELECT COUNT(*) FROM workflows WHERE client_id = ? AND status IN (?, ?, ?);";
    sqlite3_stmt* stmt;
    bool ret_val = true;
    if (sqlite3_prepare_v2(db_, sql, -1, &stmt, nullptr) == SQLITE_OK) {        
        // Bind client ID variable to the first '?' placeholder (index 1)
        sqlite3_bind_text(stmt, 1, client_id.c_str(), -1, SQLITE_STATIC);
        sqlite3_bind_int(stmt, 2, to_int(WorkflowStatus::ADMITTED));
        sqlite3_bind_int(stmt, 3, to_int(WorkflowStatus::READY));
        sqlite3_bind_int(stmt, 4, to_int(WorkflowStatus::RUNNING));
        // Execute the query
        active_workflows = 0;
        if (sqlite3_step(stmt) == SQLITE_ROW) {
            active_workflows = sqlite3_column_int(stmt, 0);
        } else {
            ret_val = false;
        }

    } else {
        Logger::Logger::get_logger()->error(sqlite3_errmsg(db_));
        ret_val = false;
    }

    sqlite3_finalize(stmt);
    return ret_val;
}

bool SQLiteDatabase::fail_all_jobs_waiting_to_run(const std::string& client_id, const std::string& workflow_id) {
    const char* sql = "UPDATE jobs SET status = ? WHERE client_id = ? AND workflow_id = ? AND status IN (?, ?, ?);";
    sqlite3_stmt* stmt = nullptr;
    int rc = sqlite3_prepare_v2(db_, sql, -1, &stmt, nullptr);
    if (rc != SQLITE_OK) {
        Logger::get_logger()->error("sqlite prepare failed: {}", sqlite3_errmsg(db_));
        if (stmt) {
            sqlite3_finalize(stmt);
        }

        return false;
    }

    sqlite3_bind_int(stmt,  1, to_int(JobStatus::CANCELED));
    sqlite3_bind_text(stmt, 2, client_id.c_str(), -1, SQLITE_STATIC);
    sqlite3_bind_text(stmt, 3, workflow_id.c_str(), -1, SQLITE_STATIC);
    sqlite3_bind_int(stmt,  4, to_int(JobStatus::PENDING));
    sqlite3_bind_int(stmt,  5, to_int(JobStatus::READY));
    sqlite3_bind_int(stmt,  6, to_int(JobStatus::QUEUED));

    bool ret_val = true;
    rc = sqlite3_step(stmt);
    if (rc != SQLITE_DONE) {
        Logger::get_logger()->error("fail_all_pending_jobs - sqlite step failed: {}", sqlite3_errmsg(db_));
        ret_val = false;
    }

    sqlite3_finalize(stmt);
    return ret_val;
}

void SQLiteDatabase::create_rate_limit_plans_table() {
    const char* ddl_cmd =
        "CREATE TABLE IF NOT EXISTS rate_limit_plans ("
        "  plan_name TEXT PRIMARY KEY,"
        "  max_concurrent_workflows INTEGER NOT NULL,"
        "  max_requests INTEGER NOT NULL,"
        "  window_sec INTEGER NOT NULL,"
        "  update_time INTEGER NOT NULL"
        ");";

    execute_ddl_cmd(ddl_cmd);
}

void SQLiteDatabase::create_policy_plans_table() {
    const char* ddl_cmd =
        "CREATE TABLE IF NOT EXISTS policy_plans ("
        "  plan_name TEXT PRIMARY KEY,"
        "  max_workflow_size_kb INTEGER NOT NULL,"
        "  max_jobs_in_workflow INTEGER NOT NULL,"
        "  max_job_size_bytes INTEGER NOT NULL,"
        "  max_workflow_runtime_sec INTEGER NOT NULL,"
        "  max_workflow_total_retries INTEGER NOT NULL,"
        "  max_job_runtime_sec INTEGER NOT NULL,"
        "  max_job_retries INTEGER NOT NULL,"
        "  max_concurrent_jobs INTEGER NOT NULL,"
        "  max_pending_jobs INTEGER NOT NULL,"
        "  workflow_retention_days INTEGER NOT NULL,"
        "  request_retention_days INTEGER NOT NULL,"
        "  payload_retention_days INTEGER NOT NULL,"
        "  update_time INTEGER NOT NULL"
        ");";

    execute_ddl_cmd(ddl_cmd);
}

void SQLiteDatabase::create_clients_table() {
    const char* ddl_cmd =
        "CREATE TABLE IF NOT EXISTS clients ("
        "  client_id TEXT PRIMARY KEY,"
        "  rate_limit_plan_name TEXT NOT NULL,"
        "  policy_plan_name TEXT NOT NULL,"
        "  status INTEGER NOT NULL,"
        "  update_time INTEGER NOT NULL,"
        "  FOREIGN KEY(rate_limit_plan_name) REFERENCES rate_limit_plans(plan_name),"
        "  FOREIGN KEY(policy_plan_name) REFERENCES policy_plans(plan_name)"
        ");";

    execute_ddl_cmd(ddl_cmd);
}

void SQLiteDatabase::create_workflow_requests_table() {
    const char* ddl_cmd =
        "CREATE TABLE IF NOT EXISTS workflow_requests ("
        "  client_id TEXT NOT NULL,"
        "  request_id TEXT NOT NULL,"
        "  workflow_id TEXT NOT NULL,"
        "  payload_size INTEGER NOT NULL,"
        "  operation_type TEXT NOT NULL,"
        "  status INTEGER NOT NULL,"
        "  reject_reason TEXT NOT NULL,"
        "  received_at INTEGER,"
        "  PRIMARY KEY (client_id, request_id)"
        ");";

    execute_ddl_cmd(ddl_cmd);
}

void SQLiteDatabase::create_workflows_table() {
    const char* ddl_cmd =
        "CREATE TABLE IF NOT EXISTS workflows ("
        "  client_id TEXT NOT NULL,"
        "  workflow_id TEXT NOT NULL,"
        "  workflow_type TEXT NOT NULL,"
        "  version TEXT NOT NULL,"
        "  status INTEGER NOT NULL,"
        "  total_jobs INTEGER NOT NULL,"
        "  received_at INTEGER NOT NULL,"
        "  started_at INTEGER,"
        "  ready_at INTEGER,"
        "  completed_at INTEGER,"
        "  PRIMARY KEY (client_id, workflow_id),"
        "  FOREIGN KEY(client_id) REFERENCES clients(client_id)"
        ");";

    execute_ddl_cmd(ddl_cmd);
}


void SQLiteDatabase::create_workflow_payload_table() {
    const char* ddl_cmd =
        "CREATE TABLE IF NOT EXISTS workflow_payloads ("
        "  payload_sequence INTEGER PRIMARY KEY AUTOINCREMENT,"
        "  client_id TEXT NOT NULL,"
        "  request_id TEXT NOT NULL,"
        "  workflow_id TEXT NOT NULL,"
        "  payload TEXT NOT NULL,"
        "  UNIQUE (client_id, request_id)"
        ");";

    execute_ddl_cmd(ddl_cmd);

    // Create an index on the workflow_payloads table for faster lookups by client_id and workflow_id
    execute_ddl_cmd(
        "CREATE INDEX IF NOT EXISTS idx_payload_workflow ON workflow_payloads(client_id, workflow_id);"
    );
}

void SQLiteDatabase::create_jobs_table() {
    const char* ddl_cmd =
        "CREATE TABLE IF NOT EXISTS jobs ("
        "  job_uuid TEXT PRIMARY KEY,"
        "  client_id TEXT NOT NULL,"
        "  workflow_id TEXT NOT NULL,"
        "  job_id TEXT NOT NULL,"
        "  status INTEGER NOT NULL,"
        "  retry_count INTEGER NOT NULL,"
        "  submitted_at INTEGER,"
        "  ready_at INTEGER,"
        "  queued_at INTEGER,"
        "  started_at INTEGER,"
        "  completed_at INTEGER,"
        "  FOREIGN KEY (client_id, workflow_id) REFERENCES workflows(client_id, workflow_id),"
        "  UNIQUE (client_id, workflow_id, job_id)"
        ");";

    execute_ddl_cmd(ddl_cmd);
}

void SQLiteDatabase::execute_ddl_cmd(const char* ddl_cmd) {
    char* err_msg = nullptr;
    const int ddl_rc = sqlite3_exec(db_, ddl_cmd, nullptr, nullptr, &err_msg);
    if (ddl_rc != SQLITE_OK) {
        std::string msg = err_msg ? err_msg : "unknown sqlite error";
        sqlite3_free(err_msg);
        throw std::runtime_error("Failed to initialize DB schema: " + msg);
    }
}

// bool SQLiteDatabase::prepare_request(const char* sql, sqlite3_stmt** stmt) {
//     bool ret_val = true;
//     int rc = sqlite3_prepare_v2(db_, sql, -1, stmt, nullptr);
//     if (rc != SQLITE_OK) {
//         Logger::get_logger()->error("prepare_request - sqlite prepare failed: {}", sqlite3_errmsg(db_));
//         if (*stmt) {
//             sqlite3_finalize(*stmt);
//         }

//         ret_val = false;
//     }

//     return ret_val;
// }

} // namespace flow_pilot
