
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

SQLiteDatabase::SQLiteDatabase(const std::string& db_path)
    : db_path_(std::move(db_path)), db_(nullptr) {
    init_db();
    create_schema();
}

SQLiteDatabase::~SQLiteDatabase() {
    if (db_) {
        sqlite3_close(db_);
        db_ = nullptr;
    }
}

bool SQLiteDatabase::get_rate_limit_plans(std::unordered_map<std::string, RateLimitConfig> &rate_limit_plans) const {
    const char* sql = "SELECT plan_name, max_concurrent_workflows, max_requests, window_sec FROM rate_limit_plans;";
    sqlite3_stmt* stmt = nullptr;
    int rc = sqlite3_prepare_v2(db_, sql, -1, &stmt, nullptr);
    if (rc != SQLITE_OK) {
        get_logger()->error("sqlite prepare failed: {}", sqlite3_errmsg(db_));
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
        get_logger()->error("sqlite prepare failed: {}", sqlite3_errmsg(db_));
        if (stmt) {
            sqlite3_finalize(stmt);
        }
        return false;
    }
    
    sqlite3_bind_text(stmt, 1, rate_limit_plan.plan_name.c_str(), -1, SQLITE_TRANSIENT);
    rc = sqlite3_step(stmt);
    if (rc == SQLITE_ROW) {
        rate_limit_plan.max_concurrent_workflows = sqlite3_column_int(stmt, 0);
        rate_limit_plan.max_requests = sqlite3_column_int(stmt, 1);
        rate_limit_plan.window_sec = sqlite3_column_int(stmt, 2);
    } else {
        get_logger()->error("Failed to fined rate limit plan '{}' in DB", rate_limit_plan.plan_name);
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
        get_logger()->error("sqlite prepare failed: {}", sqlite3_errmsg(db_));
        if (stmt) {
            sqlite3_finalize(stmt);
        }
        return false;
    }

    sqlite3_bind_text(stmt, 1, rate_limit_plan.plan_name.c_str(), -1, SQLITE_TRANSIENT);
    sqlite3_bind_int(stmt, 2, rate_limit_plan.max_concurrent_workflows);
    sqlite3_bind_int(stmt, 3, rate_limit_plan.max_requests);
    sqlite3_bind_int(stmt, 4, rate_limit_plan.window_sec);
    sqlite3_bind_int64(stmt, 5, static_cast<sqlite3_int64>(std::time(nullptr)));

    rc = sqlite3_step(stmt);
    if (rc != SQLITE_DONE) {
        get_logger()->error("sqlite step failed: {}", sqlite3_errmsg(db_));
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
        get_logger()->error("sqlite prepare failed: {}", sqlite3_errmsg(db_));
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
        get_logger()->error("sqlite prepare failed: {}", sqlite3_errmsg(db_));
        if (stmt) sqlite3_finalize(stmt);
        return false;
    }
    sqlite3_bind_text(stmt, 1, plan_policy.plan_name.c_str(), -1, SQLITE_TRANSIENT);
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
        get_logger()->error("Failed to find policy plan '{}' in DB", plan_policy.plan_name);
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
        get_logger()->error("sqlite prepare failed: {}", sqlite3_errmsg(db_));
        if (stmt) {
            sqlite3_finalize(stmt);
        }

        return false;
    }

    sqlite3_bind_text(stmt, 1, policy_plan.plan_name.c_str(), -1, SQLITE_TRANSIENT);
    sqlite3_bind_int(stmt, 2, policy_plan.max_workflow_size_kb);
    sqlite3_bind_int(stmt, 3, policy_plan.max_jobs_in_workflow);
    sqlite3_bind_int(stmt, 4, policy_plan.max_job_size_bytes);
    sqlite3_bind_int(stmt, 5, policy_plan.max_workflow_runtime_sec);
    sqlite3_bind_int(stmt, 6, policy_plan.max_workflow_total_retries);
    sqlite3_bind_int(stmt, 7, policy_plan.max_job_runtime_sec);
    sqlite3_bind_int(stmt, 8, policy_plan.max_job_retries);
    sqlite3_bind_int(stmt, 9, policy_plan.max_concurrent_jobs);
    sqlite3_bind_int(stmt, 10, policy_plan.max_pending_jobs); 
    sqlite3_bind_int(stmt, 11, policy_plan.workflow_retention_days); 
    sqlite3_bind_int(stmt, 12, policy_plan.request_retention_days); 
    sqlite3_bind_int(stmt, 13, policy_plan.payload_retention_days);
    sqlite3_bind_int64(stmt, 14, static_cast<sqlite3_int64>(std::time(nullptr)));

    rc = sqlite3_step(stmt);
    if (rc != SQLITE_DONE) {
        get_logger()->error("sqlite step failed: {}", sqlite3_errmsg(db_));
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
        get_logger()->error("sqlite prepare failed: {}", sqlite3_errmsg(db_));
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
        get_logger()->error("sqlite prepare failed: {}", sqlite3_errmsg(db_));
        if (stmt) sqlite3_finalize(stmt);
        return false;
    }

    sqlite3_bind_text(stmt, 1, user_id.c_str(), -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 2, rate_limit_plan_name.c_str(), -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 3, policy_plan_name.c_str(), -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 4, "ACTIVE", -1, SQLITE_TRANSIENT);
    sqlite3_bind_int64(stmt, 5, static_cast<sqlite3_int64>(std::time(nullptr)));

    rc = sqlite3_step(stmt);
    if (rc != SQLITE_DONE) {
        get_logger()->error("sqlite step failed: {}", sqlite3_errmsg(db_));
        ret_val = false;
    }

    sqlite3_finalize(stmt);
    return ret_val;
}

bool SQLiteDatabase::get_user_config(ClientData& user_data) const {
    bool ret_val = true;
    const char* sql = "SELECT rate_limit_plan_name, policy_plan_name FROM clients WHERE client_id = ? LIMIT 1;";
    sqlite3_stmt* stmt = nullptr;

    int rc = sqlite3_prepare_v2(db_, sql, -1, &stmt, nullptr);
    if (rc != SQLITE_OK) {
        get_logger()->error("sqlite prepare failed: {}", sqlite3_errmsg(db_));
        if (stmt) sqlite3_finalize(stmt);
        return false;
    }

    sqlite3_bind_text(stmt, 1, user_data.client_id.c_str(), -1, SQLITE_TRANSIENT);
    rc = sqlite3_step(stmt);
    if (rc == SQLITE_ROW) {
        const unsigned char* rl = sqlite3_column_text(stmt, 0);
        const unsigned char* pp = sqlite3_column_text(stmt, 1);
        user_data.rate_limit_config_plan_name = rl ? reinterpret_cast<const char*>(rl) : std::string();
        user_data.policy_plan_name = pp ? reinterpret_cast<const char*>(pp) : std::string();
    } else {
        get_logger()->error("Failed to find client_id '{}' in DB", user_data.client_id);
        ret_val = false;
    }
    
    sqlite3_finalize(stmt);
    return ret_val;
}


bool SQLiteDatabase::add_request(const RequestData& request_data,
                                 std::string& error_message) {
    size_t client_active_requests = get_client_active_requests(request_data.client_id)
    const sqlite3_int64 now = static_cast<sqlite3_int64>(std::time(nullptr));
    const char* sql = "INSERT INTO workflow_requests (client_id, request_id, workflow_id, received_at) VALUES (?, ?, ?, ?);";
    sqlite3_stmt* stmt = nullptr;
    bool ret_val = true;

    int rc = sqlite3_prepare_v2(db_, sql, -1, &stmt, nullptr);
    if (rc != SQLITE_OK) {
        error_message = std::string("Failed to prepare SQLite statement: ") + sqlite3_errmsg(db_);
        get_logger()->error(error_message);
        if (stmt) sqlite3_finalize(stmt);
        return false;
    }

    sqlite3_bind_text(stmt, 1, request_data.client_id.c_str(), -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 2, request_data.request_id.c_str(), -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 3, request_data.workflow_id.c_str(), -1, SQLITE_TRANSIENT);
    sqlite3_bind_int64(stmt, 4, now);

    rc = sqlite3_step(stmt);
    if (rc != SQLITE_DONE) {
        std::string msg;
        if (rc == SQLITE_CONSTRAINT) {
            msg = std::string("Duplicate request: request id=") + request_data.request_id + " client_id=" + request_data.client_id;
            error_message = DUPLICATE_REQUEST;
        } else {
            msg = std::string("Failed to insert request: ") + sqlite3_errmsg(db_);
            error_message = INTERNAL_DB_FAILURE;
        }
        get_logger()->error(msg);
        ret_val = false;
    }

    sqlite3_finalize(stmt);
    return ret_val;
}

bool SQLiteDatabase::add_workflow(const RequestData& request_data,
                                  const std::string& workflow_payload,
                                  std::string& error_message) {
    const char* sql_new_request = "INSERT INTO workflow_requests (client_id, request_id, workflow_id, payload, received_at) "
                                  "VALUES (?, ?, ?, ?, ?);";
    const char* sql_new_workflow = "INSERT INTO workflows (client_id, workflow_id, workflow_type, version, status, total_jobs, jobs_running, jobs_completed, received_at, started_at, updated_at, last_state_change_at) "
                                  "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);";
    const sqlite3_int64 now = static_cast<sqlite3_int64>(std::time(nullptr));

    if (sqlite3_exec(db_, "BEGIN TRANSACTION", nullptr, nullptr, nullptr) != SQLITE_OK) {
        error_message = std::string("Failed to begin SQLite transaction: ") + sqlite3_errmsg(db_);
        get_logger()->error(error_message);
        return false;
    }

    // Bind all parameters for the workflow request row.
    // This inserts a new request record and uses the same transaction context
    // as the workflow row that follows.
    if (!execute_statement(sql_new_request,
            [&](sqlite3_stmt* stmt) {
                sqlite3_bind_text(stmt, 1, request_data.client_id.c_str(), -1, SQLITE_TRANSIENT);
                sqlite3_bind_text(stmt, 2, request_data.workflow_id.c_str(), -1, SQLITE_TRANSIENT);
                sqlite3_bind_text(stmt, 3, request_data.request_id.c_str(), -1, SQLITE_TRANSIENT);
                sqlite3_bind_text(stmt, 4, workflow_payload.c_str(), -1, SQLITE_TRANSIENT);
                sqlite3_bind_int64(stmt, 5, now);
            },
            [&](int rc, const char* sqlite_err) {
                if (rc == SQLITE_CONSTRAINT) {
                    error_message = DUPLICATE_REQUEST;
                    return std::string("Duplicate request: request id=") + request_data.request_id + " client_id=" + request_data.client_id;
                }
                
                error_message = INTERNAL_DB_FAILURE;
                return std::string("Failed to insert workflow request: ") + sqlite_err;
            })) {
        return false;
    }

    // Bind all parameters for the workflow metadata row.
    // This creates the workflow record linked to the request above.
    if (!execute_statement(sql_new_workflow,
            [&](sqlite3_stmt* stmt) {
                sqlite3_bind_text(stmt, 1, request_data.client_id.c_str(), -1, SQLITE_TRANSIENT);
                sqlite3_bind_text(stmt, 2, request_data.workflow_id.c_str(), -1, SQLITE_TRANSIENT);
                sqlite3_bind_text(stmt, 3, request_data.workflow_type.c_str(), -1, SQLITE_TRANSIENT);
                sqlite3_bind_text(stmt, 4, request_data.workflow_version.c_str(), -1, SQLITE_TRANSIENT);
                sqlite3_bind_text(stmt, 5, request_data.status.c_str(), -1, SQLITE_TRANSIENT);
                sqlite3_bind_int(stmt, 6, request_data.job_count);
                sqlite3_bind_int(stmt, 7, 0);
                sqlite3_bind_int(stmt, 8, 0);
                sqlite3_bind_int64(stmt, 9, now);
                sqlite3_bind_int64(stmt, 10, now);
                sqlite3_bind_int64(stmt, 11, now);
                sqlite3_bind_int64(stmt, 12, now);
            },
            [&](int rc, const char* sqlite_err) {
                if (rc == SQLITE_CONSTRAINT) {
                    error_message = WORKFLOW_ID_EXISTS;
                    return std::string("Duplicate workflow: workflow id=") + request_data.workflow_id + " client_id=" + request_data.client_id;
                }
                error_message = INTERNAL_DB_FAILURE;
                return std::string("Failed to insert workflow record: ") + sqlite_err;
            })) {
        return false;
    }

    // Commit the transaction once both rows have been inserted successfully.
    // If commit fails, rollback ensures we do not leave a partially applied workflow.
    if (sqlite3_exec(db_, "COMMIT", nullptr, nullptr, nullptr) != SQLITE_OK) {
        error_message = INTERNAL_DB_FAILURE;
        return rollback(std::string("Failed to commit SQLite transaction: ") + sqlite3_errmsg(db_));
    }

    return true;
}

bool SQLiteDatabase::update_workflow_status(const std::string& client_id, const std::string& workflow_id, const std::string& status) {
    const char* sql = "UPDATE workflows SET status = ?, updated_at = ? WHERE client_id = ? AND workflow_id = ?;";
    sqlite3_stmt* stmt = nullptr;
    int rc = sqlite3_prepare_v2(db_, sql, -1, &stmt, nullptr);
    if (rc != SQLITE_OK) {
        get_logger()->error("sqlite prepare failed: {}", sqlite3_errmsg(db_));
        if (stmt) sqlite3_finalize(stmt);
        return false;
    }
    sqlite3_bind_text(stmt, 1, status.c_str(), -1, SQLITE_TRANSIENT);
    sqlite3_bind_int64(stmt, 2, static_cast<sqlite3_int64>(std::time(nullptr)));
    sqlite3_bind_text(stmt, 3, client_id.c_str(), -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 4, workflow_id.c_str(), -1, SQLITE_TRANSIENT);
    rc = sqlite3_step(stmt);
    if (rc != SQLITE_DONE) {
        get_logger()->error("sqlite step failed: {}", sqlite3_errmsg(db_));
        sqlite3_finalize(stmt);
        return false;
    }
    sqlite3_finalize(stmt);
    return true;
}

bool SQLiteDatabase::get_all_active_workflows(std::vector<WorkflowfullData>& workflows) const {
    const char* sql = "SELECT client_id, workflow_id, workflow_type, version, status FROM workflows WHERE status IN ('RECEIVED', 'ADMITTED', 'RUNNING');";
    sqlite3_stmt* stmt = nullptr;
    int rc = sqlite3_prepare_v2(db_, sql, -1, &stmt, nullptr);
    if (rc != SQLITE_OK) {
        get_logger()->error("sqlite prepare failed: {}", sqlite3_errmsg(db_));
        if (stmt) sqlite3_finalize(stmt);
        return false;
    }
    while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
        WorkflowfullData wd;
        const unsigned char* cid = sqlite3_column_text(stmt, 0);
        const unsigned char* wid = sqlite3_column_text(stmt, 1);
        const unsigned char* wt = sqlite3_column_text(stmt, 2);
        const unsigned char* wv = sqlite3_column_text(stmt, 3);
        const unsigned char* st = sqlite3_column_text(stmt, 4);
        wd.info.client_id = cid ? reinterpret_cast<const char*>(cid) : std::string();
        wd.info.workflow_id = wid ? reinterpret_cast<const char*>(wid) : std::string();
        wd.info.workflow_type = wt ? reinterpret_cast<const char*>(wt) : std::string();
        wd.info.workflow_version = wv ? reinterpret_cast<const char*>(wv) : std::string();
        wd.info.status = st ? reinterpret_cast<const char*>(st) : std::string();
        workflows.push_back(wd);
    }
    sqlite3_finalize(stmt);
    return true;
}

bool SQLiteDatabase::get_all_workflows_for_client(const std::string& client_id, std::vector<WorkflowfullData>& workflows) const {
    const char* sql = "SELECT client_id, workflow_id, workflow_type, version, status FROM workflows WHERE client_id = ?;";
    sqlite3_stmt* stmt = nullptr;
    int rc = sqlite3_prepare_v2(db_, sql, -1, &stmt, nullptr);
    if (rc != SQLITE_OK) {
        get_logger()->error("sqlite prepare failed: {}", sqlite3_errmsg(db_));
        if (stmt) sqlite3_finalize(stmt);
        return false;
    }
    sqlite3_bind_text(stmt, 1, client_id.c_str(), -1, SQLITE_TRANSIENT);
    while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
        WorkflowfullData wd;
        const unsigned char* cid = sqlite3_column_text(stmt, 0);
        const unsigned char* wid = sqlite3_column_text(stmt, 1);
        const unsigned char* wt = sqlite3_column_text(stmt, 2);
        const unsigned char* wv = sqlite3_column_text(stmt, 3);
        const unsigned char* st = sqlite3_column_text(stmt, 4);
        wd.info.client_id = cid ? reinterpret_cast<const char*>(cid) : std::string();
        wd.info.workflow_id = wid ? reinterpret_cast<const char*>(wid) : std::string();
        wd.info.workflow_type = wt ? reinterpret_cast<const char*>(wt) : std::string();
        wd.info.workflow_version = wv ? reinterpret_cast<const char*>(wv) : std::string();
        wd.info.status = st ? reinterpret_cast<const char*>(st) : std::string();
        workflows.push_back(wd);
    }
    sqlite3_finalize(stmt);
    return true;
}

bool SQLiteDatabase::create_schema() {
    create_rate_limit_plans_table();
    create_policy_plans_table();
    create_clients_table();
    create_workflows_table();
    create_workflow_requests_table();
    create_jobs_table();
    create_users_stats_table();
    return true;
}

IDatabase& SQLiteDatabase::get_instance() {
    static SQLiteDatabase instance(Config::get_config().db_config().db_path);
    return dynamic_cast<IDatabase&>(instance);
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

void SQLiteDatabase::create_rate_limit_plans_table() {
    const char* ddl_cmd =
        "CREATE TABLE IF NOT EXISTS rate_limit_plans ("
        "  plan_name TEXT PRIMARY KEY,"
        "  max_concurrent_workflows INTEGER NOT NULL,"
        "  max_requests INTEGER NOT NULL,"
        "  window_sec INTEGER NOT NULL,"
        "  update_time INTEGER NOT NULL"
        ");";

    create_table(ddl_cmd);
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

    create_table(ddl_cmd);
}

void SQLiteDatabase::create_clients_table() {
    const char* ddl_cmd =
        "CREATE TABLE IF NOT EXISTS clients ("
        "  client_id TEXT PRIMARY KEY,"
        "  rate_limit_plan_name TEXT NOT NULL,"
        "  policy_plan_name TEXT NOT NULL,"
        "  status TEXT NOT NULL,"
        "  update_time INTEGER NOT NULL,"
        "  FOREIGN KEY(rate_limit_plan_name) REFERENCES rate_limit_plans(plan_name),"
        "  FOREIGN KEY(policy_plan_name) REFERENCES policy_plans(plan_name)"
        ");";

    create_table(ddl_cmd);
}

void SQLiteDatabase::create_workflow_requests_table() {
    const char* ddl_cmd =
        "CREATE TABLE IF NOT EXISTS workflow_requests ("
        "  client_id TEXT NOT NULL,"
        "  request_id TEXT NOT NULL,"
        "  workflow_id TEXT NOT NULL,"
        "  payload TEXT NOT NULL,"
        "  received_at INTEGER,"
        "  PRIMARY KEY (client_id, request_id),"
        "  FOREIGN KEY(client_id, workflow_id) REFERENCES workflows(client_id, workflow_id)"
        ");";

    create_table(ddl_cmd);
}

void SQLiteDatabase::create_workflows_table() {
    const char* ddl_cmd =
        "CREATE TABLE IF NOT EXISTS workflows ("
        "  client_id TEXT NOT NULL,"
        "  workflow_id TEXT NOT NULL,"
        "  workflow_type TEXT NOT NULL,"
        "  version TEXT NOT NULL,"
        "  status TEXT NOT NULL,"
        "  total_jobs INTEGER NOT NULL,"
        "  jobs_running INTEGER NOT NULL,"
        "  jobs_completed INTEGER NOT NULL,"
        "  received_at INTEGER NOT NULL,"
        "  started_at INTEGER NOT NULL,"
        "  updated_at INTEGER,"
        "  last_state_change_at INTEGER NOT NULL,"
        "  PRIMARY KEY (client_id, workflow_id),"
        "  FOREIGN KEY(client_id) REFERENCES clients(client_id)"
        ");";

    create_table(ddl_cmd);
}

void SQLiteDatabase::create_jobs_table() {
    const char* ddl_cmd =
        "CREATE TABLE IF NOT EXISTS jobs ("
        "  client_id TEXT NOT NULL,"
        "  workflow_id TEXT NOT NULL,"
        "  job_id TEXT NOT NULL,"
        "  status TEXT NOT NULL,"
        "  completed_at INTEGER,"
        "  retry_count INTEGER NOT NULL,"
        "  PRIMARY KEY (client_id, workflow_id, job_id),"
        "  FOREIGN KEY (client_id, workflow_id) REFERENCES workflows(client_id, workflow_id)"
        ");";

    create_table(ddl_cmd);
}

void SQLiteDatabase::create_users_stats_table() {
    const char* ddl_cmd =
        "CREATE TABLE IF NOT EXISTS users_stats ("
        "  user_id TEXT PRIMARY KEY,"
        "  requests INTEGER NOT NULL,"
        "  last_request INTEGER"
        ");";

    create_table(ddl_cmd);
}

void SQLiteDatabase::create_table(const char* ddl_cmd) {
    char* err_msg = nullptr;
    const int ddl_rc = sqlite3_exec(db_, ddl_cmd, nullptr, nullptr, &err_msg);
    if (ddl_rc != SQLITE_OK) {
        std::string msg = err_msg ? err_msg : "unknown sqlite error";
        sqlite3_free(err_msg);
        throw std::runtime_error("Failed to initialize DB schema: " + msg);
    }
}

bool SQLiteDatabase::get_client_active_workflows_count(std::string& client_id, int& active_workflows) {
    const char* sql = "SELECT COUNT(*) FROM workflows WHERE client_id = ? AND status IN ('ADMITTED', 'RUNNING');";
    sqlite3_stmt* stmt;
    bool ret_val = true;
    if (sqlite3_prepare_v2(db_, sql, -1, &stmt, nullptr) == SQLITE_OK) {        
        // Bind client ID variable to the first '?' placeholder (index 1)
        sqlite3_bind_text(stmt, 1, client_id.c_str(), -1, SQLITE_TRANSIENT);
        // Execute the query
        active_workflows = 0;
        if (sqlite3_step(stmt) == SQLITE_ROW) {
            active_workflows = sqlite3_column_int(stmt, 0);
        }

        sqlite3_finalize(stmt);
    } else {
        Logger::get_instance()->error(sqlite3_errmsg(db_));
        ret_val = false;
    }

    return ret_val;
}

// bool SQLiteDatabase::rollback(std::string& error_message) {
//     if (sqlite3_exec(db_, "ROLLBACK", nullptr, nullptr, nullptr) != SQLITE_OK) {
//         get_logger()->error("SQLite rollback failed: {}", sqlite3_errmsg(db_));
//     }
//     get_logger()->error(error_message);
//     return false;
// }

// bool SQLiteDatabase::execute_statement (const char* sql,
//                                         sqlite3_stmt* stmt,
//                                         std::string failure_message) {
//     sqlite3_stmt* stmt = nullptr;
//     int rc = sqlite3_prepare_v2(db_, sql, -1, &stmt, nullptr);
//     if (rc != SQLITE_OK) {
//         return rollback(failure_message(rc, sqlite3_errmsg(db_)));
//     }

//     std::unique_ptr<sqlite3_stmt, decltype(&sqlite3_finalize)> stmt_guard(stmt, sqlite3_finalize);
//     binder(stmt);
//     rc = sqlite3_step(stmt);
//     if (rc != SQLITE_DONE) {
//         return rollback(failure_message(rc, sqlite3_errmsg(db_)));
//     }
//     return true;
// }

} // namespace flow_pilot
