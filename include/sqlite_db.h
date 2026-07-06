// sqlite_db.h - SQLite persistence interface for FlowPilot

#pragma once

#include <functional>
#include <optional>
#include <string>
#include <unordered_map>
#include <sqlite3.h>

#include "db_interface.h"
#include "client_data.h"


namespace flow_pilot {

class SQLiteDatabase : public IDatabase {
public:
    ~SQLiteDatabase() override;

    // Get the singleton instance using the configured SQLite DB path.
    static IDatabase& get_instance();
    
    // Delete copy/move
    SQLiteDatabase(const SQLiteDatabase&) = delete;
    SQLiteDatabase& operator=(const SQLiteDatabase&) = delete;
    SQLiteDatabase(SQLiteDatabase&&) = delete;
    SQLiteDatabase& operator=(SQLiteDatabase&&) = delete;

    bool get_rate_limit_plans(
        std::unordered_map<std::string, RateLimitConfig> &rate_limit_plans
    ) const override;

    bool get_rate_limit_plan(
        RateLimitConfig& plan_rate_limit
    ) const override;

    bool upsert_rate_limit_plan(
        const RateLimitConfig& rate_limit_plan
    ) override;

    bool get_policy_plans(
        std::unordered_map<std::string, PolicyPlan> &policy_plans
    )  const override;

    bool get_policy_plan(
        PolicyPlan& plan_policy
    ) const override;

    bool upsert_policy_plan(
        const PolicyPlan& policy_plan
    ) override;

    bool get_all_users(
        std::unordered_map<std::string, ClientData> &clients
    ) const override;

    bool get_user_config(
        ClientData& user_data
    ) const override;
    
    bool upsert_user_config(
        const std::string& user_id,
        const std::string& rate_limit_plan_name,
        const std::string& policy_plan_name
    ) override;

    bool add_request(
        const RequestData& request_data,
        const std::string& workflow_payload,
        std::string& error_message
    ) override;

    bool add_workflow(
        const WorkflowfullData& workflow_data,
        std::string& error_message
    ) override;

    bool update_workflow_status(const std::string& client_id, const std::string& workflow_id, const std::string& status) override;
    
    bool get_all_active_workflows(std::vector<WorkflowfullData>& workflows) const override;

    bool get_all_workflows_for_client(const std::string& client_id, std::vector<WorkflowfullData>& workflows) const override;

private:
    explicit SQLiteDatabase(const std::string& db_path);
    bool create_schema() override;
    bool get_client_active_workflows_count(std::string& client_id, int& active_workflows) override;

    void init_db();
    void create_rate_limit_plans_table();
    void create_policy_plans_table();
    void create_clients_table();
    void create_workflows_table();
    void create_workflow_requests_table();
    void create_jobs_table();
    void create_users_stats_table();
    void create_workflow_payload_table();
    void create_table(const char* ddl_cmd);
    bool rollback(const std::string& error_message);
    bool execute_statement(
        const char* sql,
        const std::function<void(sqlite3_stmt*)>& binder,
        const std::function<std::string(int, const char*)>& failure_handler);
    size_t get_client_active_requests(std::string& client_id);
    bool add_request_payload(const RequestData& request_data, const std::string& workflow_payload);

    std::string db_path_;
    sqlite3* db_ = nullptr;
};
} // namespace flow_pilot
