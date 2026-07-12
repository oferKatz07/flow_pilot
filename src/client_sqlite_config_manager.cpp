// client_sqlite_config_manager.cpp - Implementation of ClientSqliteConfigManager

#include <memory>
#include <vector>

#include "logger.h"
#include "sqlite_db.h"
#include "client_sqlite_config_manager.h"

namespace flow_pilot {

ClientSqliteConfigManager& ClientSqliteConfigManager::get_instance() {
    static ClientSqliteConfigManager instance;

    return instance;
}

bool ClientSqliteConfigManager::get_client_config(const std::string& client_id, ClientConfig& out) const {
    auto cit = clients_.find(client_id);
    if (cit == clients_.end()) {
        return false;
    }
    out.client_id = cit->second.client_id;
    const auto& rl_name = cit->second.rate_limit_config_plan_name;
    const auto& pp_name = cit->second.policy_plan_name;

    auto rlit = rate_limit_plans_.find(rl_name);
    if (rlit != rate_limit_plans_.end()) {
        out.rate_limit_config = rlit->second;
    }

    auto ppit = policy_plans_.find(pp_name);
    if (ppit != policy_plans_.end()) {
        out.policy_config = ppit->second;
    }

    return true;
}

void ClientSqliteConfigManager::load_from_db() {
    auto& db = SQLiteDatabase::get_instance();
    db.get_rate_limit_plans(rate_limit_plans_);
    db.get_policy_plans(policy_plans_);
    db.get_all_users(clients_);
}

ClientSqliteConfigManager::ClientSqliteConfigManager() {
    load_from_db();
    if (rate_limit_plans_.empty() || policy_plans_.empty() || clients_.empty()) {
        ensure_defaults();
        // reload after inserting defaults
        load_from_db();
    }
}

void ClientSqliteConfigManager::ensure_defaults() {
    auto& db = SQLiteDatabase::get_instance();

    // Default rate limit plans
    RateLimitConfig rplans[] = {
        {"sandbox", 10, 60, 1},
        {"basic", 100, 60, 5},
        {"professional", 1000, 60, 10},
        {"enterprise", 10000, 60, 50}
    };

    for (const auto& p : rplans) {
        db.upsert_rate_limit_plan(p);
    }

    // Default policy plans
    PolicyPlan pplans[] = {
        // name,         wfz, jwf, js,   wfrt, wfr,jrt, jr, cj, pj, wfr, rr,  pr
        {"sandbox",      10,  5,   200,  300,   3,  60,  1, 2,  18,  1,   1,  1},
        {"basic",        20,  10,  500,  600,   10, 300, 2, 5,  55,  10,  10, 10},
        {"professional", 50,  50,  800,  6000,  20, 600, 3, 10, 90,  30,  30, 30},
        {"enterprise",   100, 100, 1024, 12000, 40, 900, 5, 20, 180, 60,  60, 60}
    };

    for (const auto& p : pplans) {
        db.upsert_policy_plan(p);
    }

    // Create clients client_1 .. client_10 and assign plans cyclically
    std::vector<std::string> rate_names = {"professional", "basic", "sandbox", "enterprise"};
    std::vector<std::string> policy_names = {"basic", "sandbox", "professional", "enterprise"};

    for (int i = 1; i <= 10; ++i) {
        std::string client_id = "client_" + std::to_string(i);
        std::string rl = rate_names[(i-1) % rate_names.size()];
        std::string pp = policy_names[(i-1) % policy_names.size()];
        db.upsert_user_config(client_id, rl, pp);
    }

    get_logger()->info("Inserted default rate/policy plans and clients into SQLite database");
}

} // namespace flow_pilot
