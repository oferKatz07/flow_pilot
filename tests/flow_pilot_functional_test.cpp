#include <gtest/gtest.h>

#include <boost/asio/io_context.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/use_future.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <chrono>
#include <cstdint>
#include <nlohmann/json.hpp>
#include <random>
#include <string>
#include <thread>

#include "completion_handler.h"
#include "config.h"
#include "http_server.h"
#include "redis_db_async.h"
#include "redis_test_utils.h"
#include "scheduler.h"

using namespace flow_pilot;
using json = nlohmann::json;
namespace asio = boost::asio;
namespace beast = boost::beast;
namespace http = beast::http;
using tcp = asio::ip::tcp;

namespace {

struct HttpResult {
    http::status status;
    std::string body;
};

std::string generate_unique_id()
{
    static std::mt19937_64 rng(static_cast<unsigned long long>(std::chrono::high_resolution_clock::now().time_since_epoch().count()));
    std::uniform_int_distribution<unsigned long long> dist;
    return std::to_string(dist(rng));
}

unsigned short reserve_free_port()
{
    asio::io_context ioc;
    tcp::acceptor acceptor(ioc, tcp::endpoint(asio::ip::make_address("127.0.0.1"), 0));
    return acceptor.local_endpoint().port();
}

HttpResult send_http_request(http::verb method,
                             unsigned short port,
                             const std::string& target,
                             const std::string& body = {})
{
    asio::io_context client_ioc;
    tcp::resolver resolver(client_ioc);
    beast::tcp_stream stream(client_ioc);
    const auto results = resolver.resolve("127.0.0.1", std::to_string(port));
    stream.connect(results);

    http::request<http::string_body> req{method, target, 11};
    req.set(http::field::host, "127.0.0.1");
    req.set(http::field::user_agent, "flow-pilot-functional-test");
    req.keep_alive(false);
    if (!body.empty()) {
        req.set(http::field::content_type, "application/json");
        req.body() = body;
        req.prepare_payload();
    }

    http::write(stream, req);

    beast::flat_buffer buffer;
    http::response<http::string_body> res;
    http::read(stream, buffer, res);

    beast::error_code ec;
    stream.socket().shutdown(tcp::socket::shutdown_both, ec);
    return {res.result(), res.body()};
}

json make_one_job_workflow(const std::string& client_id,
                           const std::string& request_id,
                           const std::string& workflow_id)
{
    json job;
    job["job_id"] = "job-1";
    job["type"] = "test";
    job["priority"] = 5;
    job["payload"] = json::array({1, 2, 3});

    json workflow;
    workflow["client_id"] = client_id;
    workflow["request_id"] = request_id;
    workflow["workflow_id"] = workflow_id;
    workflow["workflow_type"] = "functional-test";
    workflow["jobs"] = json::array({job});
    return workflow;
}

bool wait_for_server(unsigned short port)
{
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(3);
    while (std::chrono::steady_clock::now() < deadline) {
        try {
            (void)send_http_request(http::verb::get, port, "/not-found");
            return true;
        } catch (...) {
            std::this_thread::sleep_for(std::chrono::milliseconds(25));
        }
    }

    return false;
}

} // namespace

TEST(FlowPilotFunctionalTest, ClientSubmitsWorkflowAndPollsStatusUntilCompleted)
{
    Config::get().logger().output = LogOutput::CONSOLE_ONLY;
    Config::get().client_config().config_type = ClientDataConfig::ConfigManagerTypes::TEST_MANAGER;
    Config::get().redis().host = "127.0.0.1";
    Config::get().redis().port = 6379;
#ifdef WORKFLOW_SCHEMA_PATH
    Config::get().workflow().workflow_schema_path = WORKFLOW_SCHEMA_PATH;
#endif

    auto& ioc = flow_pilot::test::redis_ioc();
    ioc.restart();
    std::shared_ptr<RedisDatabaseAsync> redis;
    try {
        redis = RedisDatabaseAsync::init(ioc, Config::get().redis());
    } catch (const std::exception& ex) {
        GTEST_SKIP() << "Redis server is not available: " << ex.what();
    }
    {
        auto clear_queue = boost::asio::co_spawn(
            ioc,
            redis->clear_execution_queue_async(),
            boost::asio::use_future);
        ioc.run();
        clear_queue.get();
        ioc.restart();
    }

    const unsigned short port = reserve_free_port();
    Config::get().server().address = "127.0.0.1";
    Config::get().server().port = port;

    auto work_guard = asio::make_work_guard(ioc);
    run_http_server(ioc);
    std::thread io_thread([&ioc]() {
        ioc.run();
    });

    ASSERT_TRUE(wait_for_server(port));

    {
        CompletionHandler completion_handler;
        scheduler workflow_scheduler(1, 1);

        const std::string client_id = "functional-client-" + generate_unique_id();
        const std::string request_id = "functional-request-" + generate_unique_id();
        const std::string workflow_id = "functional-workflow-" + generate_unique_id();
        const json workflow = make_one_job_workflow(client_id, request_id, workflow_id);

        const auto submit = send_http_request(
            http::verb::post,
            port,
            "/api/v1/workflows",
            workflow.dump());
        ASSERT_EQ(submit.status, http::status::accepted) << submit.body;

        const std::string status_target =
            "/api/v1/workflows/" + workflow_id + "?client_id=" + client_id;

        json status_response;
        bool completed = false;
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(8);
        while (std::chrono::steady_clock::now() < deadline) {
            const auto status = send_http_request(http::verb::get, port, status_target);
            ASSERT_EQ(status.status, http::status::ok) << status.body;
            status_response = json::parse(status.body);

            if (status_response["status"] == "COMPLETED") {
                completed = true;
                break;
            }

            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }

        ASSERT_TRUE(completed) << status_response.dump();
        ASSERT_EQ(status_response["workflow_id"], workflow_id);
        ASSERT_EQ(status_response["client_id"], client_id);
        ASSERT_EQ(status_response["jobs"].size(), 1u);
        EXPECT_EQ(status_response["jobs"][0]["job_id"], "job-1");
        EXPECT_EQ(status_response["jobs"][0]["status"], "COMPLETED");
    }

    work_guard.reset();
    ioc.stop();
    if (io_thread.joinable()) {
        io_thread.join();
    }
}
