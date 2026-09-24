#include <gtest/gtest.h>

#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/beast/core/flat_buffer.hpp>
#include <boost/beast/http.hpp>
#include <boost/beast/core/tcp_stream.hpp>
#include <chrono>
#include <csignal>
#include <cstdlib>
#include <nlohmann/json.hpp>
#include <string>
#include <sys/types.h>
#include <sys/wait.h>
#include <thread>
#include <unistd.h>
#include <vector>

namespace asio = boost::asio;
namespace beast = boost::beast;
namespace http = beast::http;
using tcp = asio::ip::tcp;
using json = nlohmann::json;

namespace {

unsigned short reserve_free_port()
{
    asio::io_context ioc;
    tcp::acceptor acceptor(ioc, tcp::endpoint(asio::ip::make_address("127.0.0.1"), 0));
    return acceptor.local_endpoint().port();
}

bool can_connect(unsigned short port)
{
    try {
        asio::io_context ioc;
        tcp::socket socket(ioc);
        socket.connect(tcp::endpoint(asio::ip::make_address("127.0.0.1"), port));
        return true;
    } catch (...) {
        return false;
    }
}

bool wait_for_server(unsigned short port, std::chrono::seconds timeout)
{
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        if (can_connect(port)) {
            return true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(25));
    }
    return false;
}

bool wait_for_exit(pid_t pid, std::chrono::seconds timeout, int& status)
{
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        const pid_t result = waitpid(pid, &status, WNOHANG);
        if (result == pid) {
            return true;
        }
        if (result == -1) {
            return false;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(25));
    }
    return false;
}

bool process_exited(pid_t pid, int& status)
{
    const pid_t result = waitpid(pid, &status, WNOHANG);
    return result == pid;
}


class ChildProcessGuard {
public:
    explicit ChildProcessGuard(pid_t pid)
        : pid_(pid)
    {
    }

    ChildProcessGuard(const ChildProcessGuard&) = delete;
    ChildProcessGuard& operator=(const ChildProcessGuard&) = delete;

    ~ChildProcessGuard()
    {
        cleanup();
    }

    void release()
    {
        pid_ = -1;
    }

    void cleanup()
    {
        if (pid_ <= 0) {
            return;
        }

        int status = 0;
        if (process_exited(pid_, status)) {
            pid_ = -1;
            return;
        }

        kill(pid_, SIGKILL);
        waitpid(pid_, &status, 0);
        pid_ = -1;
    }

private:
    pid_t pid_{-1};
};

http::response<http::string_body> send_get_request(unsigned short port, const std::string& target)
{
    asio::io_context ioc;
    tcp::resolver resolver(ioc);
    beast::tcp_stream stream(ioc);
    const auto results = resolver.resolve("127.0.0.1", std::to_string(port));
    stream.connect(results);

    http::request<http::string_body> req{http::verb::get, target, 11};
    req.set(http::field::host, "127.0.0.1");
    req.set(http::field::user_agent, "flow_pilot_tests");
    req.keep_alive(false);
    http::write(stream, req);

    beast::flat_buffer buffer;
    http::response<http::string_body> res;
    http::read(stream, buffer, res);
    return res;
}

void terminate_and_expect_clean_exit(pid_t pid)
{
    ASSERT_EQ(kill(pid, SIGTERM), 0) << "failed to signal flow_pilot";

    int status = 0;
    if (!wait_for_exit(pid, std::chrono::seconds(5), status)) {
        kill(pid, SIGKILL);
        waitpid(pid, &status, 0);
        FAIL() << "flow_pilot did not exit after SIGTERM; a joined thread is likely still running";
    }

    ASSERT_TRUE(WIFEXITED(status)) << "flow_pilot did not exit normally";
    EXPECT_EQ(WEXITSTATUS(status), EXIT_SUCCESS);
}

pid_t start_flow_pilot(unsigned short port, const std::vector<std::string>& extra_args)
{
#ifndef FLOW_PILOT_EXECUTABLE
    (void)port;
    (void)extra_args;
    return -1;
#else
    const std::string port_arg = std::to_string(port);
    const char* executable = FLOW_PILOT_EXECUTABLE;

    std::vector<std::string> args{
        executable,
        "--port",
        port_arg,
        "--redis-host",
        "127.0.0.1",
        "--redis-port",
        "6379"
    };
    args.insert(args.end(), extra_args.begin(), extra_args.end());

    const pid_t pid = fork();
    EXPECT_GE(pid, 0) << "fork failed";

    if (pid == 0) {
        std::vector<char*> argv;
        argv.reserve(args.size() + 1);
        for (auto& arg : args) {
            argv.push_back(arg.data());
        }
        argv.push_back(nullptr);
        execv(executable, argv.data());
        _exit(127);
    }

    return pid;
#endif
}

void expect_flow_pilot_exits_after_sigterm(const std::vector<std::string>& extra_args)
{
#ifndef FLOW_PILOT_EXECUTABLE
    GTEST_SKIP() << "FLOW_PILOT_EXECUTABLE is not configured";
#else
    const unsigned short port = reserve_free_port();
    const pid_t pid = start_flow_pilot(port, extra_args);
    ASSERT_GT(pid, 0);
    ChildProcessGuard child_guard(pid);

    if (!wait_for_server(port, std::chrono::seconds(5))) {
        int status = 0;
        if (process_exited(pid, status)) {
            child_guard.release();
            GTEST_SKIP() << "flow_pilot exited before opening the HTTP port; Redis may be unavailable";
        }

        child_guard.cleanup();
        FAIL() << "flow_pilot did not open the HTTP port before the startup timeout";
    }

    terminate_and_expect_clean_exit(pid);
    child_guard.release();
#endif
}

} // namespace

TEST(FlowPilotShutdownTest, SigtermExitsProcessCleanly)
{
    expect_flow_pilot_exits_after_sigterm({});
}

TEST(FlowPilotShutdownTest, SigtermExitsProcessCleanlyWithConfiguredThreadCounts)
{
    expect_flow_pilot_exits_after_sigterm({
        "--redis-io-threads", "2",
        "--schedulers", "2",
        "--completion-handlers", "2"
    });
}

TEST(FlowPilotShutdownTest, RuntimeStatusReportsConfiguredComponents)
{
#ifndef FLOW_PILOT_EXECUTABLE
    GTEST_SKIP() << "FLOW_PILOT_EXECUTABLE is not configured";
#else
    const unsigned short port = reserve_free_port();
    const pid_t pid = start_flow_pilot(port, {
        "--redis-io-threads", "2",
        "--schedulers", "2",
        "--completion-handlers", "2"
    });
    ASSERT_GT(pid, 0);
    ChildProcessGuard child_guard(pid);

    if (!wait_for_server(port, std::chrono::seconds(5))) {
        int status = 0;
        if (process_exited(pid, status)) {
            child_guard.release();
            GTEST_SKIP() << "flow_pilot exited before opening the HTTP port; Redis may be unavailable";
        }

        child_guard.cleanup();
        FAIL() << "flow_pilot did not open the HTTP port before the startup timeout";
    }

    const auto response = send_get_request(port, "/api/v1/runtime/status");
    EXPECT_EQ(response.result(), http::status::ok);

    const auto body = json::parse(response.body());
    EXPECT_EQ(body.at("status"), "running");
    EXPECT_EQ(body.at("started"), true);
    EXPECT_EQ(body.at("shutting_down"), false);
    EXPECT_EQ(body.at("redis_io_threads"), 2);
    EXPECT_EQ(body.at("scheduler_count"), 2);
    EXPECT_EQ(body.at("completion_handler_count"), 2);

    terminate_and_expect_clean_exit(pid);
    child_guard.release();
#endif
}
