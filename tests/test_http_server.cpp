#include <catch2/catch_test_macros.hpp>

#include <chrono>
#include <filesystem>
#include <memory>
#include <string>
#include <string_view>
#include <thread>

#include <httplib.h>

#include "db.h"
#include "http_server.h"

namespace
{
    std::filesystem::path MakeTestDir()
    {
        const auto suffix = std::chrono::steady_clock::now().time_since_epoch().count();
        return std::filesystem::temp_directory_path() / ("bitcask_http_test_" + std::to_string(suffix));
    }

    class HTTPFixture
    {
    public:
        HTTPFixture()
            : test_dir_(MakeTestDir())
        {
            std::filesystem::remove_all(test_dir_);
            std::filesystem::create_directories(test_dir_);

            db_ = bitcask::DB::Open(bitcask::Options{.data_dir = test_dir_});
            REQUIRE(db_ != nullptr);

            bitcask::http::RegisterRoutes(*db_, server_);
            port_ = server_.bind_to_any_port("127.0.0.1");
            REQUIRE(port_ > 0);

            server_thread_ = std::thread([this]() {
                server_.listen_after_bind();
            });
            server_.wait_until_ready();
        }

        ~HTTPFixture()
        {
            server_.stop();
            if (server_thread_.joinable())
            {
                server_thread_.join();
            }
            if (db_)
            {
                db_->Close();
            }
            std::filesystem::remove_all(test_dir_);
        }

        std::unique_ptr<httplib::Client> NewClient() const
        {
            return std::make_unique<httplib::Client>("127.0.0.1", port_);
        }

        std::string KeyPath(std::string_view key) const
        {
            return "/v1/kv/" + httplib::encode_path_component(std::string(key));
        }

        const std::filesystem::path& TestDir() const { return test_dir_; }

    private:
        std::filesystem::path test_dir_;
        std::unique_ptr<bitcask::DB> db_;
        httplib::Server server_;
        int port_ = 0;
        std::thread server_thread_;
    };
} // namespace

TEST_CASE_METHOD(HTTPFixture, "HTTP key value operations", "[http]")
{
    auto client = NewClient();

    auto put = client->Put(KeyPath("user:1"), "alice", "text/plain");
    REQUIRE(put);
    REQUIRE(put->status == httplib::StatusCode::OK_200);
    REQUIRE(put->body == R"({"ok":true,"message":"ok"})");

    auto get = client->Get(KeyPath("user:1"));
    REQUIRE(get);
    REQUIRE(get->status == httplib::StatusCode::OK_200);
    REQUIRE(get->body == R"({"key":"user:1","value":"alice"})");

    auto missing = client->Get(KeyPath("missing"));
    REQUIRE(missing);
    REQUIRE(missing->status == httplib::StatusCode::NotFound_404);

    auto del = client->Delete(KeyPath("user:1"));
    REQUIRE(del);
    REQUIRE(del->status == httplib::StatusCode::OK_200);

    auto deleted = client->Get(KeyPath("user:1"));
    REQUIRE(deleted);
    REQUIRE(deleted->status == httplib::StatusCode::NotFound_404);
}

TEST_CASE_METHOD(HTTPFixture, "HTTP list entries and stats", "[http]")
{
    auto client = NewClient();

    REQUIRE(client->Put(KeyPath("user:1"), "alice", "text/plain")->status == httplib::StatusCode::OK_200);
    REQUIRE(client->Put(KeyPath("user:2"), "bob", "text/plain")->status == httplib::StatusCode::OK_200);
    REQUIRE(client->Put(KeyPath("admin:1"), "root", "text/plain")->status == httplib::StatusCode::OK_200);

    auto keys = client->Get("/v1/keys");
    REQUIRE(keys);
    REQUIRE(keys->status == httplib::StatusCode::OK_200);
    REQUIRE(keys->body == R"({"keys":["admin:1","user:1","user:2"]})");

    auto entries = client->Get("/v1/entries?prefix=user%3A&reverse=true");
    REQUIRE(entries);
    REQUIRE(entries->status == httplib::StatusCode::OK_200);
    REQUIRE(entries->body == R"({"entries":[{"key":"user:2","value":"bob"},{"key":"user:1","value":"alice"}]})");

    auto stats = client->Get("/v1/stats");
    REQUIRE(stats);
    REQUIRE(stats->status == httplib::StatusCode::OK_200);
    REQUIRE(stats->body.find(R"("key_num":3)") != std::string::npos);
    REQUIRE(stats->body.find(R"("data_file_num":1)") != std::string::npos);
}

TEST_CASE_METHOD(HTTPFixture, "HTTP maintenance operations", "[http]")
{
    auto client = NewClient();

    REQUIRE(client->Put(KeyPath("key"), "value", "text/plain")->status == httplib::StatusCode::OK_200);

    auto sync = client->Post("/v1/sync", "", "text/plain");
    REQUIRE(sync);
    REQUIRE(sync->status == httplib::StatusCode::OK_200);

    const auto backup_dir = TestDir() / "backup";
    const auto backup_path = "/v1/backup?dest=" + httplib::encode_query_component(backup_dir.string());
    auto backup = client->Post(backup_path, "", "text/plain");
    REQUIRE(backup);
    REQUIRE(backup->status == httplib::StatusCode::OK_200);
    REQUIRE(std::filesystem::exists(backup_dir / "000000000.data"));
}

TEST_CASE_METHOD(HTTPFixture, "HTTP rejects empty keys", "[http]")
{
    auto client = NewClient();

    auto put = client->Put("/v1/kv/", "value", "text/plain");
    REQUIRE(put);
    REQUIRE(put->status == httplib::StatusCode::BadRequest_400);
}
