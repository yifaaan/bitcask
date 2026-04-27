#include "http_server.h"

#include <absl/status/status.h>
#include <httplib.h>

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <sstream>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "iterator.h"
#include "options.h"

namespace bitcask::http
{
    namespace
    {
        constexpr auto kJSONContentType = "application/json; charset=utf-8";

        std::string JSONEscape(std::string_view value)
        {
            std::string escaped;
            escaped.reserve(value.size() + 2);
            for (unsigned char ch : value)
            {
                switch (ch)
                {
                case '"':
                    escaped += "\\\"";
                    break;
                case '\\':
                    escaped += "\\\\";
                    break;
                case '\b':
                    escaped += "\\b";
                    break;
                case '\f':
                    escaped += "\\f";
                    break;
                case '\n':
                    escaped += "\\n";
                    break;
                case '\r':
                    escaped += "\\r";
                    break;
                case '\t':
                    escaped += "\\t";
                    break;
                default:
                    if (ch < 0x20)
                    {
                        constexpr char kHex[] = "0123456789abcdef";
                        escaped += "\\u00";
                        escaped += kHex[(ch >> 4) & 0x0F];
                        escaped += kHex[ch & 0x0F];
                    }
                    else
                    {
                        escaped.push_back(static_cast<char>(ch));
                    }
                    break;
                }
            }
            return escaped;
        }

        std::string JSONString(std::string_view value)
        {
            return "\"" + JSONEscape(value) + "\"";
        }

        void SetJSON(httplib::Response& response, int status, std::string body)
        {
            response.status = status;
            response.set_content(std::move(body), kJSONContentType);
        }

        std::string MessageBody(bool ok, std::string_view message)
        {
            return std::string("{\"ok\":") + (ok ? "true" : "false") + ",\"message\":" + JSONString(message) + "}";
        }

        int HTTPStatusFor(const absl::Status& status)
        {
            switch (status.code())
            {
            case absl::StatusCode::kInvalidArgument:
                return httplib::StatusCode::BadRequest_400;
            case absl::StatusCode::kNotFound:
                return httplib::StatusCode::NotFound_404;
            case absl::StatusCode::kAlreadyExists:
            case absl::StatusCode::kFailedPrecondition:
                return httplib::StatusCode::Conflict_409;
            case absl::StatusCode::kResourceExhausted:
                return httplib::StatusCode::InsufficientStorage_507;
            case absl::StatusCode::kUnauthenticated:
                return httplib::StatusCode::Unauthorized_401;
            case absl::StatusCode::kPermissionDenied:
                return httplib::StatusCode::Forbidden_403;
            case absl::StatusCode::kUnavailable:
                return httplib::StatusCode::ServiceUnavailable_503;
            default:
                return httplib::StatusCode::InternalServerError_500;
            }
        }

        void SetStatus(httplib::Response& response, const absl::Status& status)
        {
            if (status.ok())
            {
                SetJSON(response, httplib::StatusCode::OK_200, MessageBody(true, "ok"));
                return;
            }
            SetJSON(response, HTTPStatusFor(status), MessageBody(false, status.message()));
        }

        std::string KeyFromMatch(const httplib::Request& request)
        {
            if (request.matches.size() < 2)
            {
                return {};
            }
            return request.matches[1].str();
        }

        bool QueryBool(const httplib::Request& request, std::string_view name)
        {
            const auto key = std::string(name);
            if (!request.has_param(key))
            {
                return false;
            }
            const auto value = request.get_param_value(key);
            return value == "1" || value == "true" || value == "yes" || value == "on";
        }

        void AddCORSHeaders(httplib::Response& response)
        {
            response.set_header("Access-Control-Allow-Origin", "*");
            response.set_header("Access-Control-Allow-Methods", "GET, PUT, DELETE, POST, OPTIONS");
            response.set_header("Access-Control-Allow-Headers", "Content-Type");
        }

        std::string KeysBody(const std::vector<std::string>& keys)
        {
            std::string body = "{\"keys\":[";
            for (size_t i = 0; i < keys.size(); ++i)
            {
                if (i != 0)
                {
                    body += ',';
                }
                body += JSONString(keys[i]);
            }
            body += "]}";
            return body;
        }

        std::string EntriesBody(DB& db, const httplib::Request& request)
        {
            IteratorOptions options;
            std::string prefix;
            if (request.has_param("prefix"))
            {
                prefix = request.get_param_value("prefix");
                options.prefix = prefix;
            }
            options.reverse = QueryBool(request, "reverse");

            auto iterator = db.NewIterator(options);
            std::string body = "{\"entries\":[";
            bool first = true;
            for (iterator->Rewind(); iterator->Valid(); iterator->Next())
            {
                const auto value = iterator->Value();
                if (!value)
                {
                    continue;
                }

                if (!first)
                {
                    body += ',';
                }
                first = false;

                body += "{\"key\":";
                body += JSONString(iterator->Key());
                body += ",\"value\":";
                body += JSONString(*value);
                body += '}';
            }
            body += "]}";
            return body;
        }

        std::string StatBody(const Stat& stat)
        {
            std::ostringstream out;
            out << "{\"key_num\":" << stat.key_num << ",\"data_file_num\":" << stat.data_file_num
                << ",\"reclaimable_size\":" << stat.reclaimable_size << ",\"disk_size\":" << stat.disk_size << '}';
            return out.str();
        }

        std::filesystem::path BackupDestination(const httplib::Request& request)
        {
            if (request.has_param("dest"))
            {
                return request.get_param_value("dest");
            }
            if (request.has_param("path"))
            {
                return request.get_param_value("path");
            }
            return {};
        }
    } // namespace

    void RegisterRoutes(DB& db, httplib::Server& server)
    {
        server.set_pre_routing_handler([](const httplib::Request&, httplib::Response& response) {
            AddCORSHeaders(response);
            return httplib::Server::HandlerResponse::Unhandled;
        });

        server.Options(R"(/.*)", [](const httplib::Request&, httplib::Response& response) {
            AddCORSHeaders(response);
            response.status = httplib::StatusCode::NoContent_204;
        });

        server.Get("/v1/health", [](const httplib::Request&, httplib::Response& response) {
            SetJSON(response, httplib::StatusCode::OK_200, "{\"ok\":true}");
        });

        server.Put(R"(/v1/kv/(.*))", [&db](const httplib::Request& request, httplib::Response& response) {
            const auto key = KeyFromMatch(request);
            if (key.empty())
            {
                SetJSON(response, httplib::StatusCode::BadRequest_400, MessageBody(false, "key cannot be empty"));
                return;
            }

            SetStatus(response, db.Put(key, request.body));
        });

        server.Get(R"(/v1/kv/(.*))", [&db](const httplib::Request& request, httplib::Response& response) {
            const auto key = KeyFromMatch(request);
            if (key.empty())
            {
                SetJSON(response, httplib::StatusCode::BadRequest_400, MessageBody(false, "key cannot be empty"));
                return;
            }

            const auto value = db.Get(key);
            if (!value)
            {
                SetJSON(response, httplib::StatusCode::NotFound_404, MessageBody(false, "key not found"));
                return;
            }

            SetJSON(response, httplib::StatusCode::OK_200, "{\"key\":" + JSONString(key) + ",\"value\":" + JSONString(*value) + "}");
        });

        server.Delete(R"(/v1/kv/(.*))", [&db](const httplib::Request& request, httplib::Response& response) {
            const auto key = KeyFromMatch(request);
            if (key.empty())
            {
                SetJSON(response, httplib::StatusCode::BadRequest_400, MessageBody(false, "key cannot be empty"));
                return;
            }

            SetStatus(response, db.Delete(key));
        });

        server.Get("/v1/keys", [&db](const httplib::Request&, httplib::Response& response) {
            SetJSON(response, httplib::StatusCode::OK_200, KeysBody(db.ListKeys()));
        });

        server.Get("/v1/entries", [&db](const httplib::Request& request, httplib::Response& response) {
            SetJSON(response, httplib::StatusCode::OK_200, EntriesBody(db, request));
        });

        server.Get("/v1/stats", [&db](const httplib::Request&, httplib::Response& response) {
            SetJSON(response, httplib::StatusCode::OK_200, StatBody(db.Stat()));
        });

        server.Post("/v1/sync", [&db](const httplib::Request&, httplib::Response& response) {
            if (db.Sync())
            {
                SetJSON(response, httplib::StatusCode::OK_200, MessageBody(true, "ok"));
                return;
            }
            SetJSON(response, httplib::StatusCode::InternalServerError_500, MessageBody(false, "sync failed"));
        });

        server.Post("/v1/merge", [&db](const httplib::Request&, httplib::Response& response) {
            SetStatus(response, db.Merge());
        });

        server.Post("/v1/backup", [&db](const httplib::Request& request, httplib::Response& response) {
            const auto dest = BackupDestination(request);
            SetStatus(response, db.Backup(dest));
        });
    }

    bool Listen(DB& db, const ServerOptions& options)
    {
        httplib::Server server;
        RegisterRoutes(db, server);
        return server.listen(options.host, options.port);
    }
} // namespace bitcask::http
