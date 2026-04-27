#pragma once

#include <string>

#include "db.h"

namespace httplib
{
    class Server;
}

namespace bitcask::http
{
    struct ServerOptions
    {
        std::string host = "127.0.0.1";
        int port = 8080;
    };

    void RegisterRoutes(DB& db, httplib::Server& server);
    bool Listen(DB& db, const ServerOptions& options);
} // namespace bitcask::http
