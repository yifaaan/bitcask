#pragma once

#include <cstddef>
#include <memory>
#include <string>

#include "db.h"

namespace bitcask::redis
{

    struct ServerOptions
    {
        std::string host = "127.0.0.1";
        int port = 6379;
        int backlog = 128;
        size_t max_buffer_size = 1024 * 1024;
    };

    class Server
    {
    public:
        Server(DB& db, ServerOptions options = {});
        ~Server();

        Server(const Server&) = delete;
        Server& operator=(const Server&) = delete;
        Server(Server&&) = delete;
        Server& operator=(Server&&) = delete;

        // Open the listening socket without entering the event loop.
        bool Bind();
        // Run the Asio event loop on the current thread until Stop is called.
        bool Serve();
        // Close the acceptor and ask the event loop to exit.
        void Stop();

        // Returns the actual port after Bind, useful when options.port is 0.
        int Port() const;

    private:
        class Impl;

        std::unique_ptr<Impl> impl_;
    };

    bool Listen(DB& db, const ServerOptions& options);

} // namespace bitcask::redis
