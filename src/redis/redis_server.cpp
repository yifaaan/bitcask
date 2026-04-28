#include "redis/redis_server.h"

#include <absl/strings/string_view.h>

#include <array>
#include <memory>
#include <string>
#include <system_error>
#include <utility>

#ifndef ASIO_STANDALONE
#define ASIO_STANDALONE
#endif
#include <asio.hpp>

#include "redis/redis_command.h"
#include "resp/resp.h"
#include "types/redis_data_struct.h"

namespace bitcask::redis
{
    namespace
    {
        using asio::ip::tcp;

        // One Session owns one client connection. The shared_ptr lifetime is
        // extended by async callbacks until the connection closes.
        class Session : public std::enable_shared_from_this<Session>
        {
        public:
            Session(tcp::socket socket, DB& db, size_t max_buffer_size)
                : socket_(std::move(socket)), redis_(&db), max_buffer_size_(max_buffer_size)
            {
            }

            void Start()
            {
                Read();
            }

        private:
            // Read whatever bytes are currently available. RESP framing is
            // handled by StreamParser, so TCP packet boundaries do not matter.
            void Read()
            {
                auto self = shared_from_this();
                socket_.async_read_some(
                    asio::buffer(read_buffer_),
                    [self](const std::error_code& error, size_t bytes_transferred) {
                        if (error)
                        {
                            return;
                        }
                        self->parser_.Append(absl::string_view(self->read_buffer_.data(), bytes_transferred));
                        if (self->parser_.BufferedBytes() > self->max_buffer_size_)
                        {
                            self->WriteAndClose(resp::Err("ERR protocol error: request is too large"));
                            return;
                        }
                        self->DrainRequests();
                    });
            }

            // Parse all complete Redis command frames currently buffered. This
            // supports pipelining by collecting multiple replies before writing.
            void DrainRequests()
            {
                while (true)
                {
                    auto request = parser_.Next();
                    if (!request.ok())
                    {
                        WriteAndClose(resp::Err("ERR protocol error: " + std::string(request.status().message())));
                        return;
                    }
                    if (!request->has_value())
                    {
                        Write();
                        return;
                    }

                    auto result = ExecuteCommand(redis_, **request);
                    pending_output_ += resp::Serialize(result.reply);
                    close_after_write_ = close_after_write_ || result.close_connection;
                    if (close_after_write_)
                    {
                        Write();
                        return;
                    }
                }
            }

            // Used for protocol errors and QUIT-like flows: send the final
            // response first, then close in the write completion handler.
            void WriteAndClose(const resp::Value& value)
            {
                pending_output_ += resp::Serialize(value);
                close_after_write_ = true;
                Write();
            }

            // Keep output alive with a shared string until async_write finishes.
            // After a normal write, the session goes back to reading.
            void Write()
            {
                if (pending_output_.empty())
                {
                    if (close_after_write_)
                    {
                        Close();
                        return;
                    }
                    Read();
                    return;
                }

                auto output = std::make_shared<std::string>(std::move(pending_output_));
                pending_output_.clear();

                auto self = shared_from_this();
                asio::async_write(
                    socket_,
                    asio::buffer(*output),
                    [self, output](const std::error_code& error, size_t) {
                        if (error)
                        {
                            self->Close();
                            return;
                        }
                        if (self->close_after_write_)
                        {
                            self->Close();
                            return;
                        }
                        self->Read();
                    });
            }

            void Close()
            {
                std::error_code ignored;
                socket_.shutdown(tcp::socket::shutdown_both, ignored);
                socket_.close(ignored);
            }

            tcp::socket socket_;
            RedisDataStruct redis_;
            resp::StreamParser parser_;
            std::array<char, 4096> read_buffer_{};
            std::string pending_output_;
            size_t max_buffer_size_ = 0;
            bool close_after_write_ = false;
        };
    } // namespace

    // Hide Asio types from the public header and keep rebuild boundaries small.
    class Server::Impl
    {
    public:
        Impl(DB& db, ServerOptions options)
            : db_(db),
              options_(std::move(options)),
              acceptor_(io_context_)
        {
        }

        bool Bind()
        {
            if (acceptor_.is_open())
            {
                return true;
            }

            std::error_code error;
            if (options_.host.empty())
            {
                return BindEndpoint(tcp::endpoint(tcp::v4(), static_cast<unsigned short>(options_.port)));
            }

            tcp::resolver resolver(io_context_);
            const auto port = std::to_string(options_.port);
            auto endpoints = resolver.resolve(options_.host, port, error);
            if (error)
            {
                return false;
            }

            for (const auto& endpoint : endpoints)
            {
                if (BindEndpoint(endpoint.endpoint()))
                {
                    return true;
                }
            }

            return false;
        }

        bool Serve()
        {
            if (!Bind())
            {
                return false;
            }

            Accept();
            io_context_.run();
            return true;
        }

        void Stop()
        {
            std::error_code ignored;
            acceptor_.cancel(ignored);
            acceptor_.close(ignored);
            io_context_.stop();
        }

        int Port() const
        {
            return bound_port_;
        }

    private:
        // Bind one resolved endpoint. The caller can try several endpoints
        // until one succeeds, which covers IPv4/IPv6 resolver results.
        bool BindEndpoint(const tcp::endpoint& endpoint)
        {
            std::error_code error;
            tcp::acceptor candidate(io_context_);
            candidate.open(endpoint.protocol(), error);
            if (error)
            {
                return false;
            }

            candidate.set_option(asio::socket_base::reuse_address(true), error);
            if (error)
            {
                return false;
            }

            candidate.bind(endpoint, error);
            if (error)
            {
                return false;
            }

            candidate.listen(options_.backlog, error);
            if (error)
            {
                return false;
            }

            acceptor_ = std::move(candidate);
            bound_port_ = acceptor_.local_endpoint(error).port();
            if (error)
            {
                bound_port_ = options_.port;
            }
            return true;
        }

        // Start the accept loop. Each successful accept creates an independent
        // Session; the loop is re-armed until Stop closes the acceptor.
        void Accept()
        {
            acceptor_.async_accept([this](const std::error_code& error, tcp::socket socket) {
                if (!acceptor_.is_open())
                {
                    return;
                }

                if (!error)
                {
                    std::make_shared<Session>(std::move(socket), db_, options_.max_buffer_size)->Start();
                }

                Accept();
            });
        }

        DB& db_;
        ServerOptions options_;
        asio::io_context io_context_;
        tcp::acceptor acceptor_;
        int bound_port_ = 0;
    };

    Server::Server(DB& db, ServerOptions options) : impl_(std::make_unique<Impl>(db, std::move(options))) {}

    Server::~Server()
    {
        Stop();
    }

    bool Server::Bind()
    {
        return impl_->Bind();
    }

    bool Server::Serve()
    {
        return impl_->Serve();
    }

    void Server::Stop()
    {
        impl_->Stop();
    }

    int Server::Port() const
    {
        return impl_->Port();
    }

    bool Listen(DB& db, const ServerOptions& options)
    {
        Server server(db, options);
        return server.Serve();
    }

} // namespace bitcask::redis
