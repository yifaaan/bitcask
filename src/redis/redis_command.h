#pragma once

#include <string>
#include <vector>

#include "resp/resp.h"
#include "types/redis_data_struct.h"

namespace bitcask::redis
{

    struct CommandResult
    {
        resp::Value reply;
        bool close_connection = false;
    };

    CommandResult ExecuteCommand(RedisDataStruct& redis, const std::vector<std::string>& args);

} // namespace bitcask::redis
