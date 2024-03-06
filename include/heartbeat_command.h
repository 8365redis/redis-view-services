#ifndef HEARTBEAT_COMMAND_H
#define HEARTBEAT_COMMAND_H

#include <string>
#include "redismodule.h"

int Heartbeat_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
int Trim_From_Stream(RedisModuleCtx *ctx, RedisModuleString *last_read_id, std::string client_name);

#endif /* HEARTBEAT_COMMAND_H */