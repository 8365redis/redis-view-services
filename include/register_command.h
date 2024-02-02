#ifndef REGISTER_COMMAND_H
#define REGISTER_COMMAND_H

#include "redismodule.h"

int Register_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);

#endif /* REGISTER_COMMAND_H */