#ifndef TEST_COMMAND_H
#define TEST_COMMAND_H

#include "redis/redismodule.h"

int Test_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);

#endif /* TEST_COMMAND_H */
