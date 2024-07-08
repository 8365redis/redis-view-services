#ifndef UNSUBSCRIBE_COUNT_COMMAND_H
#define UNSUBSCRIBE_COUNT_COMMAND_H

#include "redismodule.h"

int UnSubscribe_Count_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);

#endif /* UNSUBSCRIBE_COUNT_COMMAND_H */