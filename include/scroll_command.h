#ifndef SCROLL_COMMAND_H
#define SCROLL_COMMAND_H

#include "redismodule.h"

int Scroll_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);

#endif /* SCROLL_COMMAND_H */