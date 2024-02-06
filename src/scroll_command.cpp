#include "scroll_command.h"
#include "module_constants.h"
#include "logger.h"
#include <tuple>


int Scroll_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc != 4) {
        return RedisModule_WrongArity(ctx);
    }

    RedisModuleString *query_id_str = argv[1];
    long long query_id = -1;

    RedisModuleString *offset_str = argv[2];
    long long offset = -1;

    RedisModuleString *limit_str = argv[3];
    long long limit = -1;

    if ( RedisModule_StringToLongLong( query_id_str, &query_id) == REDISMODULE_OK && \
        RedisModule_StringToLongLong( offset_str, &offset) == REDISMODULE_OK &&  \
        RedisModule_StringToLongLong( limit_str, &limit) == REDISMODULE_OK) {
        if (query_id < 0 || offset < 0 || limit < 0) {
            LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Scroll_RedisCommand given params is invalid." );
            return RedisModule_ReplyWithError(ctx, "Given params must be bigger than zero");            
        }
        SCROLL_WAITING_LIST.push_back(std::make_tuple(query_id, offset, limit));

        std::string ok_msg = "OK " +  std::to_string(query_id) + " " + std::to_string(offset) + " " + std::to_string(limit);
        RedisModule_ReplyWithSimpleString(ctx, ok_msg.c_str());
        return REDISMODULE_OK;

    } else {
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Scroll_RedisCommand failed parse params." );
        return RedisModule_ReplyWithError(ctx, strerror(errno));        
    }
}