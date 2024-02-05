#include "unsubscribe_command.h"

int UnSubscribe_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    
    if (argc != 2) {
        return RedisModule_WrongArity(ctx);
    }

    RedisModuleString *unsubscribe_id = argv[1];
    long long id_2_unsubscribe = -1;
    if (RedisModule_StringToLongLong(ctx,argv[1],&myval) == REDISMODULE_OK) {
        if(id_2_unsubscribe < 0 || id_2_unsubscribe > LAST_VIEW_SEARCH_IDENTIFIER) {
            LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "UnSubscribe_RedisCommand given ID is invalid." );
            return RedisModule_ReplyWithError(ctx, "Given id must be bigger than -1 and smaller than latest query id");
        } else {
            UNSUBSCRIBE_WAITING_LIST.insert(id_2_unsubscribe);
            std::string id_str = std::to_string(id_2_unsubscribe);
            std::string ok_msg = "OK " + id_str + " unsubscribed";
            RedisModule_ReplyWithSimpleString(ctx, ok_msg.c_str());
        }
    } else {
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "UnSubscribe_RedisCommand failed parse ID." );
        return RedisModule_ReplyWithError(ctx, strerror(errno));
    }
}