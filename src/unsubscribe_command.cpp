#include <string>

#include "unsubscribe_command.h"
#include "logger.h"
#include "module_data_handler.h"


int UnSubscribe_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "UnSubscribe_RedisCommand called." );
    
    if (argc != 2) {
        return RedisModule_WrongArity(ctx);
    }

    Data_Handler &d_h = Data_Handler::getInstance();

    RedisModuleString *unsubscribe_id = argv[1];
    long long id_2_unsubscribe = -1;
    if (RedisModule_StringToLongLong( unsubscribe_id, &id_2_unsubscribe) == REDISMODULE_OK) {
        if(id_2_unsubscribe < 0 || id_2_unsubscribe > LAST_VIEW_SEARCH_IDENTIFIER) {
            LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "UnSubscribe_RedisCommand given ID is invalid." );
            return RedisModule_ReplyWithError(ctx, "Given id must be bigger than -1 and smaller than latest query id");
        } else {
            d_h.unsub_wait_list.insert(id_2_unsubscribe);
            std::string id_str = std::to_string(id_2_unsubscribe);
            std::string ok_msg = "OK " + id_str + " unsubscribed";
            RedisModule_ReplyWithSimpleString(ctx, ok_msg.c_str());
            return REDISMODULE_OK;
        }
    } else {
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "UnSubscribe_RedisCommand failed parse ID." );
        return RedisModule_ReplyWithError(ctx, strerror(errno));
    }
}