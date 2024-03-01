#include <string>

#include "unregister_command.h"
#include "logger.h"
#include "module_data_handler.h"


int UnRegister_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "UnRegister_RedisCommand called." );
    
    if (argc != 2) {
        return RedisModule_WrongArity(ctx);
    }

    Data_Handler &d_h = Data_Handler::getInstance();

    RedisModuleString *unregister_client = argv[1];

    std::string client_to_unregister = RedisModule_StringPtrLen( unregister_client, NULL);

    if ( client_to_unregister.empty() ) {
        if ( d_h.registered_clients.count(client_to_unregister) == 0) {
            LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "UnRegister_RedisCommand client is not registered." );
            return RedisModule_ReplyWithError(ctx, strerror(errno));               
        } 
        d_h.registered_clients.erase(client_to_unregister);
        d_h.unreg_wait_list.insert(client_to_unregister);
    } else {
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "UnRegister_RedisCommand failed parse client name." );
        return RedisModule_ReplyWithError(ctx, strerror(errno));
    }


}