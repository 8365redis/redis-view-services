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

    RedisModuleString *unregister_client = argv[1];
    std::string client_to_unregister = RedisModule_StringPtrLen( unregister_client, NULL);

    if(Unregister_Client(ctx, client_to_unregister) == REDISMODULE_ERR) {
        return RedisModule_ReplyWithError(ctx, strerror(errno));
    }
    
    RedisModule_ReplyWithSimpleString(ctx, "OK");
    return REDISMODULE_OK;
}

int Unregister_Client(RedisModuleCtx *ctx, std::string client_to_unregister) {
    Data_Handler &d_h = Data_Handler::getInstance();
    LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Unregister_Client called for client : " +  client_to_unregister);

    if ( !client_to_unregister.empty() ) {
        if ( d_h.registered_clients.count(client_to_unregister) == 0) {
            LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Unregister_Client client is not registered." );
            return REDISMODULE_ERR;
        } 
    } else {
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Unregister_Client failed parse client name." );
        return REDISMODULE_ERR;
    }

    d_h.registered_clients.erase(client_to_unregister);
    d_h.unreg_wait_list.insert(client_to_unregister);
    d_h.client_2_last_seen.erase(client_to_unregister);

    LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Unregister_Client is successful for client : " +  client_to_unregister);
    return REDISMODULE_OK;    
}