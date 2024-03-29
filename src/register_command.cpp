#include "register_command.h"
#include "logger.h"
#include "module_data_handler.h"

int Register_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);

    Data_Handler &d_h = Data_Handler::getInstance();

    LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "Register_RedisCommand called." );
    if (argc < 2  || argc > 3) {
        return RedisModule_WrongArity(ctx);
    }

    // Get Client ID
    RedisModuleString *client_name = argv[1];
    std::string client_name_str = RedisModule_StringPtrLen(client_name, NULL);
    unsigned long long client_id = RedisModule_GetClientId(ctx);

    if(d_h.registered_clients.count(client_name_str) > 0 ) {
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Register_RedisCommand failed , client is already registered." );
        return RedisModule_ReplyWithError(ctx, strerror(errno));       
    }
    
    // Set client name
    if (RedisModule_SetClientNameById(client_id, client_name) != REDISMODULE_OK){
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Register_RedisCommand failed to set client name." );
        return RedisModule_ReplyWithError(ctx, strerror(errno));
    }
    
    // Check if the stream exists and delete if it is
    if( RedisModule_KeyExists(ctx, client_name) ) { // NOT checking if it is stream
        RedisModuleKey *stream_key = RedisModule_OpenKey(ctx, client_name, REDISMODULE_WRITE);
        if (RedisModule_DeleteKey(stream_key) != REDISMODULE_OK ) {
            LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Register_RedisCommand failed to delete the stream." );
            return RedisModule_ReplyWithError(ctx, strerror(errno));
        }
    }

    // Create a new stream
    RedisModuleKey *stream_key = RedisModule_OpenKey(ctx, client_name, REDISMODULE_WRITE);
    RedisModuleString **xadd_params = (RedisModuleString **) RedisModule_Alloc(sizeof(RedisModuleString *) * 2);
    const char *dummy = "d";
    xadd_params[0] = RedisModule_CreateString(ctx, dummy, strlen(dummy));
    xadd_params[1] = RedisModule_CreateString(ctx, dummy, strlen(dummy));
    int stream_add_resp = RedisModule_StreamAdd( stream_key, REDISMODULE_STREAM_ADD_AUTOID, NULL, xadd_params, 1);
    if (stream_add_resp != REDISMODULE_OK) {
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Register_RedisCommand failed to create the stream." );
        return RedisModule_ReplyWithError(ctx, strerror(errno));
    }
    RedisModule_StreamTrimByLength(stream_key, 0, 0);  // Clear the stream
    RedisModule_ReplyWithSimpleString(ctx, "OK");

    //UPDATE GLOBAL REGISTERED
    d_h.registered_clients.insert(client_name_str);
    //UPDATE GLOBAL LAST SEEN
    auto now = std::chrono::system_clock::now();
    auto ms  = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch());
    unsigned long long ms_value = ms.count();
    d_h.client_2_last_seen[client_name_str] = ms_value;

    LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "Register_RedisCommand succesfull for client id: " +  std::to_string(client_id) + " , client name : " + client_name_str + " , last seen : " + std::to_string(ms_value));

    return REDISMODULE_OK;
}