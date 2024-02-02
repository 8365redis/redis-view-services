#include "module_utils.h"
#include "logger.h"

std::string Get_Client_Name(RedisModuleCtx *ctx) {
    unsigned long long client_id = RedisModule_GetClientId(ctx);
    return Get_Client_Name_From_ID(ctx, client_id);
}

std::string Get_Client_Name_From_ID(RedisModuleCtx *ctx, unsigned long long client_id) {
    RedisModuleString *client_name = RedisModule_GetClientNameById(ctx, client_id); 
    if ( client_name == NULL){
        LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "Get_Client_Name_From_ID : Failed to get client name." );
        return "";
    }
    std::string client_name_str = RedisModule_StringPtrLen(client_name, NULL);
    if ( client_name_str.empty()){
        LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "Get_Client_Name_From_ID : Failed to get client name because client name is not set." );
        return "";
    }
    return client_name_str;
}
