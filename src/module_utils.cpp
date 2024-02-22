#include "module_utils.h"
#include "logger.h"
#include "module_constants.h"
#include "module_data_handler.h"

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

void Print_Status(RedisModuleCtx *ctx , std::string optional){
    Data_Handler &d_h = Data_Handler::getInstance();
    
    std::string log_str = "\nSTATUS\n";
    log_str = log_str + "[" + optional + "]\n"; 
    log_str = log_str + "CLIENT_2_QUERY_MAP\n"; 
    for(auto &c2q : d_h.client_2_query){
        log_str = log_str + "Client : " +  c2q.first + "\n"; 
        for(auto &qId : c2q.second){
            log_str = log_str + "Query : " + std::to_string(qId.first) + " , ";
        }
        log_str = log_str + "\n";
    }
    log_str = log_str + "\n";
    log_str = log_str + "ID_2_QUERY_MAP\n"; 
    for(auto &i2q : d_h.id_2_query){
        log_str = log_str + "Query ID : " +  std::to_string(i2q.first) +  " , Query : " + i2q.second + "\n"; 
        log_str = log_str + "\n";
    }
    LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , log_str );
}
