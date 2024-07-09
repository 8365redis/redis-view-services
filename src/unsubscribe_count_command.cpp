#include <string>

#include "unsubscribe_count_command.h"
#include "logger.h"
#include "module_data_handler.h"
#include "module_utils.h"
#include "logger.h"


int UnSubscribe_Count_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "UnSubscribe_Count_RedisCommand called." );

    if (argc != 2) {
        return RedisModule_WrongArity(ctx);
    }

    Data_Handler &d_h = Data_Handler::getInstance();

    RedisModuleString *unsubscribe_id = argv[1];
    unsigned long long id_2_unsubscribe = -1;
    if (RedisModule_StringToULongLong( unsubscribe_id, &id_2_unsubscribe) == REDISMODULE_OK) {
        if(id_2_unsubscribe < 0 ) {
            LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "UnSubscribe_Count_RedisCommand given ID is invalid." );
            return RedisModule_ReplyWithError(ctx, "Given id must be bigger than -1 and smaller than latest query id");
        } 
    } else {
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "UnSubscribe_Count_RedisCommand failed parse ID." );
        return RedisModule_ReplyWithError(ctx, strerror(errno));
    }

    std::vector<std::string> arguments_arg;
    std::string client_name = Get_Client_Name(ctx);

    Client_Size_Info info_to_unsub(client_name, arguments_arg, -1, id_2_unsubscribe); // arguments_arg and count is not relevant

    if(d_h.client_size_info.count(client_name) == 0){
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "UnSubscribe_Count_RedisCommand Client doesnt have any count operation: " + client_name);
        std::string err_msg = "Client doesnt have count operation";
        RedisModule_ReplyWithSimpleString(ctx, err_msg.c_str());
        return REDISMODULE_ERR; 
    } else {
        bool matching_count_found = false;
        for(const auto& clients_count : d_h.client_size_info[client_name]){
            if (clients_count.first == id_2_unsubscribe){
                matching_count_found = true;
                break;
            }
        }
        if(matching_count_found == false) {
            LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "UnSubscribe_Count_RedisCommand Client doesnt have any matching count operation : " + client_name);
            std::string err_msg = "Client doesnt have matching count operation";
            RedisModule_ReplyWithSimpleString(ctx, err_msg.c_str());
            return REDISMODULE_ERR;      
        }
    }

    d_h.unsub_count_wait_list.push_back(info_to_unsub);

    std::string ok_msg = "OK " + std::to_string(id_2_unsubscribe) + " unsubscribed";
    RedisModule_ReplyWithSimpleString(ctx, ok_msg.c_str());
    return REDISMODULE_OK;    
    
}