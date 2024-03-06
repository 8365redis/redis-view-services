#include <thread>

#include "client_tracker.h"
#include "logger.h"
#include "module_data_handler.h"
#include "module_utils.h"
#include "unregister_command.h"

void Handle_Client_Event(RedisModuleCtx *ctx, RedisModuleEvent eid, uint64_t subevent, void *data) {
    if (data == NULL) {
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Handle_Client_Event failed with NULL data." );
        return;
    }
    RedisModuleClientInfo *client_info = (RedisModuleClientInfo*)data;
    unsigned long long client_id = client_info->id;
    LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "Handle_Client_Event client : " + std::to_string(client_id) );
    std::string client_name = Get_Client_Name_From_ID(ctx, client_id);
    Data_Handler &d_h = Data_Handler::getInstance();
    if (eid.id == REDISMODULE_EVENT_CLIENT_CHANGE) {
        switch (subevent) {
            case REDISMODULE_SUBEVENT_CLIENT_CHANGE_DISCONNECTED: 
                LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "Handle_Client_Event client disconnected : " + client_name );
                if (!client_name.empty() && d_h.registered_clients.count(client_name) > 0 ) {
                    Unregister_Client(ctx, client_name);
                }
                break;
            case REDISMODULE_SUBEVENT_CLIENT_CHANGE_CONNECTED: 
                LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "Handle_Client_Event client connected : " + client_name);
                break;
        }
    }
}

void Start_Client_Handler(RedisModuleCtx *ctx) {
    LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "Start_Client_Handler ");
    std::thread client_checker_thread(Client_TTL_Handler, ctx);
    client_checker_thread.detach();
}

void Client_TTL_Handler(RedisModuleCtx *ctx) {
    Data_Handler &d_h = Data_Handler::getInstance();
    while(true) {
        auto now = std::chrono::system_clock::now();
        auto ms  = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch());
        unsigned long long now_ms_value = ms.count();
        std::vector<std::string> expire_client_list; 
        for(const auto &pair : d_h.client_2_last_seen) {
            unsigned long long diff_in_ms = now_ms_value - pair.second;
            if( diff_in_ms >= (unsigned long long)(CLIENT_TIMEOUT * 1000) ) {
                LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "Client_TTL_Handler , kill the client : " + pair.first );
                expire_client_list.push_back(pair.first);
            }
        }
        for(auto client : expire_client_list) {
            Unregister_Client(ctx, client);
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1000)); // Check every second
    }
}