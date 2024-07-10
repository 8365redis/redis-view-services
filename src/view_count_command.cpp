#include "view_count_command.h"

#include <string>
#include <thread>

#include "logger.h"
#include "module_utils.h"
#include "module_constants.h"

static unsigned long LAST_VIEW_COUNT_IDENTIFIER = 1;

int Search_Count_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
    RedisModule_AutoMemory(ctx);

    Data_Handler &d_h = Data_Handler::getInstance();

    std::vector<RedisModuleString*> arguments(argv+1, argv + argc);
    arguments.push_back(RedisModule_CreateString(ctx, "LIMIT", 5 ));
    arguments.push_back(RedisModule_CreateString(ctx, "0", 1 ));
    arguments.push_back(RedisModule_CreateString(ctx, "0", 1 ));

    std::vector<std::string> arguments_arg;
    for(RedisModuleString* arg : arguments){
        arguments_arg.push_back(RedisModule_StringPtrLen(arg, NULL));
    }

    long long int current_count = 0;
    RedisModuleCallReply *reply = RedisModule_Call(ctx,"FT.SEARCH", "v", arguments.begin(), arguments.size());
    if (RedisModule_CallReplyType(reply) != REDISMODULE_REPLY_ARRAY) {
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Search_Count_RedisCommand failed." );
        return REDISMODULE_ERR;
    } else {
        RedisModuleCallReply *key_int_reply = RedisModule_CallReplyArrayElement(reply, 0);
        if (RedisModule_CallReplyType(key_int_reply) == REDISMODULE_REPLY_INTEGER){
            current_count = RedisModule_CallReplyInteger(key_int_reply);
            LOG(ctx, REDISMODULE_LOGLEVEL_VERBOSE , "Search_Count_RedisCommand get size : " + std::to_string(current_count));
        }else {
            LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Search_Count_RedisCommand failed to get reply current_count.");
            return REDISMODULE_ERR;
        }
    }

    // Reply array sizing
    RedisModule_ReplyWithArray(ctx , 6);  // id , count , result's   

    // Reply Add the new query ID  
    RedisModule_ReplyWithCString(ctx, COUNT_QUERY_ID_STR.c_str());
    RedisModule_ReplyWithLongLong(ctx, LAST_VIEW_COUNT_IDENTIFIER);

    // Reply Add the query 
    std::string index_and_query = arguments_arg[0] + " " + arguments_arg[1]; 
    RedisModule_ReplyWithCString(ctx, COUNT_QUERY_STR.c_str());
    RedisModule_ReplyWithCString(ctx, index_and_query.c_str());

    // Reply count  
    RedisModule_ReplyWithCString(ctx, COUNT_STR.c_str());
    RedisModule_ReplyWithLongLong(ctx, current_count);

    std::string client_name = Get_Client_Name(ctx);
    Client_Size_Info info(client_name, arguments_arg, current_count , LAST_VIEW_COUNT_IDENTIFIER);
    
    d_h.client_size_info[client_name].insert(std::make_pair(LAST_VIEW_COUNT_IDENTIFIER, info));

    LAST_VIEW_COUNT_IDENTIFIER++;
    return REDISMODULE_OK;
}

void Start_Search_Count_Handler(RedisModuleCtx *ctx) {
    LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "Start_Search_Count_Handler called." );
    std::thread main_search_thread(Search_Count_Handler, ctx);
    main_search_thread.detach();
}


void Search_Count_Handler(RedisModuleCtx *ctx) {
    Data_Handler &d_h = Data_Handler::getInstance();
    while(true) {
        // First unsub the count
        for(const auto &unsub_count_info : d_h.unsub_count_wait_list){
            if(d_h.client_size_info.count(unsub_count_info.stream_name) == 0){
                LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Search_Count_Handler Unsub for count not found for client : " + unsub_count_info.stream_name);
                continue;
            } else {
                for (auto it = d_h.client_size_info[unsub_count_info.stream_name].begin(); it != d_h.client_size_info[unsub_count_info.stream_name].end(); ) {
                    if ( (*it).first == unsub_count_info.id) {
                        it = d_h.client_size_info[unsub_count_info.stream_name].erase(it); // erase returns the next iterator
                    } else {
                        ++it; // only increment if not erased
                    }
                }
            }
        }

        //LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "Search_Count_Handler called." );
        RedisModule_ThreadSafeContextLock(ctx);
        for(auto &clients_info : d_h.client_size_info){
            for (auto& key2info : clients_info.second){
                auto &info = key2info.second;
                //LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "Search_Count_Handler handling : " + info.stream_name );
                long long int current_count = 0;
                if(info.arguments.size() < 2){
                    LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Search_Count_Handler client size info argument size less than 2.");
                    continue;
                }
                std::string index_and_query = info.arguments[0] + " " + info.arguments[1];

                std::vector<RedisModuleString*> arguments;
                for(std::string arg : info.arguments){
                    arguments.push_back(RedisModule_CreateString(ctx, arg.c_str(), arg.length()));
                }
                
                RedisModuleCallReply *reply = RedisModule_Call(ctx,"FT.SEARCH", "v", arguments.begin(), arguments.size());
                //printf("Search_Count_Handler reply : %d\n",RedisModule_CallReplyType(reply));
                if (RedisModule_CallReplyType(reply) != REDISMODULE_REPLY_ARRAY) {
                    LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Search_Count_RedisCommand Handler failed." );
                } else {
                    RedisModuleCallReply *key_int_reply = RedisModule_CallReplyArrayElement(reply, 0);
                    if (RedisModule_CallReplyType(key_int_reply) == REDISMODULE_REPLY_INTEGER){
                        current_count = RedisModule_CallReplyInteger(key_int_reply);
                        LOG(ctx, REDISMODULE_LOGLEVEL_VERBOSE , "Search_Count_RedisCommand Handler get size : " + std::to_string(current_count)  + " for query : " + index_and_query);
                    }else {
                        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Search_Count_RedisCommand Handler failed to get reply current_count." );
                    }
                }

                const int stream_write_size = 6; // id , query, count ; per key and value
                const int stream_write_field_num = stream_write_size / 2 ;
                if(info.count != current_count) {
                    RedisModuleString* client_name = RedisModule_CreateString(ctx, info.stream_name.c_str(), info.stream_name.length());
                    RedisModuleKey *stream_key = RedisModule_OpenKey(ctx, client_name, REDISMODULE_WRITE);
                    RedisModuleString **xadd_params = (RedisModuleString **) RedisModule_Alloc(sizeof(RedisModuleString *) * stream_write_size);

                    xadd_params[0] = RedisModule_CreateString(ctx, COUNT_QUERY_ID_STR.c_str(), COUNT_QUERY_ID_STR.length());
                     std::string id_str = std::to_string(info.id);
                    xadd_params[1] = RedisModule_CreateString(ctx, id_str.c_str(), id_str.length());

                    xadd_params[2] = RedisModule_CreateString(ctx, COUNT_QUERY_STR.c_str(), COUNT_QUERY_STR.length());
                    xadd_params[3] = RedisModule_CreateString(ctx, index_and_query.c_str(), index_and_query.length());

                    std::string size_str = std::to_string(current_count);
                    xadd_params[4] = RedisModule_CreateString(ctx, COUNT_STR.c_str(), COUNT_STR.length());
                    xadd_params[5] = RedisModule_CreateString(ctx, size_str.c_str(), size_str.length());

                    int stream_add_resp = RedisModule_StreamAdd( stream_key, REDISMODULE_STREAM_ADD_AUTOID, NULL, xadd_params, stream_write_field_num);
                    if (stream_add_resp != REDISMODULE_OK) {
                        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Search_Count_RedisCommand failed to create the stream." );
                    }
                }
                info.count = current_count;
            }
        }
        RedisModule_ThreadSafeContextUnlock(ctx);
        std::this_thread::sleep_for(std::chrono::milliseconds(1000)); // Check every second
    }
}
