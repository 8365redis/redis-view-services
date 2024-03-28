#include "view_count_command.h"

#include <string>
#include <thread>

#include "logger.h"
#include "module_utils.h"


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
    

    Client_Size_Info info;
    info.stream_name = Get_Client_Name(ctx);
    info.arguments.insert(info.arguments.begin(), arguments_arg.begin(), arguments_arg.end());

    d_h.client_size_info.push_back(info);
    //printf("CLIENT_SIZE_INFO size : %lu\n",CLIENT_SIZE_INFO.size());
    
    RedisModule_ReplyWithSimpleString(ctx, "OK");
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
        //LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "Search_Count_Handler called." );
        RedisModule_ThreadSafeContextLock(ctx);
        for(Client_Size_Info &info : d_h.client_size_info){
            //LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "Search_Count_Handler handling : " + info.stream_name );
            long long size = 0;
            std::string index_and_query = "";
            int arg_index = 0;
            for(std::string arg : info.arguments){
                if(arg_index < 2) {
                    index_and_query += arg;
                    index_and_query += " ";
                }
                arg_index++;
                //printf("Search_Count_Handler arg: %s\n", arg.c_str() );
            }

            std::vector<RedisModuleString*> arguments;
            for(std::string arg : info.arguments){
                arguments.push_back(RedisModule_CreateString(ctx, arg.c_str(), arg.length()));
            }
            
            RedisModuleCallReply *reply = RedisModule_Call(ctx,"FT.SEARCH", "v", arguments.begin(), arguments.size());
            //printf("Search_Count_Handler reply : %d\n",RedisModule_CallReplyType(reply));
            if (RedisModule_CallReplyType(reply) != REDISMODULE_REPLY_ARRAY) {
                LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Main_Search_RedisCommand failed." );
            } else {
                RedisModuleCallReply *key_int_reply = RedisModule_CallReplyArrayElement(reply, 0);
                if (RedisModule_CallReplyType(key_int_reply) == REDISMODULE_REPLY_INTEGER){
                    size = RedisModule_CallReplyInteger(key_int_reply);
                }else {
                    LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Main_Search_RedisCommand failed to get reply size." );
                }
            }

            RedisModuleString* client_name = RedisModule_CreateString(ctx, info.stream_name.c_str(), info.stream_name.length());
            RedisModuleKey *stream_key = RedisModule_OpenKey(ctx, client_name, REDISMODULE_WRITE);
            RedisModuleString **xadd_params = (RedisModuleString **) RedisModule_Alloc(sizeof(RedisModuleString *) * 4);
            xadd_params[0] = RedisModule_CreateString(ctx,"INDEX_QUERY", strlen("INDEX_QUERY"));
            xadd_params[1] = RedisModule_CreateString(ctx, index_and_query.c_str(), index_and_query.length());
            xadd_params[2] = RedisModule_CreateString(ctx, "LENGTH", strlen("LENGTH"));
            std::string size_str = std::to_string(size);
            xadd_params[3] = RedisModule_CreateString(ctx, size_str.c_str(), size_str.length());
            int stream_add_resp = RedisModule_StreamAdd( stream_key, REDISMODULE_STREAM_ADD_AUTOID, NULL, xadd_params, 2);
            if (stream_add_resp != REDISMODULE_OK) {
                LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Register_RedisCommand failed to create the stream." );
            }
        }
        RedisModule_ThreadSafeContextUnlock(ctx);
        std::this_thread::sleep_for(std::chrono::milliseconds(1000)); // Check every second
    }
}
