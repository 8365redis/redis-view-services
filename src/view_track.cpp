#include <string>
#include <vector>
#include <set>
#include <unordered_map>
#include <thread>
#include <json.hpp>
#include "redismodule.h"
#include "logger.h"
#include "view_search_command.h"
#include "module_constants.h"
#include "module_utils.h"

#ifndef CCT_MODULE_VERSION
#define CCT_MODULE_VERSION "unknown"
#endif

using json = nlohmann::json;


#ifdef __cplusplus
extern "C" {
#endif

static RedisModuleCtx *rdts_staticCtx;

int Main_Search_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
int Register_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);

void Start_Main_Search_Handler(RedisModuleCtx *ctx);
void Main_Search_Handler(RedisModuleCtx *ctx, std::vector<Client_Size_Info> &client_size_info_arg);


int RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {

    const char* version_string = { CCT_MODULE_VERSION " compiled at " __TIME__ " "  __DATE__  };
    LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "CCT_MODULE_VERSION : " + std::string(version_string));

    #ifdef _DEBUG
    LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "THIS IS A DEBUG BUILD." );
    #endif
    #ifdef NDEBUG
    LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "THIS IS A RELEASE BUILD." );
    #endif 

    if (RedisModule_Init(ctx, "CCT-VIEW", 1, REDISMODULE_APIVER_1) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    rdts_staticCtx = RedisModule_GetDetachedThreadSafeContext(ctx);

    if (RedisModule_CreateCommand(ctx,"VIEW.REGISTER", Register_RedisCommand , "admin write", 0, 0, 0) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    } else {
        LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "VIEW.REGISTER command created successfully.");
    }

    if (RedisModule_CreateCommand(ctx,"VIEW.MAIN.SEARCH", Main_Search_RedisCommand , "readonly", 0, 0, 0) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    } else {
        LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "VIEW.MAIN.SEARCH command created successfully.");
    }

    if (RedisModule_CreateCommand(ctx,"VIEW.SEARCH", View_Search_RedisCommand , "readonly", 0, 0, 0) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    } else {
        LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "VIEW.SEARCH command created successfully.");
    }

    Start_Main_Search_Handler(rdts_staticCtx);

    Start_View_Search_Handler(rdts_staticCtx);
 
    return REDISMODULE_OK;
}

int Register_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    
    if (argc < 2  || argc > 3) {
        return RedisModule_WrongArity(ctx);
    }

    unsigned long long client_query_ttl = 0;
    if (argc == 3) {
        RedisModuleString *client_query_ttl_str = argv[2];
        if(RedisModule_StringToULongLong(client_query_ttl_str, &client_query_ttl) == REDISMODULE_ERR ) {
            LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Register_RedisCommand failed to set client query TTL. Invalid TTL value." );
            return RedisModule_ReplyWithError(ctx, strerror(errno));
        }
    }

    // Get Client ID
    RedisModuleString *client_name = argv[1];
    unsigned long long client_id = RedisModule_GetClientId(ctx);
    
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
    return REDISMODULE_OK;
}

int Main_Search_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
    RedisModule_AutoMemory(ctx);

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

    CLIENT_SIZE_INFO.push_back(info);
    //printf("CLIENT_SIZE_INFO size : %lu\n",CLIENT_SIZE_INFO.size());
    
    RedisModule_ReplyWithSimpleString(ctx, "OK");
    return REDISMODULE_OK;
}

void Start_Main_Search_Handler(RedisModuleCtx *ctx) {
    LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "Start_Main_Search_Handler called." );
    std::thread main_search_thread(Main_Search_Handler, ctx, std::ref(CLIENT_SIZE_INFO));
    main_search_thread.detach();
}


void Main_Search_Handler(RedisModuleCtx *ctx, std::vector<Client_Size_Info> &client_size_info_arg) {

    while(true) {
        //LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "Main_Search_Handler called." );
        RedisModule_ThreadSafeContextLock(ctx);
        for(Client_Size_Info &info : client_size_info_arg){
            //LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "Main_Search_Handler handling : " + info.stream_name );
            long long size = 0;
            std::string index_and_query = "";
            int arg_index = 0;
            for(std::string arg : info.arguments){
                if(arg_index < 2) {
                    index_and_query += arg;
                    index_and_query += " ";
                }
                arg_index++;
                //printf("Main_Search_Handler arg: %s\n", arg.c_str() );
            }

            std::vector<RedisModuleString*> arguments;
            for(std::string arg : info.arguments){
                arguments.push_back(RedisModule_CreateString(ctx, arg.c_str(), arg.length()));
            }
            
            RedisModuleCallReply *reply = RedisModule_Call(ctx,"FT.SEARCH", "v", arguments.begin(), arguments.size());
            //printf("Main_Search_Handler reply : %d\n",RedisModule_CallReplyType(reply));
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


#ifdef __cplusplus
}
#endif

