#include <string>
#include <vector>
#include <set>
#include <unordered_map>
#include <thread>
#include <json.hpp>
#include "redismodule.h"
#include "logger.h"

#ifndef CCT_MODULE_VERSION
#define CCT_MODULE_VERSION "unknown"
#endif

using json = nlohmann::json;


#ifdef __cplusplus
extern "C" {
#endif

RedisModuleCtx *rdts_staticCtx;

const std::string CCT_MODULE_QUERY_DELIMETER = "-CCT_DEL-";

typedef struct  {
    std::string stream_name;
    std::vector<std::string> arguments;
} Client_Size_Info;

std::vector<Client_Size_Info> CLIENT_SIZE_INFO; // Will change to map
std::unordered_map<std::string, std::vector<std::string>> CLIENT_2_QUERY_MAP; // Query is vector of string
// Value is key to value mapping , Query as a string this time , concentation of parameters with delimeter
std::unordered_map<std::string, std::unordered_map<std::string, json>> QUERY_2_VALUE_MAP; 

int Main_Search_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
int Register_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
std::string Get_Client_Name(RedisModuleCtx *ctx);
std::string Get_Client_Name_From_ID(RedisModuleCtx *ctx, unsigned long long client_id);
void Start_Main_Search_Handler(RedisModuleCtx *ctx);
void Main_Search_Handler(RedisModuleCtx *ctx, std::vector<Client_Size_Info> &client_size_info_arg);
int View_Search_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
void View_Search_Handler(RedisModuleCtx *ctx, std::unordered_map<std::string, std::unordered_map<std::string, json>> &query_2_value ,
                             std::unordered_map<std::string, std::vector<std::string>> &client_2_query);
void Start_View_Search_Handler(RedisModuleCtx *ctx);

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
    
    /*
    long long size = 0;
    // Forward Search
    RedisModuleCallReply *reply = RedisModule_Call(ctx,"FT.SEARCH", "v", arguments.begin(), arguments.size());
    if (RedisModule_CallReplyType(reply) != REDISMODULE_REPLY_ARRAY) {
        return RedisModule_ReplyWithError(ctx, strerror(errno));
    } else {
        RedisModuleCallReply *key_int_reply = RedisModule_CallReplyArrayElement(reply, 0);
        if (RedisModule_CallReplyType(key_int_reply) == REDISMODULE_REPLY_INTEGER){
            size = RedisModule_CallReplyInteger(key_int_reply);
            printf("main search size : %lld \n",size);
        }else {
            LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Main_Search_RedisCommand failed to get reply size." );
            return RedisModule_ReplyWithError(ctx, strerror(errno));
            return REDISMODULE_ERR;
        }
    }
    */

    Client_Size_Info info;
    info.stream_name = Get_Client_Name(ctx);
    info.arguments.insert(info.arguments.begin(), arguments_arg.begin(), arguments_arg.end());

    CLIENT_SIZE_INFO.push_back(info);
    //printf("CLIENT_SIZE_INFO size : %lu\n",CLIENT_SIZE_INFO.size());
    
    RedisModule_ReplyWithSimpleString(ctx, "OK");
    return REDISMODULE_OK;
}

int View_Search_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
    RedisModule_AutoMemory(ctx);

    std::vector<RedisModuleString*> arguments(argv+1, argv + argc);
    std::vector<std::string> arguments_arg;
    for(RedisModuleString* arg : arguments) {
        arguments_arg.push_back(RedisModule_StringPtrLen(arg, NULL));
    }

    std::string arguments_arg_str = "";
    for(auto const& e : arguments_arg) arguments_arg_str += (e + CCT_MODULE_QUERY_DELIMETER);
    if(arguments_arg_str.length() > CCT_MODULE_QUERY_DELIMETER.length() ) {
        arguments_arg_str.erase(arguments_arg_str.length() - CCT_MODULE_QUERY_DELIMETER.length());
    }

    std::string client_name = Get_Client_Name(ctx);
    CLIENT_2_QUERY_MAP[client_name] = arguments_arg;
    printf("Client : %s , Query : %s \n" , client_name.c_str(), arguments_arg_str.c_str());
    std::unordered_map<std::string, nlohmann::json> empty;
    QUERY_2_VALUE_MAP[arguments_arg_str] = empty;
    
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
        LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "Main_Search_Handler called." );
        for(Client_Size_Info &info : client_size_info_arg){
            LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "Main_Search_Handler handling : " + info.stream_name );
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
        std::this_thread::sleep_for(std::chrono::milliseconds(1000)); // Check every second
    }
}

void Start_View_Search_Handler(RedisModuleCtx *ctx) {
    LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "Start_Main_Search_Handler called." );
    std::thread view_search_thread(View_Search_Handler, ctx, std::ref(QUERY_2_VALUE_MAP), std::ref(CLIENT_2_QUERY_MAP));
    view_search_thread.detach();
}


void View_Search_Handler(RedisModuleCtx *ctx, std::unordered_map<std::string, std::unordered_map<std::string, json>> &query_2_value ,
                             std::unordered_map<std::string, std::vector<std::string>> &client_2_query) {

    while(true) {
        LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "View_Search_Handler called." );
        for (auto &client_2_query_item : client_2_query) {

            std::string arguments_arg_str = "";
            for(auto const& e : client_2_query_item.second) arguments_arg_str += (e + CCT_MODULE_QUERY_DELIMETER);
            if(arguments_arg_str.length() > CCT_MODULE_QUERY_DELIMETER.length() ) {
                arguments_arg_str.erase(arguments_arg_str.length() - CCT_MODULE_QUERY_DELIMETER.length());
            }

            std::vector<RedisModuleString*> arguments;
            for(std::string arg : client_2_query_item.second){
                arguments.push_back(RedisModule_CreateString(ctx, arg.c_str(), arg.length()));
            }
            
            // Forward Search
            RedisModuleCallReply *reply = RedisModule_Call(ctx, "FT.SEARCH", "v", arguments.begin(), arguments.size());
            if (RedisModule_CallReplyType(reply) != REDISMODULE_REPLY_ARRAY) {
                LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "View_Search_Handler FT.SEARCH failed." );
            }
            // Parse Search Result
            const size_t reply_length = RedisModule_CallReplyLength(reply);
            std::vector<std::vector<std::string>> keys;
            for (size_t i = 1; i < reply_length; i++) {   // Starting from 1 as first one count
                RedisModuleCallReply *key_reply = RedisModule_CallReplyArrayElement(reply, i);
                if (RedisModule_CallReplyType(key_reply) == REDISMODULE_REPLY_STRING) {
                    RedisModuleString *response = RedisModule_CreateStringFromCallReply(key_reply);
                    const char *response_str = RedisModule_StringPtrLen(response, NULL);
                    std::vector<std::string> response_vector = {response_str};
                    keys.push_back(response_vector);
                }else if ( RedisModule_CallReplyType(key_reply) == REDISMODULE_REPLY_ARRAY) {
                    size_t inner_reply_length = RedisModule_CallReplyLength(reply);
                    std::vector<std::string> inner_keys;
                    for (size_t i = 0; i < inner_reply_length; i++) {
                        RedisModuleCallReply *inner_key_reply = RedisModule_CallReplyArrayElement(key_reply, i);
                        if (RedisModule_CallReplyType(inner_key_reply) == REDISMODULE_REPLY_STRING) {
                            RedisModuleString *inner_response = RedisModule_CreateStringFromCallReply(inner_key_reply);
                            const char *inner_response_str = RedisModule_StringPtrLen(inner_response, NULL);
                            inner_keys.push_back(inner_response_str);
                        }
                    }
                    keys.push_back(inner_keys);
                }
            }
            
            std::unordered_map<std::string, nlohmann::json> old_values = query_2_value[arguments_arg_str];
            //{key_id : json_value}
            std::unordered_map<std::string, nlohmann::json> new_values;
            std::string key = "";
            for(auto vec : keys){
                if(vec.size() == 1){
                    key = vec.at(0);
                } else {
                    if(key.empty()){
                        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "View_Search_Handler Key is empty." );
                        continue;
                    }
                    std::string value = vec.at(1);
                    if (!json::accept(value)){
                        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "View_Search_Handler JSON is not valid." );
                    } else {
                        new_values[key] = json::parse(value);
                    }
                }
            }
           

            //Compare previous values
            std::unordered_map<std::string, nlohmann::json> diff_values;
            /*
            std::set<std::string> view_old_keys;
            for(auto &kv : query_2_value) {
                view_old_keys.insert(kv.first);
            }
            std::set<std::string> view_new_keys;
            for(auto &kv : new_values) {
                view_new_keys.insert(kv.first);
            }

            // Iterate new keys from old keys
            for(auto &new_key_from_view : view_new_keys){
                nlohmann::json old_value =  old_values[new_key_from_view];
                nlohmann::json new_value = new_values[new_key_from_view];
                if(old_values.find(new_key_from_view) != old_values.end()){
                    // new key is in the old key set , now compare value
                    if(new_value != old_value) {
                        diff_values[new_key_from_view] = new_value;
                    } 
                } else {
                    // new key is in not in the old key set , directly add it to diff
                    diff_values[new_key_from_view] = new_value;
                }
            }

            // Iterate old key from new keys for finding keys not in view anymore
            for(auto &old_key_from_view : view_old_keys){
                if(view_new_keys.find(old_key_from_view) != view_new_keys.end()){
                    diff_values[old_key_from_view] = json::object();
                }
            }
            */


            // Write new values 
            query_2_value[arguments_arg_str] = new_values;

            // Write to stream
            
            int stream_write_size = diff_values.size();
            int stream_write_size_total = (stream_write_size * 2) + 2;
            RedisModuleString* client_name = RedisModule_CreateString(ctx, client_2_query_item.first.c_str(), client_2_query_item.first.length());
            RedisModuleKey *stream_key = RedisModule_OpenKey(ctx, client_name, REDISMODULE_WRITE);
            RedisModuleString **xadd_params = (RedisModuleString **) RedisModule_Alloc(sizeof(RedisModuleString *) * stream_write_size_total);
            xadd_params[0] = RedisModule_CreateString(ctx,"QUERY", strlen("QUERY"));
            xadd_params[1] = RedisModule_CreateString(ctx, arguments_arg_str.c_str(), arguments_arg_str.length());
            int i = 2;
            for(auto&value : diff_values){
                if(i > stream_write_size_total){
                    assert("Stream writing pass limit ???");
                }
                xadd_params[i] = RedisModule_CreateString(ctx, value.first.c_str(), value.first.length());
                xadd_params[i+1] = RedisModule_CreateString(ctx, value.second.dump().c_str(), value.second.dump().length());
                i += 2;
            }
            int stream_add_resp = RedisModule_StreamAdd( stream_key, REDISMODULE_STREAM_ADD_AUTOID, NULL, xadd_params, stream_write_size_total/2);
            if (stream_add_resp != REDISMODULE_OK) {
                LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Register_RedisCommand failed to create the stream." );
            }            
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1000)); // Check every second
    }
}


#ifdef __cplusplus
}
#endif

