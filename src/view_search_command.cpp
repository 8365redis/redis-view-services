#include <string>
#include <set>
#include <thread>
#include "view_search_command.h"
#include "logger.h"
#include "module_constants.h"
#include "module_utils.h"

using json = nlohmann::json;

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

void Start_View_Search_Handler(RedisModuleCtx *ctx) {
    LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "Start_Main_Search_Handler called." );
    std::thread view_search_thread(View_Search_Handler, ctx, std::ref(QUERY_2_VALUE_MAP), std::ref(CLIENT_2_QUERY_MAP));
    view_search_thread.detach();
}


void View_Search_Handler(RedisModuleCtx *ctx, std::unordered_map<std::string, std::unordered_map<std::string, json>> &query_2_value ,
                             std::unordered_map<std::string, std::vector<std::string>> &client_2_query) {

    while(true) {
        //LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "View_Search_Handler called." );
        RedisModule_ThreadSafeContextLock(ctx);
        for (auto &client_2_query_item : client_2_query) {
            //LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "View_Search_Handler iterate for: " + client_2_query_item.first );
            std::string arguments_arg_str = "";
            for(auto const& e : client_2_query_item.second) arguments_arg_str += (e + CCT_MODULE_QUERY_DELIMETER);
            if(arguments_arg_str.length() > CCT_MODULE_QUERY_DELIMETER.length() ) {
                arguments_arg_str.erase(arguments_arg_str.length() - CCT_MODULE_QUERY_DELIMETER.length());
            }

            std::vector<RedisModuleString*> arguments;
            for(std::string arg : client_2_query_item.second){
                arguments.push_back(RedisModule_CreateString(ctx, arg.c_str(), arg.length()));
                LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "View_Search_Handler: Arguments : " + arg );
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
            
            std::set<std::string> view_old_keys;
            for(auto &kv : query_2_value) {
                view_old_keys.insert(kv.first);
                LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "View_Search_Handler: Old key : " + kv.first );
            }
            std::set<std::string> view_new_keys;
            for(auto &kv : new_values) {
                view_new_keys.insert(kv.first);
                LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "View_Search_Handler: New key : " + kv.first );
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
            


            // Write new values 
            query_2_value[arguments_arg_str] = new_values;

            // Write to stream
            
            int stream_write_size = diff_values.size();
            if( stream_write_size != 0 ) { 
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
        }
        RedisModule_ThreadSafeContextUnlock(ctx);
        std::this_thread::sleep_for(std::chrono::milliseconds(1000)); // Check every second
    }
}