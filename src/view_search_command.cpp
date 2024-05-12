#include <string>
#include <set>
#include <thread>
#include "view_search_command.h"
#include "logger.h"
#include "module_constants.h"
#include "module_utils.h"
#include "module_data_handler.h"

using json = nlohmann::json;

static long long int LAST_VIEW_SEARCH_IDENTIFIER = 1;

class View_Diff {
public:
    long long int index = 0;
    std::string old_key = "";
    std::string new_key = "";
    std::string operation = "";
    nlohmann::json old_value;
    nlohmann::json new_value;
    nlohmann::json diff;
    nlohmann::json to_json()  {
        nlohmann::json view_diff_str;
        view_diff_str["index"] = index;
        view_diff_str["old_key"] = old_key;
        view_diff_str["new_key"] = new_key;
        view_diff_str["operation"] = operation;
        view_diff_str["old_value"] = old_value;
        view_diff_str["new_value"] = new_value;
        view_diff_str["diff"] = diff;
        return view_diff_str;
    };
};  


bool AttributeExist(RedisModuleCtx *ctx, std::string index_name, std::string sortby_arg) {
    Data_Handler &d_h = Data_Handler::getInstance();
    // Next two line MUST match the part in NeedToAddDialect4 function
    // That part caches we just check if exist
    sortby_arg = "$." + sortby_arg;
    std::string type_key = index_name + "-" + sortby_arg;

    if(d_h.index_attribute_2_type.count(type_key) > 0) {
        return true;
    } else {
        return false;
    }
}

bool NeedToAddDialect4(RedisModuleCtx *ctx, std::string index_name, std::string sortby_arg) { 

    Data_Handler &d_h = Data_Handler::getInstance();

    //printf("Index name: %s\n", index_name.c_str()); // Debug print
    //printf("Sortby arg: %s\n", sortby_arg.c_str()); // Debug print
    sortby_arg = "$." + sortby_arg;

    std::string type_key = index_name + "-" + sortby_arg;
    if(d_h.index_attribute_2_type.count(type_key) > 0) {
        //printf("Type key : %s found in cache\n", type_key.c_str()); // Debug print
        if(d_h.index_attribute_2_type[type_key] == "NUMERIC") {
            return true;
        }
        return false;
    }

    RedisModuleCallReply *info_reply = RedisModule_Call(ctx, "FT.INFO", "c", index_name.c_str());
    if (info_reply == NULL || RedisModule_CallReplyType(info_reply) == REDISMODULE_REPLY_ERROR) {
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Query check for tag has failed with ft.info failed. Call failed." );
        return false;
    }

    if (RedisModule_CallReplyType(info_reply) != REDISMODULE_REPLY_ARRAY) {
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Query check for tag has failed with ft.info failed. Not array." );
        return false;
    }

    const size_t aggreate_reply_length = RedisModule_CallReplyLength(info_reply);
    //printf("Reply length: %zu\n", aggreate_reply_length); // Debug print

    RedisModuleCallReply *attributes_reply = NULL;
    const char *specific_identifier = sortby_arg.c_str() ; 
    //printf("Specific identifier : %s\n", specific_identifier); // Debug print

    // Find the "attributes" section
    for (size_t i = 0; i < aggreate_reply_length; i++) {
        RedisModuleCallReply *key_reply = RedisModule_CallReplyArrayElement(info_reply, i);
        
        if (RedisModule_CallReplyType(key_reply) == REDISMODULE_REPLY_STRING){
            RedisModuleString *response = RedisModule_CreateStringFromCallReply(key_reply);
            const char *key_str = RedisModule_StringPtrLen(response, NULL);
            if(key_str == nullptr){
                continue;
            }
            if (strcmp(key_str, "attributes") == 0) {
                attributes_reply = RedisModule_CallReplyArrayElement(info_reply, i + 1);
                break;
            }
        }
    }

    if (attributes_reply == NULL) {
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Query check for tag has failed with ft.info doesn't have attributes." );
        return false;
    }

    if (RedisModule_CallReplyType(attributes_reply) != REDISMODULE_REPLY_ARRAY) {
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Query check for tag has failed with ft.info attributes failed. Not array." );
        return false;
    }

    // Process the "attributes" section
    const size_t attributes_length = RedisModule_CallReplyLength(attributes_reply);
    //printf("Attributes length: %zu\n", attributes_length); // Debug print

    bool attribute_found = false;
    bool attribute_type_is_numeric = false;

    for (size_t i = 0; i < attributes_length; i++) {
        RedisModuleCallReply *attribute_inner_reply = RedisModule_CallReplyArrayElement(attributes_reply, i);
        if (RedisModule_CallReplyType(attribute_inner_reply) != REDISMODULE_REPLY_ARRAY) {
            LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Query check for tag has failed with ft.info attributes inner failed. Not array." );
            return false;
        }
        const size_t attribute_inner_length = RedisModule_CallReplyLength(attribute_inner_reply);
        //printf("Attributes inner length: %zu\n", attribute_inner_length); // Debug print


        // Check if the attribute is the specific identifier
        RedisModuleCallReply *identifier_reply = RedisModule_CallReplyArrayElement(attribute_inner_reply, 1); // 1 is the index of the identifier value
        if (RedisModule_CallReplyType(identifier_reply) == REDISMODULE_REPLY_STRING) {
            RedisModuleString *identifier_response = RedisModule_CreateStringFromCallReply(identifier_reply);
            const char *identifier_str = RedisModule_StringPtrLen(identifier_response, NULL);
            //printf("Identifier: %s\n", identifier_str); // Debug print
            if (strcmp(identifier_str, specific_identifier) != 0) {
                continue;
            } else {
                attribute_found = true;
            }
        } else {
            LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Query check for tag has failed with ft.info attributes (for only one) inner identifier failed. Not string. Continue with others." );
            continue;
        }

        const char *type = NULL;

        // Find the "type" key within the attribute
        for (size_t j = 0; j < attribute_inner_length; j++) {
            RedisModuleCallReply *attribute_inner_reply_item = RedisModule_CallReplyArrayElement(attribute_inner_reply, j);
            RedisModuleString *attribute_inner_reply_item_str = RedisModule_CreateStringFromCallReply(attribute_inner_reply_item);
            const char *inner_reply_item_str = RedisModule_StringPtrLen(attribute_inner_reply_item_str, NULL);

            if (strcmp(inner_reply_item_str, "type") == 0) {
                RedisModuleCallReply *type_reply = RedisModule_CallReplyArrayElement(attribute_inner_reply, j + 1);
                RedisModuleString *type_reply_str = RedisModule_CreateStringFromCallReply(type_reply);
                type = RedisModule_StringPtrLen(type_reply_str, NULL);
                //printf("Type: %s\n", type); // Debug print
                break;
            }
        }

        if (type != NULL)  {
            if (  strcmp(type, "NUMERIC") == 0){
                attribute_type_is_numeric = true;
                //printf("Attribute with key : %s is not NUMERIC \n", type_key.c_str()); // Debug print
            } //else {
                //printf("Attribute with key : %s is NUMERIC \n", type_key.c_str()); // Debug print
            //}
            d_h.index_attribute_2_type[type_key] = type;
            break;
        }

    }

    if(attribute_found == false) {
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Query check for tag has failed with ft.info attribute with given identifier not found." );
    } else if(attribute_type_is_numeric == true) {
        //printf("Attribute with key : %s returning true \n", type_key.c_str()); // Debug print
        return true;
    }

    return false;
}

int View_Search_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
    RedisModule_AutoMemory(ctx);

    LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "View_Search_RedisCommand called." );

    Data_Handler &d_h = Data_Handler::getInstance();
    
    std::vector<RedisModuleString*> arguments_redis_string_vector(argv+1, argv + argc);
    std::vector<std::string> arguments_string_vector;
    std::vector<std::string> arguments_string_converted_vector;
    bool limit_arg_found = false;
    bool sortby_arg_found = false;
    std::string sortby_arg = "";
    std::string sortby_arg_order = "";
    for(long unsigned int i = 0; i < arguments_redis_string_vector.size(); i++) {
        std::string arg_str = RedisModule_StringPtrLen(arguments_redis_string_vector[i], NULL);
        if (limit_arg_found == false && arg_str == "LIMIT") {
            limit_arg_found = true;
        }
        if (sortby_arg_found == false && arg_str == "SORTBY") {
            sortby_arg_found = true;
            if(i+1 < arguments_redis_string_vector.size()){
                sortby_arg = RedisModule_StringPtrLen(arguments_redis_string_vector[i+1], NULL);
            }
            if(i+2 < arguments_redis_string_vector.size()){
                sortby_arg_order = RedisModule_StringPtrLen(arguments_redis_string_vector[i+2], NULL);
                if(sortby_arg_order != "DESC" && sortby_arg_order != "ASC") {
                    sortby_arg_order = "ASC"; // DEFAULT
                }
            }
        }
        arguments_string_vector.push_back(arg_str);
    }

    // INNERKEY MUST be last 2 parameters
    std::string inner_key_arg = "";
    bool inner_key_arg_found = false;
    if(arguments_string_vector.size() > 2 && arguments_string_vector[arguments_string_vector.size() - 2] == "INNERKEY") {
        inner_key_arg = arguments_string_vector[arguments_string_vector.size() - 1];
        inner_key_arg_found = true;
        arguments_string_vector.pop_back(); // Need to delete last two params
        arguments_string_vector.pop_back(); // Not a duplicate line , this is faster
    }

    if(sortby_arg_found == false || sortby_arg == ""){
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "View_Search_RedisCommand query doesn't have SORTBY argument." );
        return RedisModule_ReplyWithError(ctx,"No Sortby arg");
    }

    if(limit_arg_found == false) {
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "View_Search_RedisCommand query doesn't have LIMIT argument." );
        return RedisModule_ReplyWithError(ctx, "No Limit arg");
    }

    //Check dialect 4
    bool needToAddDialect4 = NeedToAddDialect4(ctx, arguments_string_vector[0], sortby_arg);
    // After first dialect check we can check if the inner key exist (need to fill the cache for the index)
    bool inner_key_exist = AttributeExist(ctx, arguments_string_vector[0], inner_key_arg);
    if (needToAddDialect4) {
        if(inner_key_exist && inner_key_arg_found) {
            needToAddDialect4 = NeedToAddDialect4(ctx, arguments_string_vector[0], inner_key_arg); // Optimize here , handle it with one call
        }
        if(needToAddDialect4) {
            LOG(ctx, REDISMODULE_LOGLEVEL_VERBOSE , "View_Search_RedisCommand adding query DiALECT 4." );
            arguments_string_vector.push_back("DIALECT");
            arguments_string_vector.push_back("4");
        }
    }

    if(!inner_key_exist){
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "View_Search_RedisCommand Given INNERKEY is not found in index , fallback to __key." );
        inner_key_arg_found = false;
        inner_key_arg = "";
    }


    //convert to aggragate
    for(auto &ft_search_arg : arguments_string_vector) {
        if(ft_search_arg == "SORTBY") {
            if(!inner_key_arg_found){
                arguments_string_converted_vector.push_back("LOAD");
                arguments_string_converted_vector.push_back("1");
                arguments_string_converted_vector.push_back("@__key");
            }
            arguments_string_converted_vector.push_back(ft_search_arg);
            arguments_string_converted_vector.push_back("4");
            if(inner_key_arg_found) {
                arguments_string_converted_vector.push_back("@"+inner_key_arg);
                arguments_string_converted_vector.push_back("ASC");                
            }            
            arguments_string_converted_vector.push_back("@"+sortby_arg);
            arguments_string_converted_vector.push_back(sortby_arg_order);
            if(!inner_key_arg_found) {
                arguments_string_converted_vector.push_back("@__key");
                arguments_string_converted_vector.push_back("ASC");            
            } 
        } else if (ft_search_arg == sortby_arg ) {
            continue; // We have already added
        }else if (ft_search_arg == "DESC" || ft_search_arg == "ASC") {
            continue; // We have already added
        } else {
            arguments_string_converted_vector.push_back(ft_search_arg);
        }
    }

    std::string client_name_str = Get_Client_Name(ctx);

    std::string arguments_string_converted = "";
    for(auto const& e : arguments_string_converted_vector) arguments_string_converted += (e + CCT_MODULE_QUERY_DELIMETER);
    if(arguments_string_converted.length() > CCT_MODULE_QUERY_DELIMETER.length() ) {
        arguments_string_converted.erase(arguments_string_converted.length() - CCT_MODULE_QUERY_DELIMETER.length());
    }

    int new_argc = arguments_string_converted_vector.size();
    RedisModuleString **new_argv = (RedisModuleString **)RedisModule_Alloc(sizeof(RedisModuleString *) * new_argc);
    for (int i = 0; i < new_argc; ++i) {
        new_argv[i] = RedisModule_CreateString(ctx, arguments_string_converted_vector[i].c_str(), arguments_string_converted_vector[i].size());
    }


    LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "View_Search_RedisCommand called for client : " + client_name_str + " with converted argument : " + arguments_string_converted );
    
    // Forward Search
    RedisModuleCallReply *aggregate_reply = RedisModule_Call(ctx, "FT.AGGREGATE", "v", new_argv, new_argc);
    if (RedisModule_CallReplyType(aggregate_reply) != REDISMODULE_REPLY_ARRAY) {
        RedisModule_Free(new_argv);
        return RedisModule_ReplyWithError(ctx, strerror(errno));
    }
    
    LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "View_Search_RedisCommand forwarded request is succesfull." );
    RedisModule_Free(new_argv);

    // Parse Search Result
    long long total_size_from_reply;
    const size_t aggreate_reply_length = RedisModule_CallReplyLength(aggregate_reply);
    LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "View_Search_RedisCommand aggregate_reply size : " + aggreate_reply_length );
    RedisModuleCallReply *key_int_reply = RedisModule_CallReplyArrayElement(aggregate_reply, 0);
    if (RedisModule_CallReplyType(key_int_reply) == REDISMODULE_REPLY_INTEGER){
        total_size_from_reply = RedisModule_CallReplyInteger(key_int_reply);
    }else {
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "View_Search_RedisCommand failed to get aggregate_reply size." );
        return RedisModule_ReplyWithError(ctx, strerror(errno));   
    }

    // First get keys
    std::vector<std::string> keys;
    for (size_t i = 1; i < aggreate_reply_length; i++) {   // Starting from 1 as first one count
        RedisModuleCallReply *key_reply = RedisModule_CallReplyArrayElement(aggregate_reply, i);
        if ( RedisModule_CallReplyType(key_reply) == REDISMODULE_REPLY_ARRAY) {
            size_t inner_reply_length = RedisModule_CallReplyLength(key_reply);
            if(inner_reply_length == 4) {
                RedisModuleCallReply *inner_key_reply = RedisModule_CallReplyArrayElement(key_reply, 1); // getting first value which MUST be the key
                RedisModuleString *arr_elem_key_name = RedisModule_CreateStringFromCallReply(inner_key_reply);
                const char *arr_elem_key_name_str = RedisModule_StringPtrLen(arr_elem_key_name, NULL);
                LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "View_Search_RedisCommand pushing key: " + std::string(arr_elem_key_name_str) );
                keys.push_back(arr_elem_key_name_str);
            } else {
                LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "View_Search_RedisCommand get unexpected (array size is wrong)." );
                return RedisModule_ReplyWithError(ctx, strerror(errno));                    
            }
        }else {
            LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "View_Search_RedisCommand get unexpected (not an array)." );
            return RedisModule_ReplyWithError(ctx, strerror(errno));              
        }
    }

    // Reply array sizing
    RedisModule_ReplyWithArray(ctx , 6);  // id , count , k_v's   

    // Reply Add the new query ID  
    RedisModule_ReplyWithCString(ctx, "query_id");
    RedisModule_ReplyWithLongLong(ctx, LAST_VIEW_SEARCH_IDENTIFIER);

    // Reply Add the response size
    RedisModule_ReplyWithCString(ctx, "count");
    RedisModule_ReplyWithLongLong(ctx, total_size_from_reply);

    // Reply key and values
    RedisModule_ReplyWithCString(ctx, "key_and_values");

    std::unordered_map<std::string, nlohmann::json> key_value_map;
    if (keys.size() != 0 ) {

        RedisModule_ReplyWithArray(ctx , keys.size() * 4); // key name , value 

        //Now get values
        int get_argc = keys.size();
        RedisModuleString **get_argv = (RedisModuleString **)RedisModule_Alloc(sizeof(RedisModuleString *) * get_argc);
        for (int i = 0; i < get_argc; ++i) {
            get_argv[i] = RedisModule_CreateString(ctx, keys[i].c_str(), keys[i].size());
        }

        RedisModuleCallReply *get_reply = RedisModule_Call(ctx, "JSON.MGET", "vc", get_argv , get_argc, "$");
        if (RedisModule_CallReplyType(get_reply) == REDISMODULE_REPLY_ERROR) {
            LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "View_Search_RedisCommand JSON.MGET returned not an array." );
            RedisModule_Free(get_argv);
            return RedisModule_ReplyWithError(ctx, strerror(errno));
        } 
        RedisModule_Free(get_argv);


        // Parse get values
        
        const size_t get_reply_length = RedisModule_CallReplyLength(get_reply);
        if(get_reply_length != keys.size() ) {
            LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "View_Search_RedisCommand JSON.MGET returned array size is not right. Reply : " +  std::to_string(get_reply_length)  );
            return RedisModule_ReplyWithError(ctx, strerror(errno));        
        }
        for (size_t i = 0; i < get_reply_length; i++) {  
            RedisModuleCallReply *get_inner_reply = RedisModule_CallReplyArrayElement(get_reply, i);
            RedisModuleString *get_reply_elem = RedisModule_CreateStringFromCallReply(get_inner_reply);
            const char *get_reply_elem_str = RedisModule_StringPtrLen(get_reply_elem, NULL);
            if (!json::accept(get_reply_elem_str)){
                LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "View_Search_RedisCommand JSON.MGET returned an element which is not valid json" );
                continue;
            }
            std::string value = get_reply_elem_str;
            if (value.size() > 0 && value[0] == '[' && value[value.size() - 1] == ']') {
                value = value.substr(1, value.size() - 2);
            }
            key_value_map[keys[i]] = json::parse(value);
            LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "View_Search_RedisCommand key: " + std::string(keys[i]) + " value: " + value );
        }

        // Return Values
        for (auto &k : keys){
            RedisModule_ReplyWithCString(ctx, "key_name");
            RedisModule_ReplyWithStringBuffer(ctx, k.c_str() ,k.length() );
            RedisModule_ReplyWithCString(ctx, "value");
            RedisModule_ReplyWithArray(ctx , 4); // partial and data
            RedisModule_ReplyWithCString(ctx, "attribute");
            RedisModule_ReplyWithCString(ctx, "$");
            if (key_value_map.count(k) == 0) {
                RedisModule_ReplyWithCString(ctx, "data");
                RedisModule_ReplyWithEmptyString(ctx);
                LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "View_Search_RedisCommand JSON.MGET returned an element not matching key" );
                continue;            
            }
            std::string val = key_value_map[k].dump();
            RedisModule_ReplyWithCString(ctx, "data");
            RedisModule_ReplyWithStringBuffer(ctx, val.c_str() , val.length());
        }
    } else {
        RedisModule_ReplyWithEmptyArray(ctx);
    }

    //UPDATE GLOBALS
    if( d_h.client_2_query.count(client_name_str) == 0) { 
        std::unordered_map<long long int, std::vector<std::string>> first_entry_dict;
        first_entry_dict[LAST_VIEW_SEARCH_IDENTIFIER] = arguments_string_converted_vector;
        d_h.client_2_query[client_name_str] = first_entry_dict;
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "View_Search_RedisCommand clients first query." );
    }else {
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "View_Search_RedisCommand client has previous queries." );
        d_h.client_2_query[client_name_str][LAST_VIEW_SEARCH_IDENTIFIER] = arguments_string_converted_vector;
    }

    d_h.id_2_query[LAST_VIEW_SEARCH_IDENTIFIER] = arguments_string_converted;
    d_h.query_2_value[LAST_VIEW_SEARCH_IDENTIFIER] = key_value_map;
    d_h.query_2_index[LAST_VIEW_SEARCH_IDENTIFIER] = keys;

    LAST_VIEW_SEARCH_IDENTIFIER = LAST_VIEW_SEARCH_IDENTIFIER + 1;


    return REDISMODULE_OK;
}

void Start_View_Search_Handler(RedisModuleCtx *ctx) {
    LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "Start_View_Search_Handler called." );
    std::thread view_search_thread(View_Search_Handler, ctx);
    view_search_thread.detach();
}

void View_Search_Handler(RedisModuleCtx *ctx) {
    Data_Handler &d_h = Data_Handler::getInstance();
    while(true) {
        //LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "View_Search_Handler called." );
        RedisModule_ThreadSafeContextLock(ctx);
        for (auto &client_2_query_item : d_h.client_2_query) {
            std::string current_client = client_2_query_item.first;
            for (auto &id_to_query_item : client_2_query_item.second) {
                //LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "View_Search_Handler iterate for: " + client_2_query_item.first );
                long long int current_query_id = id_to_query_item.first;

                std::vector<RedisModuleString*> arguments;
                for(std::string arg : id_to_query_item.second){
                    arguments.push_back(RedisModule_CreateString(ctx, arg.c_str(), arg.length()));
                    //LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "View_Search_Handler: Arguments : " + arg );
                }
                
                LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "View_Search_Handler calling FT.AGGREGATE with args : " + d_h.id_2_query[current_query_id] );
                // Forward Search
                RedisModuleCallReply *reply = RedisModule_Call(ctx, "FT.AGGREGATE", "v", arguments.begin(), arguments.size());
                if (RedisModule_CallReplyType(reply) != REDISMODULE_REPLY_ARRAY) {
                    LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "View_Search_Handler FT.AGGREGATE failed." );
                }

                // First get keys
                const size_t aggreate_reply_length = RedisModule_CallReplyLength(reply);
                std::vector<std::string> view_new_keys;
                for (size_t i = 1; i < aggreate_reply_length; i++) {   // Starting from 1 as first one count
                    RedisModuleCallReply *key_reply = RedisModule_CallReplyArrayElement(reply, i);
                    if ( RedisModule_CallReplyType(key_reply) == REDISMODULE_REPLY_ARRAY) {
                        size_t inner_reply_length = RedisModule_CallReplyLength(key_reply);
                        if(inner_reply_length == 4) {
                            RedisModuleCallReply *inner_key_reply = RedisModule_CallReplyArrayElement(key_reply, 1);
                            RedisModuleString *arr_elem_key_name = RedisModule_CreateStringFromCallReply(inner_key_reply);
                            const char *arr_elem_key_name_str = RedisModule_StringPtrLen(arr_elem_key_name, NULL);
                            LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "View_Search_Handler pushing key: " + std::string(arr_elem_key_name_str) );
                            view_new_keys.push_back(arr_elem_key_name_str);
                        } else {
                            LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "View_Search_Handler get unexpected (array size is wrong)." );                  
                        }
                    }else {
                        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "View_Search_Handler get unexpected (not an array)." );     
                    }
                }

                //Now get values
                int get_argc = view_new_keys.size();
                RedisModuleString **get_argv = (RedisModuleString **)RedisModule_Alloc(sizeof(RedisModuleString *) * get_argc);
                for (int i = 0; i < get_argc; ++i) {
                    get_argv[i] = RedisModule_CreateString(ctx, view_new_keys[i].c_str(), view_new_keys[i].size());
                }

                RedisModuleCallReply *get_reply = RedisModule_Call(ctx, "JSON.MGET", "vc", get_argv , get_argc, "$");
                if (RedisModule_CallReplyType(get_reply) != REDISMODULE_REPLY_ARRAY) {
                    LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "View_Search_Handler JSON.MGET returned not an array." );
                }
                RedisModule_Free(get_argv);

                // Parse get values
                std::unordered_map<std::string, nlohmann::json> new_values;
                const size_t get_reply_length = RedisModule_CallReplyLength(get_reply);
                if(get_reply_length != view_new_keys.size()) {
                    LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "View_Search_Handler JSON.MGET returned array size is not right. Reply : " +  std::to_string(get_reply_length)  );
                }
                for (size_t i = 0; i < get_reply_length; i++) {  
                    RedisModuleCallReply *get_inner_reply = RedisModule_CallReplyArrayElement(get_reply, i);
                    RedisModuleString *get_reply_elem = RedisModule_CreateStringFromCallReply(get_inner_reply);
                    const char *get_reply_elem_str = RedisModule_StringPtrLen(get_reply_elem, NULL);
                    if (!json::accept(get_reply_elem_str)){
                        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "View_Search_Handler JSON.MGET returned an element which is not valid json" );
                        continue;
                    }
                    std::string value = get_reply_elem_str;
                    if (value.size() > 0 && value[0] == '[' && value[value.size() - 1] == ']') {
                        value = value.substr(1, value.size() - 2);
                    }
                    new_values[view_new_keys[i]] = json::parse(value);
                    LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "View_Search_Handler key: " + std::string(view_new_keys[i]) + " value: " + value );
                }

                std::unordered_map<std::string, nlohmann::json> old_values;
                if(d_h.query_2_value.count(current_query_id) > 0 ) {
                    old_values = d_h.query_2_value[current_query_id];
                }
                std::vector<std::string> view_old_keys;
                if(d_h.query_2_index.count(current_query_id) > 0 ) {
                    view_old_keys = d_h.query_2_index[current_query_id];
                }

                //Compare previous values
                std::vector<View_Diff> diff_values;

                // Iterate new keys from old keys
                long unsigned int diff_index = 0;
                while (diff_index < view_old_keys.size()){
                    //LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "View_Search_Handler: Starting to check old keys" );
                    std::string old_key = view_old_keys[diff_index];
                    std::string new_key = "";
                    if (diff_index < view_new_keys.size()) {
                        new_key = view_new_keys[diff_index];
                    }
                    nlohmann::json old_value =  old_values[old_key];
                    nlohmann::json new_value;
                    if(!new_key.empty()) {
                        new_value = new_values[new_key];
                    }
                    LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "View_Search_Handler: Old key loop : Old key : " + old_key + " , and new key : "  + new_key);
                    LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "View_Search_Handler: Old key loop : Old value : " + old_value.dump() + " , and new value : "  + new_value.dump());
                    if(!new_key.empty() && !old_key.empty()) {
                        if(new_value != old_value) {
                            LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "View_Search_Handler: Keys are same values are different , key: " + new_key );
                            View_Diff current_diff;
                            current_diff.index = diff_index;
                            current_diff.old_key = old_key;
                            current_diff.new_key = new_key;
                            current_diff.old_value = old_value;
                            current_diff.new_value = new_value;
                            current_diff.operation = "UPDATE";
                            current_diff.diff = json::diff(old_value, new_value);
                            diff_values.push_back(current_diff);
                        }else {
                            LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "View_Search_Handler: Keys are same values are same" );
                        }
                    } else if( new_key.empty() && !old_key.empty()) { // Some keys are deleted
                        View_Diff current_diff;
                        current_diff.index = diff_index;
                        current_diff.old_key = old_key;
                        current_diff.new_key = new_key;
                        current_diff.old_value = old_value;
                        current_diff.new_value = new_value;
                        current_diff.operation = "DELETE";
                        diff_values.push_back(current_diff);
                    }
                    diff_index++;
                }

                // If the new key list is bigger than old key list it means we have new keys
                while (diff_index < view_new_keys.size()) {
                    //LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "View_Search_Handler: Starting to check and add new keys" );
                    std::string new_key = view_new_keys[diff_index];
                    nlohmann::json new_value = new_values[new_key];
                    View_Diff current_diff;
                    current_diff.index = diff_index;
                    current_diff.new_key = new_key;
                    current_diff.new_value = new_value;
                    current_diff.operation = "NEW";
                    diff_values.push_back(current_diff);                    
                    diff_index++;
                }

                                // Write new values 
                d_h.query_2_value[current_query_id] = new_values;
                // Write new order 
                d_h.query_2_index[current_query_id] = view_new_keys;

                // Write to stream
                int diff_size = diff_values.size();
                json diff_values_json_array = json::array();
                for(auto &diff : diff_values){
                    diff_values_json_array.push_back(diff.to_json());
                }

                if( diff_size != 0 ) { 
                    int stream_write_size_total = 2 + 2 + 2;
                    RedisModuleString* client_name = RedisModule_CreateString(ctx, current_client.c_str(), current_client.length());
                    RedisModuleKey *stream_key = RedisModule_OpenKey(ctx, client_name, REDISMODULE_WRITE);
                    RedisModuleString **xadd_params = (RedisModuleString **) RedisModule_Alloc(sizeof(RedisModuleString *) * stream_write_size_total);
        
                    xadd_params[0] = RedisModule_CreateString(ctx,"QUERY_ID", strlen("QUERY_ID"));
                    xadd_params[1] = RedisModule_CreateStringFromLongLong(ctx, current_query_id);

        
                    xadd_params[2] = RedisModule_CreateString(ctx,"QUERY", strlen("QUERY"));
                    xadd_params[3] = RedisModule_CreateString(ctx, d_h.id_2_query[current_query_id].c_str(), d_h.id_2_query[current_query_id].length());

                    xadd_params[4] = RedisModule_CreateString(ctx, "DIFF", strlen("DIFF"));
                    xadd_params[5] = RedisModule_CreateString(ctx, diff_values_json_array.dump().c_str(), diff_values_json_array.dump().length());

                    int stream_add_resp = RedisModule_StreamAdd( stream_key, REDISMODULE_STREAM_ADD_AUTOID, NULL, xadd_params, stream_write_size_total/2);
                    if (stream_add_resp != REDISMODULE_OK) {
                        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "View_Search_Handler failed to add to the stream." );
                    }
                }
            }
        }
        // Unregister Clients
        for (auto &unreg_client : d_h.unreg_wait_list) {
            if(d_h.client_2_query.count(unreg_client) > 0) {
                for (auto &id_2_query : d_h.client_2_query[unreg_client]) {
                    d_h.unsub_wait_list.insert(id_2_query.first); // For queries add them to unsubscribe
                }
            }
            d_h.client_2_query.erase(unreg_client);

            // Check if the stream exists for this client
            RedisModuleString* unreg_client_stream_name = RedisModule_CreateString(ctx, unreg_client.c_str(), unreg_client.length());
            if( RedisModule_KeyExists(ctx, unreg_client_stream_name) ) { // NOT checking if it is stream
                RedisModuleKey *stream_key = RedisModule_OpenKey(ctx, unreg_client_stream_name, REDISMODULE_WRITE);
                if (RedisModule_DeleteKey(stream_key) != REDISMODULE_OK ) {
                    LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Unregister failed to delete the stream." ); // Rollback ?
                }
            }
        }
        d_h.unreg_wait_list.clear();

        // Unsubscribe queries
        for (auto &unsub_id : d_h.unsub_wait_list) {
            LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "View_Search_Handler unsubscribing query id :. " + std::to_string(unsub_id) );
            // client2query
            for (auto &client_2_query_item : d_h.client_2_query) {
                client_2_query_item.second.erase(unsub_id);
            }
            // query2value
            d_h.query_2_value.erase(unsub_id);
            // id2query
            d_h.id_2_query.erase(unsub_id);

            d_h.query_2_index.erase(unsub_id);
        }
        d_h.unsub_wait_list.clear();

        // Process Scrolls
        for (auto &scroll_triplet : d_h.scroll_wait_list) {
            long long int query_id = std::get<0>(scroll_triplet);
            long long int offset = std::get<1>(scroll_triplet);
            long long int limit = std::get<2>(scroll_triplet);
            LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "View_Search_Handler scrolling query id :. " + std::to_string(query_id) );
            
            for(auto &c2q : d_h.client_2_query) {
                std::string current_client = c2q.first;
                std::vector<std::string> query_vec = c2q.second[query_id];
                for(long unsigned int arg_index = 0 ; arg_index < query_vec.size() ; arg_index++) {
                    if (query_vec[arg_index] == "LIMIT") {
                        if(query_vec.size() > arg_index + 2){
                            query_vec[arg_index+1] = offset;
                            query_vec[arg_index+2] = limit;
                            d_h.client_2_query[current_client][query_id] = query_vec; // Update global 
                            std::string arguments_string = "";
                            for(auto const& e : query_vec) arguments_string += (e + CCT_MODULE_QUERY_DELIMETER);
                            if(arguments_string.length() > CCT_MODULE_QUERY_DELIMETER.length() ) {
                                arguments_string.erase(arguments_string.length() - CCT_MODULE_QUERY_DELIMETER.length());
                            }
                            d_h.id_2_query[query_id] = arguments_string; // Update global 
                            
                        } else {
                            LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Scroll_RedisCommand skipped for id: ." + std::to_string(query_id) );
                            break;
                        }
                        break;
                    }
                }
            }
        }
        d_h.scroll_wait_list.clear();

        RedisModule_ThreadSafeContextUnlock(ctx);
        std::this_thread::sleep_for(std::chrono::milliseconds(1000)); // Check every second
    }
}
