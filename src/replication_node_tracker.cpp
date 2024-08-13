#include <sstream>
#include <regex>
#include <iostream>
#include <string>
#include <chrono>
#include <vector>
#include <thread>
#include "replication_node_tracker.h"
#include "logger.h"


void Get_Replication_Nodes(RedisModuleCtx *ctx, std::unordered_map<std::string, IP_Address_Port> &replication_nodes_search) {
    //LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Get_Replication_Nodes called." );
    RedisModuleCallReply *info_reply = RedisModule_Call(ctx, "INFO", "c", "replication");
    if (info_reply == NULL || RedisModule_CallReplyType(info_reply) == REDISMODULE_REPLY_ERROR) {
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Get_Replication_Nodes INFO replication has failed. Call failed." );
        return;
    }

    if (RedisModule_CallReplyType(info_reply) != REDISMODULE_REPLY_STRING) {
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Get_Replication_Nodes INFO replication has failed. Not string." );
        return;
    }    

    RedisModuleString *response = RedisModule_CreateStringFromCallReply(info_reply);
    const char *info_replication_str = RedisModule_StringPtrLen(response, NULL);
    std::string info_replication = info_replication_str;
    std::istringstream info_replication_stream(info_replication);

    std::regex pattern(R"(slave\d+:ip=([\d\.]+),port=(\d+),state=(\w+))");
    std::smatch matches;
    std::string::const_iterator searchStart(info_replication.cbegin());
    replication_nodes_search.clear();
    while (std::regex_search(searchStart, info_replication.cend(), matches, pattern)) {
        std::string ip = matches[1];
        std::string port = matches[2];
        std::string state = matches[3];
        if (state == "online") {
            IP_Address_Port ip_port = IP_Address_Port(ip, port);
            //LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "Get_Replication_Nodes INFO replication has added a new node from info: " +  ip_port.get_key());
            std::string current_key = ip_port.get_key();
            replication_nodes_search[current_key] = ip_port;

        }
        searchStart = matches.suffix().first;
    }
}