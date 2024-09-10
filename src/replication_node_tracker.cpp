#include <sstream>
#include <iostream>
#include <string>
#include <chrono>
#include <vector>
#include <thread>
#include "replication_node_tracker.h"
#include "logger.h"

std::string Parse_Info_For_Port(const std::string& input) {
    std::string key = "tcp_port:";
    std::size_t pos = input.find(key);
    
    if (pos != std::string::npos) {
        // Find the position after "tcp_port:"
        pos += key.length();
        
        // Extract the value after "tcp_port:" until the next non-digit character
        std::string port;
        while (pos < input.length() && isdigit(input[pos])) {
            port += input[pos];
            ++pos;
        }
        return port;
    }

    return "";  // Return an empty string if "tcp_port:" is not found
}

void Parse_Info_For_Slave(const std::string& input, int& slaveCount, std::unordered_map<std::string, IP_Address_Port> &replication_nodes_search) {
    std::string slavePrefix = "slave";
    slaveCount = 0;

    std::size_t pos = 0;

    // Loop to find all occurrences of "slave" and extract the information
    while ((pos = input.find(slavePrefix + std::to_string(slaveCount), pos)) != std::string::npos) {
        // Increment slave count
        ++slaveCount;

        // Find the IP address
        std::size_t ipPos = input.find("ip=", pos);
        std::size_t ipEnd = input.find(",", ipPos);
        std::string ip = input.substr(ipPos + 3, ipEnd - (ipPos + 3));

        // Find the port
        std::size_t portPos = input.find("port=", ipEnd);
        std::size_t portEnd = input.find(",", portPos);
        std::string port_str = input.substr(portPos + 5, portEnd - (portPos + 5));

        // Store the slave information
        int port = 0;
        try { 
            port = std::stoi(port_str);
        } catch (std::invalid_argument const& ex) {
            port = 0;
        }
        IP_Address_Port ip_port = IP_Address_Port(ip, port);
        std::string current_key = ip_port.get_key();
        replication_nodes_search[current_key] = ip_port;
        // Move the position forward for the next iteration
        pos = portEnd;
    }
}


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

    int slave_count = 0;
    replication_nodes_search.clear();
    Parse_Info_For_Slave(info_replication, slave_count, replication_nodes_search);
    std::string replication_nodes_str;
    for(const auto &k_v : replication_nodes_search){
        replication_nodes_str = replication_nodes_str + " , " + k_v.first ;
    }
    //LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "Get_Replication_Nodes current slaves : " +  replication_nodes_str );
}

int Get_Master_Redis_Port(RedisModuleCtx *ctx) {
    RedisModuleCallReply *info_reply = RedisModule_Call(ctx, "INFO", "c", "server");
    if (info_reply == NULL || RedisModule_CallReplyType(info_reply) == REDISMODULE_REPLY_ERROR) {
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Get_Master_Redis_Port INFO server has failed. Call failed." );
        return 0;
    }
    if (RedisModule_CallReplyType(info_reply) != REDISMODULE_REPLY_STRING) {
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Get_Master_Redis_Port INFO server has failed. Not string." );
        return 0;
    }
    RedisModuleString *response = RedisModule_CreateStringFromCallReply(info_reply);
    std::string info_server_str = RedisModule_StringPtrLen(response, NULL);
    std::string port_str = Parse_Info_For_Port(info_server_str);
    LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "Get_Master_Redis_Port INFO got port number : " +  port_str );
    int port = 0;
    try { 
        port = std::stoi(port_str);
    } catch (std::invalid_argument const& ex) {
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Handler_Forward_Request given port is invalid");
        return 0;
    }
    return port;
}