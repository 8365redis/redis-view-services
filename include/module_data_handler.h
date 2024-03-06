#ifndef MODULE_DATA_HANDLER_H
#define MODULE_DATA_HANDLER_H

#include <string>
#include <vector>
#include <unordered_map>
#include <set>
#include <json.hpp>
#include <tuple>

#include "module_constants.h"

using json = nlohmann::json;

class Data_Handler {
public:
    static Data_Handler& getInstance(){
        static Data_Handler instance; 
        return instance;
    }
private:
    Data_Handler() {}                   
public:    
    Data_Handler(Data_Handler const&) = delete;
    void operator=(Data_Handler const&) = delete;
    std::vector<Client_Size_Info> client_size_info; // Will change to map
    std::unordered_map<std::string, std::unordered_map<long long int, std::vector<std::string>>> client_2_query; // Query is vector of string
    // Value is key to value mapping , Query as a string this time , concentation of parameters with delimeter
    std::unordered_map<long long int, std::unordered_map<std::string, json>> query_2_value; // Query is query id
    std::unordered_map<long long int, std::vector<std::string>> query_2_index; // Query is query id, index of keys in 
    std::unordered_map<long long int, std::string> id_2_query; // ID : QUERY,  Query is string
    std::unordered_map<std::string, unsigned long long> client_2_last_seen; // client to last seen in ms
    std::set<long long int> unsub_wait_list;
    std::set<std::string> unreg_wait_list;
    std::set<std::string> registered_clients; 
    std::vector<std::tuple<long long int, long long int, long long int>> scroll_wait_list;
};

#endif /* MODULE_DATA_HANDLER_H */