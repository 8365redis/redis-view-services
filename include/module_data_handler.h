#ifndef MODULE_DATA_HANDLER_H
#define MODULE_DATA_HANDLER_H

#include <string>
#include <vector>
#include <unordered_map>
#include <set>
#include <tuple>
#include <atomic>
#include "json/json.hpp"
#include "module_constants.h"


using json = nlohmann::json;

class IP_Address_Port {
private:
    std::string ip;
    int port;
    std::string key;
public:
    IP_Address_Port(const std::string& ip, const int port) : ip(ip), port(port) { key = ip + ":" + std::to_string(port); }
    IP_Address_Port() : ip(""), port(0) { key = ip + ":" + std::to_string(port); }
    IP_Address_Port(const IP_Address_Port& other) : ip(other.ip), port(other.port), key(other.key) {}
    IP_Address_Port(IP_Address_Port&& other) noexcept : ip(std::move(other.ip)), port(std::move(other.port)), key(std::move(other.key)) {}

    IP_Address_Port& operator=(const IP_Address_Port& other) {
        if (this != &other) {
            ip = other.ip;
            port = other.port;
            key = other.key;
        }
        return *this;
    }

    IP_Address_Port& operator=(IP_Address_Port&& other) noexcept {
        if (this != &other) {
            ip = std::move(other.ip);
            port = std::move(other.port);
            key = std::move(other.key);
        }
        return *this;
    }

    std::string get_IP() const { return ip; }
    int get_port() const { return port; }
    std::string get_key() const { return key; }
};


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
    std::unordered_map<std::string, std::unordered_map<unsigned long, Client_Size_Info>> client_size_info; 
    std::unordered_map<std::string, std::unordered_map<long long int, std::vector<std::string>>> client_2_query; // Query is vector of string
    // Value is key to value mapping , Query as a string this time , concentation of parameters with delimeter
    std::unordered_map<long long int, std::unordered_map<std::string, json>> query_2_value; // Query is query id
    std::unordered_map<long long int, std::unordered_map<std::string, json>> query_2_value_buffer; // Query is query id
    std::unordered_map<long long int, std::vector<std::string>> query_2_index; // Query is query id, index of keys in
    std::unordered_map<long long int, std::vector<std::string>> query_2_index_buffer; // Query is query id, index of keys in 
    std::unordered_map<long long int, std::string> id_2_query; // ID : QUERY,  Query is string
    std::unordered_map<std::string, unsigned long long> client_2_last_seen; // client to last seen in ms
    std::unordered_map<std::string, std::string> index_attribute_2_type; // for example a.b = TAG , a.c = TEXT , a.c.d = NUMERIC
    std::set<long long int> unsub_wait_list;
    std::vector<Client_Size_Info> unsub_count_wait_list;
    std::set<std::string> unreg_search_wait_list;
    std::set<std::string> unreg_count_wait_list;
    std::set<std::string> registered_clients; 
    std::unordered_map<long long int, std::tuple<long long int, long long int, long long int>> scroll_wait_list;
    std::unordered_map<long long int, std::tuple<long long int, long long int, long long int>> scroll_wait_list_buffer;
    std::unordered_map<long long int, std::string> id_2_innerkey_prefix; // Query ID : innerkey prefix if given

    IP_Address_Port master_node;
    std::atomic<bool> search_handler_running; 
};

#endif /* MODULE_DATA_HANDLER_H */