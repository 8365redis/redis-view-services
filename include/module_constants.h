#ifndef MODULE_CONSTANTS_H
#define MODULE_CONSTANTS_H

#include <string>
#include <vector>
#include <unordered_map>
#include <json.hpp>
#include <set>
#include <tuple>

using json = nlohmann::json;

typedef struct  {
    std::string stream_name;
    std::vector<std::string> arguments;
} Client_Size_Info;

static long long int LAST_VIEW_SEARCH_IDENTIFIER = 0;

static const int DEFAULT_DIALECT = 4; 

static const std::string CCT_MODULE_QUERY_DELIMETER = "-CCT_DEL-";
static std::vector<Client_Size_Info> CLIENT_SIZE_INFO; // Will change to map
static std::unordered_map<std::string, std::unordered_map<long long int, std::vector<std::string>>> CLIENT_2_QUERY_MAP; // Query is vector of string
// Value is key to value mapping , Query as a string this time , concentation of parameters with delimeter
static std::unordered_map<long long int, std::unordered_map<std::string, json>> QUERY_2_VALUE_MAP; // Query is query id
static std::unordered_map<long long int, std::vector<std::string>> QUERY_2_INDEX_MAP; // Query is query id, index of keys in 
static std::unordered_map<long long int, std::string> ID_2_QUERY_MAP; // ID : QUERY,  Query is string
static std::set<long long int> UNSUBSCRIBE_WAITING_LIST;
static std::vector<std::tuple<long long int, long long int, long long int>> SCROLL_WAITING_LIST;

#endif /* MODULE_CONSTANTS_H */