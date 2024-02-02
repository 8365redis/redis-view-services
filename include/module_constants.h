#ifndef MODULE_CONSTANTS_H
#define MODULE_CONSTANTS_H

#include <string>
#include <vector>
#include <unordered_map>
#include <json.hpp>

using json = nlohmann::json;

typedef struct  {
    std::string stream_name;
    std::vector<std::string> arguments;
} Client_Size_Info;

static const std::string CCT_MODULE_QUERY_DELIMETER = "-CCT_DEL-";
static std::vector<Client_Size_Info> CLIENT_SIZE_INFO; // Will change to map
static std::unordered_map<std::string, std::vector<std::string>> CLIENT_2_QUERY_MAP; // Query is vector of string
// Value is key to value mapping , Query as a string this time , concentation of parameters with delimeter
static std::unordered_map<std::string, std::unordered_map<std::string, json>> QUERY_2_VALUE_MAP; 

#endif /* MODULE_CONSTANTS_H */