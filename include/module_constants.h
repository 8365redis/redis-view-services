#ifndef MODULE_CONSTANTS_H
#define MODULE_CONSTANTS_H

#include <string>
#include <vector>
#include <unordered_map>
#include "json.hpp"
#include <set>
#include <tuple>

using json = nlohmann::json;

typedef struct  {
    std::string stream_name;
    std::vector<std::string> arguments;
} Client_Size_Info;

static long long int LAST_VIEW_SEARCH_IDENTIFIER = 0;

static const int DEFAULT_DIALECT = 4; 

static const std::string CCT_MODULE_QUERY_DELIMETER = " ";

static const int CLIENT_TIMEOUT = 60;  // seconds

#endif /* MODULE_CONSTANTS_H */