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
    long long int count;
} Client_Size_Info;

static const int DEFAULT_DIALECT = 4; 

static const std::string CCT_MODULE_QUERY_DELIMETER = " ";

static const int CLIENT_TIMEOUT = 600;  // seconds

const std::string REPLY_DATA_ROOT_STR = "$";

const std::string REPLY_QUERY_ID_STR = "query_id";
const std::string REPLY_COUNT_STR = "count";
const std::string REPLY_RESULT_STR = "result";
const std::string REPLY_KEY_STR = "key";
const std::string REPLY_VALUE_STR = "value";
const std::string REPLY_PATH_STR = "path";
const std::string REPLY_DATA_STR = "data";

const std::string OPERATION_NEW = "NEW";
const std::string OPERATION_UPDATE = "UPDATE";
const std::string OPERATION_DELETE = "DELETE";

const std::string STREAM_CLIENT_ID_STR = "client_id";
const std::string STREAM_QUERY_ID_STR = "query_id";
const std::string STREAM_QUERY_STR = "query";
const std::string STREAM_DIFF_STR = "diff";

#endif /* MODULE_CONSTANTS_H */