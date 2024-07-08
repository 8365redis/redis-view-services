#ifndef MODULE_CONSTANTS_H
#define MODULE_CONSTANTS_H

#include <string>
#include <vector>
#include <unordered_map>
#include "json.hpp"
#include <set>
#include <tuple>

using json = nlohmann::json;

struct Client_Size_Info {
    std::string stream_name;
    std::vector<std::string> arguments;
    long long int count;
    std::size_t hash_value; // Add a hash value field


    Client_Size_Info() : count(0), hash_value(0) {}

    // Constructor to initialize and compute the hash value
    Client_Size_Info(const std::string& name, const std::vector<std::string>& args, long long int cnt)
        : stream_name(name), arguments(args), count(cnt) {
        hash_value = computeHash();
    }

    std::size_t computeHash() const {
        std::size_t hash = std::hash<std::string>{}(stream_name);
        for (const auto& arg : arguments) {
            hash ^= std::hash<std::string>{}(arg) + 0x9e3779b9 + (hash << 6) + (hash >> 2);
        }
        return hash;
    }

    // Comparison operator using the hash value
    bool operator==(const Client_Size_Info& other) const {
        return hash_value == other.hash_value;
    }

    // Function to update the hash value
    void updateHash() {
        hash_value = computeHash();
    }
};

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

const std::string COUNT_QUERY_STR = "count_query";
const std::string COUNT_STR = "count";

#endif /* MODULE_CONSTANTS_H */