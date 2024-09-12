#ifndef MODULE_CONSTANTS_H
#define MODULE_CONSTANTS_H

#include <string>
#include <vector>
#include <unordered_map>
#include <set>
#include <tuple>
#include "json/json.hpp"

using json = nlohmann::json;

struct Client_Size_Info {
    std::string stream_name;
    std::vector<std::string> arguments;
    long long int count;
    std::size_t hash_value;
    std::size_t id;


    Client_Size_Info() : count(0), hash_value(0), id(0) {}
    Client_Size_Info(const Client_Size_Info& other) : stream_name(other.stream_name), arguments(other.arguments), count(other.count), hash_value(other.hash_value), id(other.id) {}
    Client_Size_Info(Client_Size_Info&& other) noexcept : stream_name(std::move(other.stream_name)), arguments(std::move(other.arguments)), count(std::move(other.count)), hash_value(std::move(other.hash_value)), id(std::move(other.id)) {}

    // Constructor to initialize and compute the hash value
    Client_Size_Info(const std::string& name, const std::vector<std::string>& args, long long int cnt, std::size_t i)
        : stream_name(name), arguments(args), count(cnt), id(i) {
        hash_value = computeHash();
    }

    std::size_t computeHash() const {
        std::size_t hash = 0;
        for (const auto& arg : arguments) {
            hash ^= std::hash<std::string>{}(arg) + 0x9e3779b9 + (hash << 6) + (hash >> 2);
        }
        return hash;
    }

    // Comparison operator using the hash value
    bool operator==(const Client_Size_Info& other) const {
        return hash_value == other.hash_value;
    }

};

const std::string DEFAULT_MASTER_IP = "127.0.0.1";
const int DEFAULT_MASTER_PORT = 6379;


static const int NANO_2_MILI = 1000 * 1000;

static const int SEARCH_HANDLER_DEFAULT_FREQUENCY = 1000;
static const int SEARCH_HANDLER_MIN_FREQUENCY = 100;
static const int COUNT_HANDLER_DEFAULT_FREQUENCY = 1000;
static const int COUNT_HANDLER_MIN_FREQUENCY = 100;

const std::string MONITORING_DISABLED_CONF_STR = "NO_MONITORING";
const std::string MONITORING_RETENTION_CONF_STR = "MONITORING_RETENTION";

static const int DEFAULT_DIALECT = 4;

static const std::string CCT_MODULE_QUERY_DELIMETER = " ";

static const std::string MONITORING_TS_DELIMETER = ":";

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
const std::string COUNT_QUERY_ID_STR = "count_query_id";

const std::string IMPLICIT_KEY_NAME = "@__key";
const std::string IMPLICIT_KEY_NAME_SIMPLE = "__key";

#endif /* MODULE_CONSTANTS_H */