import random
import string
import json
from json import JSONEncoder
from redis.commands.json.path import Path
from redis.commands.search.field import TextField, NumericField, TagField
from redis.commands.search.indexDefinition import IndexDefinition, IndexType

TEST_INDEX_NAME = "test_index"
TEST_INDEX_PREFIX = "test_data:"

def flush_db(r):
    r.flushall()

def add_json_data(r, count, size):
    for i in range(count):
        d = {}
        d["base"] = {}
        for j in range(size):
            if(j == 1):
                d["base"]["data1"] = "data1"
            elif(j == 2):
                d["base"]["data2"] = "data2"
            else:
                d["base"]["data" + str(j)] = "data-" + str(i) +"-"+ str(j)
        r.json().set(TEST_INDEX_PREFIX + str(i), Path.root_path(), d)
    

def create_index(r):
    schema = (TagField("$.base.data1", as_name="base.data1"), TagField("$.base.data2", as_name="base.data2"))
    r.ft(TEST_INDEX_NAME).create_index(schema, definition=IndexDefinition(prefix=[TEST_INDEX_PREFIX], index_type=IndexType.JSON))
