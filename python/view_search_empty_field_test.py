from manage_redis import kill_redis, connect_redis_with_start, connect_redis
from manage_redis_data import flush_db, create_index, add_json_data, TEST_INDEX_NAME
import pytest
import cct_prepare
from redis.commands.json.path import Path
from redis.commands.search.query import GeoFilter, NumericFilter, Query
from redis.commands.search.result import Result
from redis.commands.search.aggregation import AggregateRequest
import json
from redis.commands.json.path import Path
from redis.commands.search.field import TextField, NumericField, TagField
from redis.commands.search.indexDefinition import IndexDefinition, IndexType

@pytest.fixture(autouse=True)
def before_and_after_test():
    print("Start")
    yield
    kill_redis()
    print("End")

def test_search_with_empty_value():
    producer = connect_redis_with_start()
    flush_db(producer) # clean all db first

    schema = (NumericField("$.field_a", as_name="field_a", sortable=True), TagField("$.field_b", as_name="field_b", sortable=True),  \
              TagField("$.field_c", as_name="field_c", sortable=True), TagField("$.field_d", as_name="field_d", index_empty=True) )
    
    producer.ft(TEST_INDEX_NAME).create_index(schema, definition=IndexDefinition(prefix=[cct_prepare.TEST_INDEX_PREFIX], index_type=IndexType.JSON))

    # ADD INITIAL DATA
    for i in range(4):
        d = {}
        d["field_a"] = (i + 1) + 1000
        d["field_b"] = str(2000 + i -1)
        d["field_c"] = "passport"
        if i % 2 :
            d["field_d"] = ""
        else:
            d["field_d"] = "not_empty"
        key = cct_prepare.TEST_INDEX_PREFIX + str(i)
        producer.json().set(key, Path.root_path(), d)

    # FIRST CLIENT
    client1 = connect_redis()
    client1.execute_command("VIEW.REGISTER " + cct_prepare.TEST_APP_NAME_1)


    # DIALECT 1
    try:
        response = client1.execute_command("FT.AGGREGATE " + cct_prepare.TEST_INDEX_NAME + " @field_d:{''} SORTBY 2 @field_a ASC DIALECT 1")
    except Exception as e :
        assert str(e) == "Syntax error at offset 12 near field_d"
    try:
        response = client1.execute_command("FT.SEARCH " + cct_prepare.TEST_INDEX_NAME + " @field_d:{''} DIALECT 1")
    except Exception as e :
        assert str(e) == "Syntax error at offset 12 near field_d"

    # DIALECT 2
    response = client1.execute_command("FT.AGGREGATE " + cct_prepare.TEST_INDEX_NAME + " @field_d:{''} SORTBY 2 @field_a ASC DIALECT 2")
    assert str(response) == '''[2, ['field_a', '1002'], ['field_a', '1004']]''' 
    response = client1.execute_command("FT.SEARCH " + cct_prepare.TEST_INDEX_NAME + " @field_d:{''} DIALECT 2")
    assert str(response) == '''[2, 'test_data:1', ['$', '{"field_a":1002,"field_b":"2000","field_c":"passport","field_d":""}'], 'test_data:3', ['$', '{"field_a":1004,"field_b":"2002","field_c":"passport","field_d":""}']]'''

    # DIALECT 3
    response = client1.execute_command("FT.AGGREGATE " + cct_prepare.TEST_INDEX_NAME + " @field_d:{''} SORTBY 2 @field_a ASC DIALECT 3")
    assert str(response) == '''[2, ['field_a', '1002'], ['field_a', '1004']]''' 
    response = client1.execute_command("FT.SEARCH " + cct_prepare.TEST_INDEX_NAME + " @field_d:{''} DIALECT 3")
    assert str(response) == '''[2, 'test_data:1', ['$', '[{"field_a":1002,"field_b":"2000","field_c":"passport","field_d":""}]'], 'test_data:3', ['$', '[{"field_a":1004,"field_b":"2002","field_c":"passport","field_d":""}]']]'''

    # DIALECT 4
    #response = client1.execute_command("FT.AGGREGATE " + cct_prepare.TEST_INDEX_NAME + " @field_d:{''} SORTBY 2 @field_a ASC DIALECT 4")  #RediSearch CRASH!!!!

    response = client1.execute_command("FT.SEARCH " + cct_prepare.TEST_INDEX_NAME + " @field_d:{''} DIALECT 4")
    assert str(response) == '''[1, 'test_data:1', ['$', '[{"field_a":1002,"field_b":"2000","field_c":"passport","field_d":""}]'], 'test_data:3', ['$', '[{"field_a":1004,"field_b":"2002","field_c":"passport","field_d":""}]']]'''
    response = client1.execute_command("FT.AGGREGATE " + cct_prepare.TEST_INDEX_NAME + " @field_d:{''} DIALECT 4")
    assert str(response) == '''[1, [], []]'''
    response = client1.execute_command("FT.AGGREGATE " + cct_prepare.TEST_INDEX_NAME + " @field_d:{''} LOAD 1 @__key DIALECT 4")
    assert str(response) == '''[1, ['__key', 'test_data:1'], ['__key', 'test_data:3']]'''


def test_search_count_with_empty_value():
    producer = connect_redis_with_start()
    flush_db(producer) # clean all db first

    schema = (NumericField("$.field_a", as_name="field_a", sortable=True), TagField("$.field_b", as_name="field_b", sortable=True),  \
              TagField("$.field_c", as_name="field_c", sortable=True), TagField("$.field_d", as_name="field_d", index_empty=True) )
    
    producer.ft(TEST_INDEX_NAME).create_index(schema, definition=IndexDefinition(prefix=[cct_prepare.TEST_INDEX_PREFIX], index_type=IndexType.JSON))

    # ADD INITIAL DATA
    for i in range(400):
        d = {}
        d["field_a"] = (i + 1) + 1000
        d["field_b"] = str(2000 + i -1)
        d["field_c"] = "passport"
        if i % 2 :
            d["field_d"] = ""
        else:
            d["field_d"] = "not_empty"
        key = cct_prepare.TEST_INDEX_PREFIX + str(i)
        producer.json().set(key, Path.root_path(), d)

    # FIRST CLIENT
    client1 = connect_redis()
    client1.execute_command("VIEW.REGISTER " + cct_prepare.TEST_APP_NAME_1)
    response = client1.execute_command("FT.SEARCH " + cct_prepare.TEST_INDEX_NAME + " @field_d:{''} LIMIT 0 0 DIALECT 2")
    assert 200 == response[0]
    response = client1.execute_command("FT.AGGREGATE " + cct_prepare.TEST_INDEX_NAME + " @field_d:{''} LIMIT 0 0 DIALECT 2")
    assert 200 == response[0]

def test_search_with_missing_attribute():
    producer = connect_redis_with_start()
    flush_db(producer) # clean all db first

    schema = (NumericField("$.field_a", as_name="field_a", sortable=True), TagField("$.field_b", as_name="field_b", sortable=True),  \
              TagField("$.field_c", as_name="field_c", sortable=True), TagField("$.field_d", as_name="field_d", index_missing=True) )
    
    producer.ft(TEST_INDEX_NAME).create_index(schema, definition=IndexDefinition(prefix=[cct_prepare.TEST_INDEX_PREFIX], index_type=IndexType.JSON))

    # ADD INITIAL DATA
    for i in range(4):
        d = {}
        d["field_a"] = (i + 1) + 1000
        d["field_b"] = str(2000 + i -1)
        d["field_c"] = "passport"
        if i % 2 :
            d["field_d"] = "not_missing"
        key = cct_prepare.TEST_INDEX_PREFIX + str(i)
        producer.json().set(key, Path.root_path(), d)

    # FIRST CLIENT
    client1 = connect_redis()
    client1.execute_command("VIEW.REGISTER " + cct_prepare.TEST_APP_NAME_1)


    # DIALECT 1
    try:
        response = client1.execute_command("FT.AGGREGATE " + cct_prepare.TEST_INDEX_NAME + " ismissing(@field_d) SORTBY 2 @field_a ASC DIALECT 1")
    except Exception as e :
        assert str(e) == "Syntax error at offset 18 near field_d"
    try:
        response = client1.execute_command("FT.SEARCH " + cct_prepare.TEST_INDEX_NAME + " ismissing(@field_d) DIALECT 1")
    except Exception as e :
        assert str(e) == "Syntax error at offset 18 near field_d"

    # DIALECT 2
    response = client1.execute_command("FT.AGGREGATE " + cct_prepare.TEST_INDEX_NAME + " ismissing(@field_d) SORTBY 2 @field_a ASC DIALECT 2")
    assert str(response) == '''[2, ['field_a', '1001'], ['field_a', '1003']]'''
    response = client1.execute_command("FT.SEARCH " + cct_prepare.TEST_INDEX_NAME + " ismissing(@field_d) DIALECT 2")
    assert str(response) == '''[2, 'test_data:0', ['$', '{"field_a":1001,"field_b":"1999","field_c":"passport"}'], 'test_data:2', ['$', '{"field_a":1003,"field_b":"2001","field_c":"passport"}']]'''

    # DIALECT 3
    response = client1.execute_command("FT.AGGREGATE " + cct_prepare.TEST_INDEX_NAME + " ismissing(@field_d) SORTBY 2 @field_a ASC DIALECT 3")
    assert str(response) == '''[2, ['field_a', '1001'], ['field_a', '1003']]'''
    response = client1.execute_command("FT.SEARCH " + cct_prepare.TEST_INDEX_NAME + " ismissing(@field_d) DIALECT 3")
    assert str(response) == '''[2, 'test_data:0', ['$', '[{"field_a":1001,"field_b":"1999","field_c":"passport"}]'], 'test_data:2', ['$', '[{"field_a":1003,"field_b":"2001","field_c":"passport"}]']]'''

    # DIALECT 4
    #response = client1.execute_command("FT.AGGREGATE " + cct_prepare.TEST_INDEX_NAME + " ismissing(@field_d) SORTBY 2 @field_a ASC DIALECT 4")  #RediSearch CRASH!!!!

    response = client1.execute_command("FT.SEARCH " + cct_prepare.TEST_INDEX_NAME + " ismissing(@field_d) DIALECT 4")
    assert str(response) == '''[1, 'test_data:0', ['$', '[{"field_a":1001,"field_b":"1999","field_c":"passport"}]'], 'test_data:2', ['$', '[{"field_a":1003,"field_b":"2001","field_c":"passport"}]']]'''
    response = client1.execute_command("FT.AGGREGATE " + cct_prepare.TEST_INDEX_NAME + " ismissing(@field_d) DIALECT 4")
    assert str(response) == '''[1, [], []]'''
    response = client1.execute_command("FT.AGGREGATE " + cct_prepare.TEST_INDEX_NAME + " ismissing(@field_d) LOAD 1 @__key DIALECT 4")
    assert str(response) == '''[1, ['__key', 'test_data:0'], ['__key', 'test_data:2']]'''


def test_search_count_with_missing_attribute():
    producer = connect_redis_with_start()
    flush_db(producer) # clean all db first

    schema = (NumericField("$.field_a", as_name="field_a", sortable=True), TagField("$.field_b", as_name="field_b", sortable=True),  \
              TagField("$.field_c", as_name="field_c", sortable=True), TagField("$.field_d", as_name="field_d", index_missing=True) )
    
    producer.ft(TEST_INDEX_NAME).create_index(schema, definition=IndexDefinition(prefix=[cct_prepare.TEST_INDEX_PREFIX], index_type=IndexType.JSON))

    # ADD INITIAL DATA
    for i in range(400):
        d = {}
        d["field_a"] = (i + 1) + 1000
        d["field_b"] = str(2000 + i -1)
        d["field_c"] = "passport"
        if i % 2 :
            d["field_d"] = "not_missing"
        key = cct_prepare.TEST_INDEX_PREFIX + str(i)
        producer.json().set(key, Path.root_path(), d)

    # FIRST CLIENT
    client1 = connect_redis()
    client1.execute_command("VIEW.REGISTER " + cct_prepare.TEST_APP_NAME_1)

    response = client1.execute_command("FT.SEARCH " + cct_prepare.TEST_INDEX_NAME + " ismissing(@field_d) LIMIT 0 0 DIALECT 2")
    assert 200 == response[0]
    response = client1.execute_command("FT.AGGREGATE " + cct_prepare.TEST_INDEX_NAME + " ismissing(@field_d) LIMIT 0 0 DIALECT 2")
    assert 200 == response[0]


def test_view_search_with_empty_value():
    producer = connect_redis_with_start()
    flush_db(producer) # clean all db first

    schema = (NumericField("$.field_a", as_name="field_a", sortable=True), TagField("$.field_b", as_name="field_b", sortable=True),  \
              TagField("$.field_c", as_name="field_c", sortable=True), TagField("$.field_d", as_name="field_d", index_empty=True) )
    
    producer.ft(TEST_INDEX_NAME).create_index(schema, definition=IndexDefinition(prefix=[cct_prepare.TEST_INDEX_PREFIX], index_type=IndexType.JSON))

    # ADD INITIAL DATA
    for i in range(4):
        d = {}
        d["field_a"] = (i + 1) + 1000
        d["field_b"] = str(2000 + i -1)
        d["field_c"] = "passport"
        if i % 2 :
            d["field_d"] = ""
        else:
            d["field_d"] = "not_empty"
        key = cct_prepare.TEST_INDEX_PREFIX + str(i)
        producer.json().set(key, Path.root_path(), d)

    # FIRST CLIENT
    client1 = connect_redis()
    client1.execute_command("VIEW.REGISTER " + cct_prepare.TEST_APP_NAME_1)

    response = client1.execute_command("VIEW.SEARCH " + cct_prepare.TEST_INDEX_NAME + " @field_d:{''} SORTBY 2 field_a ASC LIMIT 0 10 DIALECT 2")
    assert str(response) == '''['query_id', 1, 'count', 2, 'result', ['key', 'test_data:1', 'value', ['path', '$', 'data', '{"field_a":1002,"field_b":"2000","field_c":"passport","field_d":""}'], 'key', 'test_data:3', 'value', ['path', '$', 'data', '{"field_a":1004,"field_b":"2002","field_c":"passport","field_d":""}']]]'''
    
    response = client1.execute_command("VIEW.SEARCH " + cct_prepare.TEST_INDEX_NAME + " @field_d:{''} SORTBY 2 field_b ASC LIMIT 0 10 DIALECT 2")
    assert str(response) == '''['query_id', 2, 'count', 2, 'result', ['key', 'test_data:1', 'value', ['path', '$', 'data', '{"field_a":1002,"field_b":"2000","field_c":"passport","field_d":""}'], 'key', 'test_data:3', 'value', ['path', '$', 'data', '{"field_a":1004,"field_b":"2002","field_c":"passport","field_d":""}']]]'''


def test_view_search_with_missing_attribute():
    producer = connect_redis_with_start()
    flush_db(producer) # clean all db first

    schema = (NumericField("$.field_a", as_name="field_a", sortable=True), TagField("$.field_b", as_name="field_b", sortable=True),  \
              TagField("$.field_c", as_name="field_c", sortable=True), TagField("$.field_d", as_name="field_d", index_missing=True) )
    
    producer.ft(TEST_INDEX_NAME).create_index(schema, definition=IndexDefinition(prefix=[cct_prepare.TEST_INDEX_PREFIX], index_type=IndexType.JSON))

    # ADD INITIAL DATA
    for i in range(4):
        d = {}
        d["field_a"] = (i + 1) + 1000
        d["field_b"] = str(2000 + i -1)
        d["field_c"] = "passport"
        if i % 2 :
            d["field_d"] = "not_missing"
        key = cct_prepare.TEST_INDEX_PREFIX + str(i)
        producer.json().set(key, Path.root_path(), d)

    # FIRST CLIENT
    client1 = connect_redis()
    client1.execute_command("VIEW.REGISTER " + cct_prepare.TEST_APP_NAME_1)

    response = client1.execute_command("VIEW.SEARCH " + cct_prepare.TEST_INDEX_NAME + " ismissing(@field_d) SORTBY 2 field_a ASC LIMIT 0 10 DIALECT 2")
    assert str(response) == '''['query_id', 1, 'count', 2, 'result', ['key', 'test_data:0', 'value', ['path', '$', 'data', '{"field_a":1001,"field_b":"1999","field_c":"passport"}'], 'key', 'test_data:2', 'value', ['path', '$', 'data', '{"field_a":1003,"field_b":"2001","field_c":"passport"}']]]'''
    response = client1.execute_command("VIEW.SEARCH " + cct_prepare.TEST_INDEX_NAME + " ismissing(@field_d) SORTBY 2 field_b ASC LIMIT 0 10 DIALECT 2")
    assert str(response) == '''['query_id', 2, 'count', 2, 'result', ['key', 'test_data:0', 'value', ['path', '$', 'data', '{"field_a":1001,"field_b":"1999","field_c":"passport"}'], 'key', 'test_data:2', 'value', ['path', '$', 'data', '{"field_a":1003,"field_b":"2001","field_c":"passport"}']]]'''
