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

def test_view_search_with_basic_filters():
    producer = connect_redis_with_start()
    flush_db(producer) # clean all db first
    cct_prepare.create_flat_index(producer)

    # ADD INITIAL DATA
    for i in range(20):
        d = cct_prepare.generate_single_flat_object(1000-i , 2000+i, "aaa")
        key = cct_prepare.TEST_INDEX_PREFIX + str(i)
        producer.json().set(key, Path.root_path(), d)

    # FIRST CLIENT
    client1 = connect_redis()
    client1.execute_command("VIEW.REGISTER " + cct_prepare.TEST_APP_NAME_1)
    
    response = client1.execute_command("VIEW.SEARCH " + cct_prepare.TEST_INDEX_NAME + " * SORTBY 1 User_Address_ID FILTER @User_Address_ID>=2002 LIMIT 0 5")
    dictionary = json.loads(response[5][3][3])
    assert dictionary["User_Address_ID"] == "2002"

    response = client1.execute_command("VIEW.SEARCH " + cct_prepare.TEST_INDEX_NAME + " * SORTBY 1 User_Address_ID FILTER @User_Address_ID==2000 LIMIT 0 5")
    dictionary = json.loads(response[5][3][3])
    assert dictionary["User_Address_ID"] == "2000"

    response = client1.execute_command("VIEW.SEARCH " + cct_prepare.TEST_INDEX_NAME + " * SORTBY 1 User_Address_ID FILTER @User_Address_ID<2000 LIMIT 0 5")
    assert response[5] == []

def test_view_search_with_exists_filter():
    producer = connect_redis_with_start()
    flush_db(producer) # clean all db first

    schema = (NumericField("$.field_a", as_name="field_a", sortable=True), TagField("$.field_b", as_name="field_b", sortable=True),  \
              TagField("$.field_c", as_name="field_c", sortable=True), TagField("$.field_d", as_name="field_d", sortable=True) )
    producer.ft(TEST_INDEX_NAME).create_index(schema, definition=IndexDefinition(prefix=[cct_prepare.TEST_INDEX_PREFIX], index_type=IndexType.JSON))

    # ADD INITIAL DATA
    for i in range(4):
        d = {}
        d["field_a"] = (i + 1) + 1000
        d["field_b"] = str(2000 + i -1)
        d["field_c"] = "passport"
        if(i % 2):
            d["field_d"] = "i_exist_in_odd"
        key = cct_prepare.TEST_INDEX_PREFIX + str(i)
        producer.json().set(key, Path.root_path(), d)

    # FIRST CLIENT
    client1 = connect_redis()
    client1.execute_command("VIEW.REGISTER " + cct_prepare.TEST_APP_NAME_1)

    response = client1.execute_command("VIEW.SEARCH " + cct_prepare.TEST_INDEX_NAME + " * SORTBY 1 field_a LIMIT 0 4")
    #print(response)
    dictionary = json.loads(response[5][3][3])
    dictionary.get("field_d") == None
    dictionary = json.loads(response[5][7][3])
    assert dictionary["field_d"] == "i_exist_in_odd"
    
    response = client1.execute_command("VIEW.SEARCH " + cct_prepare.TEST_INDEX_NAME + " * SORTBY 1 field_a FILTER exists(@field_d) LIMIT 0 4")
    #print(response)
    dictionary = json.loads(response[5][3][3])
    assert dictionary["field_d"] == "i_exist_in_odd"
    dictionary = json.loads(response[5][7][3])
    assert dictionary["field_d"] == "i_exist_in_odd"

    response = client1.execute_command("VIEW.SEARCH " + cct_prepare.TEST_INDEX_NAME + " * SORTBY 1 field_a FILTER !exists(@field_d) LIMIT 0 4")
    dictionary = json.loads(response[5][3][3])
    dictionary.get("field_d") == None
    dictionary = json.loads(response[5][7][3])
    dictionary.get("field_d") == None


