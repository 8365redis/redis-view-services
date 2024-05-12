from manage_redis import kill_redis, connect_redis_with_start, connect_redis
from manage_redis_data import flush_db, create_index, add_json_data, TEST_INDEX_NAME
import pytest
import cct_prepare
from redis.commands.json.path import Path
from redis.commands.search.query import GeoFilter, NumericFilter, Query
from redis.commands.search.result import Result
from redis.commands.search.field import TextField, NumericField, TagField
from redis.commands.search.indexDefinition import IndexDefinition, IndexType

@pytest.fixture(autouse=True)
def before_and_after_test():
    print("Start")
    yield
    kill_redis()
    print("End")

def test_same_query_result():
    producer = connect_redis_with_start()
    flush_db(producer) # clean all db first
    cct_prepare.create_index(producer)

    # ADD INITIAL DATA
    for i in range(100):
        d = cct_prepare.generate_single_object(1000 , 2000 + i, "aaa")
        key = cct_prepare.TEST_INDEX_PREFIX + str(i)
        producer.json().set(key, Path.root_path(), d)

    # FIRST CLIENT
    client1 = connect_redis()
    client1.execute_command("VIEW.REGISTER " + cct_prepare.TEST_APP_NAME_1)
    query = "VIEW.SEARCH " + cct_prepare.TEST_INDEX_NAME + " * " + "SORTBY User.ID LIMIT 0 3"
    response1 = client1.execute_command(query)
    response1.pop(0)
    #print(response1)
    response2 = client1.execute_command(query)
    response2.pop(0)
    #print(response2)

    assert response1 == response2

    #UPDATE DATA 
    d = cct_prepare.generate_single_object(1000  , 2001, "aaa")
    key = cct_prepare.TEST_INDEX_PREFIX + str(0)
    producer.json().set(key, Path.root_path(), d)

    response3 = client1.execute_command(query)
    response3.pop(0)
    #print(response3)
    response4 = client1.execute_command(query)
    response4.pop(0)
    #print(response4)

    assert response3 == response4

    response5 = client1.execute_command(query)
    response5.pop(0)
    #UPDATE DATA 
    d = cct_prepare.generate_single_object(1000  , 2001, "aaa")
    key = cct_prepare.TEST_INDEX_PREFIX + str(1)
    producer.json().set(key, Path.root_path(), d)
    response6 = client1.execute_command(query)
    response6.pop(0)

    assert response5 == response6

def test_same_query_with_changed_dataset():
    producer = connect_redis_with_start()
    flush_db(producer) # clean all db first
    cct_prepare.create_index(producer)

    # ADD INITIAL DATA
    for i in range(100):
        d = cct_prepare.generate_single_object(1000 , 2000 + i, "aaa")
        key = cct_prepare.TEST_INDEX_PREFIX + str(i)
        producer.json().set(key, Path.root_path(), d)

def test_aggragate_sortby_ordering():
    producer = connect_redis_with_start()
    flush_db(producer) # clean all db first 
    schema = (NumericField("$.aaa", as_name="aaa", sortable=True), TagField("$.bbb", as_name="bbb", sortable=True),  \
              TagField("$.ccc", as_name="ccc", sortable=True) )
    producer.ft(TEST_INDEX_NAME).create_index(schema, definition=IndexDefinition(prefix=[cct_prepare.TEST_INDEX_PREFIX], index_type=IndexType.JSON)) 

    # ADD INITIAL DATA
    for i in range(20):
        d = {}
        d["aaa"] = 100-i
        d["bbb"] = str(100+i)
        d["ccc"] = "ccc"
        key = cct_prepare.TEST_INDEX_PREFIX + str(1 + i)
        producer.json().set(key, Path.root_path(), d)
    
    query = "FT.AGGREGATE test_index * LOAD 1 @__key SORTBY 4 @aaa ASC @__key ASC LIMIT 0 3"
    response = producer.execute_command("FT.AGGREGATE test_index * LOAD 1 @__key SORTBY 4 @aaa ASC @__key ASC LIMIT 0 3")
    print("Query : " + query)
    print("LOAD RESPONSE : " + str(response))

    query = "FT.AGGREGATE test_index * SORTBY 4 @aaa ASC @bbb ASC LIMIT 0 3"
    response = producer.execute_command(query)
    print("\nQuery : " + query)
    print("INNER KEY RESPONSE : " + str(response))

    query = "FT.AGGREGATE test_index * SORTBY 4 @bbb ASC @aaa ASC LIMIT 0 3"
    response = producer.execute_command(query)
    print("\nQuery : " + query)
    print("INNER KEY RESPONSE : " + str(response))

    query = "FT.AGGREGATE test_index * SORTBY 4 @ccc ASC @aaa ASC LIMIT 0 3"
    response = producer.execute_command(query)
    print("\nQuery : " + query)
    print("INNER KEY RESPONSE : " + str(response))

    query = "FT.AGGREGATE test_index * SORTBY 4 @aaa ASC @ccc ASC LIMIT 0 3"
    response = producer.execute_command(query)
    print("\nQuery : " + query)
    print("INNER KEY RESPONSE : " + str(response))

