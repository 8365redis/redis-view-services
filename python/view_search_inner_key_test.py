from manage_redis import kill_redis, connect_redis_with_start, connect_redis
from manage_redis_data import flush_db, create_index, add_json_data, TEST_INDEX_NAME
import pytest
import cct_prepare
from redis.commands.json.path import Path
from redis.commands.search.query import GeoFilter, NumericFilter, Query
from redis.commands.search.field import TextField, NumericField, TagField
from redis.commands.search.indexDefinition import IndexDefinition, IndexType
from redis.commands.search.result import Result
import time

@pytest.fixture(autouse=True)
def before_and_after_test():
    print("Start")
    yield
    kill_redis()
    print("End")

def test_no_inner_key():
    producer = connect_redis_with_start()
    flush_db(producer) # clean all db first
    cct_prepare.create_index(producer)

    # ADD INITIAL DATA
    for i in range(20):
        d = cct_prepare.generate_single_object(1000 , 2000-i, "aaa")
        key = cct_prepare.TEST_INDEX_PREFIX + str(1 + i)
        producer.json().set(key, Path.root_path(), d)
    
    # FIRST CLIENT
    client1 = connect_redis()
    client1.execute_command("VIEW.REGISTER app1")

    response = client1.execute_command("VIEW.SEARCH test_index * SORTBY User.ID DESC LIMIT 0 3")
    print("\nVIEW.SEARCH RESPONSE : " + str(response))
    query_id = int(response[1])
    assert query_id == 1
    time.sleep(2)

def test_no_inner_key_with_scroll():
    producer = connect_redis_with_start()
    flush_db(producer) # clean all db first
    cct_prepare.create_index(producer)

    # ADD INITIAL DATA
    for i in range(20):
        d = cct_prepare.generate_single_object(1000 , 2000-i, "aaa")
        key = cct_prepare.TEST_INDEX_PREFIX + str(1 + i)
        producer.json().set(key, Path.root_path(), d)
    
    # FIRST CLIENT
    client1 = connect_redis()
    client1.execute_command("VIEW.REGISTER app1")

    response = client1.execute_command("VIEW.SEARCH test_index * SORTBY User.ID DESC LIMIT 0 3")
    print("\nVIEW.SEARCH RESPONSE : " + str(response))
    query_id = int(response[1])
    assert query_id == 1

    response = client1.execute_command(f"VIEW.SEARCH.SCROLL 1 3 3")
    print("\VIEW.SEARCH.SCROLL RESPONSE : " + str(response))

    #response = client1.execute_command(f"VIEW.SEARCH.SCROLL 1 6 3")
    #print("\VIEW.SEARCH.SCROLL RESPONSE : " + str(response))
    time.sleep(2)

    response = client1.execute_command(f"VIEW.SEARCH.SCROLL 1 6 3")
    print("\VIEW.SEARCH.SCROLL RESPONSE : " + str(response))

def test_no_inner_key_with_scroll_with_different_data_type():
    producer = connect_redis_with_start()
    flush_db(producer) # clean all db first 
    schema = (NumericField("$.aaa", as_name="aaa", sortable=True), TagField("$.bbb", as_name="bbb", sortable=True),  \
              TextField("$.ccc", as_name="ccc", sortable=True) )
    producer.ft(TEST_INDEX_NAME).create_index(schema, definition=IndexDefinition(prefix=[cct_prepare.TEST_INDEX_PREFIX], index_type=IndexType.JSON)) 

    # ADD INITIAL DATA
    for i in range(20):
        d = {}
        d["aaa"] = 100-i
        d["bbb"] = str(100+i)
        d["ccc"] = "ccc"
        key = cct_prepare.TEST_INDEX_PREFIX + str(1 + i)
        producer.json().set(key, Path.root_path(), d)

        # FIRST CLIENT
    client1 = connect_redis()
    client1.execute_command("VIEW.REGISTER app1")

    #QUERY
    response = client1.execute_command("VIEW.SEARCH test_index * SORTBY aaa DESC LIMIT 0 3")
    print("VIEW.SEARCH RESPONSE (aaa) : " + str(response))
    response = client1.execute_command(f"VIEW.SEARCH.SCROLL 1 3 3")
    print("VIEW.SEARCH.SCROLL RESPONSE (aaa): " + str(response))
    time.sleep(1)
    #QUERY
    response = client1.execute_command("VIEW.SEARCH test_index * SORTBY bbb DESC LIMIT 0 3")
    print("\nVIEW.SEARCH RESPONSE (bbb) : " + str(response))
    response = client1.execute_command(f"VIEW.SEARCH.SCROLL 2 3 3")
    print("VIEW.SEARCH.SCROLL RESPONSE (bbb): " + str(response))
    time.sleep(1)
    #QUERY
    response = client1.execute_command("VIEW.SEARCH test_index * SORTBY ccc DESC LIMIT 0 3")
    print("\nVIEW.SEARCH RESPONSE (ccc) : " + str(response))
    response = client1.execute_command(f"VIEW.SEARCH.SCROLL 3 3 3")
    print("VIEW.SEARCH.SCROLL RESPONSE (ccc): " + str(response))
    

def test_with_inner_key():
    producer = connect_redis_with_start()
    flush_db(producer) # clean all db first
    cct_prepare.create_index(producer)

    # ADD INITIAL DATA
    for i in range(20):
        key = cct_prepare.TEST_INDEX_PREFIX + str(1 + i)
        d = cct_prepare.generate_single_object(1000 , key, "aaa")
        producer.json().set(key, Path.root_path(), d)
    
    # FIRST CLIENT
    client1 = connect_redis()
    client1.execute_command("VIEW.REGISTER app1")

    response = client1.execute_command("VIEW.SEARCH test_index * SORTBY User.ID DESC LIMIT 0 3 INNERKEY User.Address.ID")
    print("\nVIEW.SEARCH RESPONSE 1: " + str(response))
    query_id = int(response[1])
    assert query_id == 1
    time.sleep(2)

    response = client1.execute_command("VIEW.SEARCH test_index * SORTBY User.ID DESC LIMIT 3 5 INNERKEY User.Address.ID")
    print("\nVIEW.SEARCH RESPONSE 2: " + str(response))

def test_wrong_inner_key():
    producer = connect_redis_with_start()
    flush_db(producer) # clean all db first
    cct_prepare.create_index(producer)

    # ADD INITIAL DATA
    for i in range(20):
        key = cct_prepare.TEST_INDEX_PREFIX + str(1 + i)
        d = cct_prepare.generate_single_object(1000 , key, "aaa")
        producer.json().set(key, Path.root_path(), d)
    
    # FIRST CLIENT
    client1 = connect_redis()
    client1.execute_command("VIEW.REGISTER app1")

    response = client1.execute_command("VIEW.SEARCH test_index * SORTBY User.ID DESC LIMIT 0 3 INNERKEY not.valid.inner.key")
    print("\nNOT VALID INNERKEY : " + str(response))
    query_id = int(response[1])
    assert query_id == 1

    time.sleep(2)

    response = client1.execute_command("VIEW.SEARCH test_index * SORTBY User.ID DESC LIMIT 0 3")
    print("\nDEFAULT                : " + str(response))
    query_id = int(response[1])
    assert query_id == 2

    time.sleep(1)

    response = client1.execute_command("VIEW.SEARCH test_index * SORTBY User.ID DESC LIMIT 5 5")
    print("\nDEFAULT                : " + str(response))

def test_sortby_and_inner_key_same():
    producer = connect_redis()
    flush_db(producer) # clean all db first
    cct_prepare.create_index(producer)

    # ADD INITIAL DATA
    for i in range(20):
        key = cct_prepare.TEST_INDEX_PREFIX + str(1 + i)
        d = cct_prepare.generate_single_object(1000 , key, "aaa")
        producer.json().set(key, Path.root_path(), d)

    # FIRST CLIENT
    client1 = connect_redis()
    client1.execute_command("VIEW.REGISTER app1")

    response = client1.execute_command("VIEW.SEARCH test_index * SORTBY User.ID DESC LIMIT 0 3 INNERKEY User.Address.ID")
    print("\nVIEW.SEARCH RESPONSE 1: " + str(response))

    time.sleep(1.1)

    response = client1.execute_command("VIEW.SEARCH test_index * SORTBY User.Address.ID DESC LIMIT 0 3 INNERKEY User.Address.ID")
    print("\nVIEW.SEARCH RESPONSE 2: " + str(response))

    time.sleep(1.1)

    response = client1.execute_command("VIEW.SEARCH test_index * SORTBY User.Address.ID DESC LIMIT 0 3")
    print("\nVIEW.SEARCH RESPONSE 3: " + str(response))