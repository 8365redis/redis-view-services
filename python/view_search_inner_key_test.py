from manage_redis import kill_redis, connect_redis_with_start, connect_redis
from manage_redis_data import flush_db, create_index, add_json_data, TEST_INDEX_NAME
import pytest
import cct_prepare
from redis.commands.json.path import Path
from redis.commands.search.query import GeoFilter, NumericFilter, Query
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
    print("\nVIEW.SEARCH RESPONSE : " + str(response))
    query_id = int(response[1])
    assert query_id == 1

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

    response = client1.execute_command("VIEW.SEARCH test_index * SORTBY User.ID DESC LIMIT 0 3")
    print("\nDEFAULT            : " + str(response))
    query_id = int(response[1])
    assert query_id == 2