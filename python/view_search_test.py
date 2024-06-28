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

def test_view_search_return_single():
    producer = connect_redis_with_start()
    flush_db(producer) # clean all db first
    cct_prepare.create_index(producer)

    # ADD INITIAL DATA
    d = cct_prepare.generate_single_object(1000 , 2000, "aaa")
    key = cct_prepare.TEST_INDEX_PREFIX + str(1)
    producer.json().set(key, Path.root_path(), d)

    # FIRST CLIENT
    client1 = connect_redis()
    client1.execute_command("VIEW.REGISTER " + cct_prepare.TEST_APP_NAME_1)
    response = client1.execute_command("VIEW.SEARCH " + cct_prepare.TEST_INDEX_NAME + " @User\\.PASSPORT:{" + "aaa" + "}" + cct_prepare.QUERY_FULL_POSTFIX)
    print(str(response))

    query_id = int(response[1])
    assert query_id == 1
    response_size = response[5].count('key')
    assert response_size == 1

def test_view_search_return_multi():
    producer = connect_redis_with_start()
    flush_db(producer) # clean all db first
    cct_prepare.create_index(producer)

    # ADD INITIAL DATA
    for i in range(20):
        d = cct_prepare.generate_single_object(1000-i , 2000+i, "aaa")
        key = cct_prepare.TEST_INDEX_PREFIX + str(i)
        producer.json().set(key, Path.root_path(), d)

    # FIRST CLIENT
    client1 = connect_redis()
    client1.execute_command("VIEW.REGISTER " + cct_prepare.TEST_APP_NAME_1)
    response = client1.execute_command("VIEW.SEARCH " + cct_prepare.TEST_INDEX_NAME + " * SORTBY 1 User.ID LIMIT 0 3")
    print(str(response))

    query_id = int(response[1])
    assert query_id == 1
    response_size = response[5].count('key')
    assert response_size == 3

def test_view_search_multi_sort_1():
    producer = connect_redis_with_start()
    flush_db(producer) # clean all db first
    cct_prepare.create_index(producer)

    # ADD INITIAL DATA
    for i in range(20):
        d = cct_prepare.generate_single_object(1000-i , 2000+i, "aaa")
        key = cct_prepare.TEST_INDEX_PREFIX + str(i)
        producer.json().set(key, Path.root_path(), d)

    # FIRST CLIENT
    client1 = connect_redis()
    client1.execute_command("VIEW.REGISTER " + cct_prepare.TEST_APP_NAME_1)
    response = client1.execute_command("VIEW.SEARCH " + cct_prepare.TEST_INDEX_NAME + " * SORTBY 2 User.ID User.PASSPORT LIMIT 0 2")
    print(str(response))

    query_id = int(response[1])
    assert query_id == 1
    response_size = response[5].count('key')
    assert response_size == 2

def test_view_search_multi_sort_2():
    producer = connect_redis_with_start()
    flush_db(producer) # clean all db first
    cct_prepare.create_index(producer)

    # ADD INITIAL DATA
    for i in range(20):
        d = cct_prepare.generate_single_object(1000-i , 2000+i, "aaa")
        key = cct_prepare.TEST_INDEX_PREFIX + str(i)
        producer.json().set(key, Path.root_path(), d)

    # FIRST CLIENT
    client1 = connect_redis()
    client1.execute_command("VIEW.REGISTER " + cct_prepare.TEST_APP_NAME_1)
    response = client1.execute_command("VIEW.SEARCH " + cct_prepare.TEST_INDEX_NAME + " * SORTBY 3 User.ID ASC User.PASSPORT LIMIT 0 2")
    print(str(response))

    query_id = int(response[1])
    assert query_id == 1
    response_size = response[5].count('key')
    assert response_size == 2

def test_view_search_multi_sort_3():
    producer = connect_redis_with_start()
    flush_db(producer) # clean all db first
    cct_prepare.create_index(producer)

    # ADD INITIAL DATA
    for i in range(20):
        d = cct_prepare.generate_single_object(1000-i , 2000+i, "aaa")
        key = cct_prepare.TEST_INDEX_PREFIX + str(i)
        producer.json().set(key, Path.root_path(), d)

    # FIRST CLIENT
    client1 = connect_redis()
    client1.execute_command("VIEW.REGISTER " + cct_prepare.TEST_APP_NAME_1)
    response = client1.execute_command("VIEW.SEARCH " + cct_prepare.TEST_INDEX_NAME + " * SORTBY 4 User.ID ASC User.PASSPORT DESC LIMIT 0 2")
    print(str(response))

    query_id = int(response[1])
    assert query_id == 1
    response_size = response[5].count('key')
    assert response_size == 2

def test_view_search_multi_sort_4():
    producer = connect_redis_with_start()
    flush_db(producer) # clean all db first
    cct_prepare.create_index(producer)

    # ADD INITIAL DATA
    for i in range(20):
        d = cct_prepare.generate_single_object(1000-i , 2000+i, "aaa")
        key = cct_prepare.TEST_INDEX_PREFIX + str(i)
        producer.json().set(key, Path.root_path(), d)

    # FIRST CLIENT
    client1 = connect_redis()
    client1.execute_command("VIEW.REGISTER " + cct_prepare.TEST_APP_NAME_1)
    response = client1.execute_command("VIEW.SEARCH " + cct_prepare.TEST_INDEX_NAME + " * SORTBY 5 User.ID ASC User.PASSPORT DESC User.Address.ID  LIMIT 0 2")
    print(str(response))

    query_id = int(response[1])
    assert query_id == 1
    response_size = response[5].count('key')
    assert response_size == 2

def test_view_search_multi_sort_5():
    producer = connect_redis_with_start()
    flush_db(producer) # clean all db first
    cct_prepare.create_index(producer)

    # ADD INITIAL DATA
    for i in range(20):
        d = cct_prepare.generate_single_object(1000-i , 2000+i, "aaa")
        key = cct_prepare.TEST_INDEX_PREFIX + str(i)
        producer.json().set(key, Path.root_path(), d)

    # FIRST CLIENT
    client1 = connect_redis()
    client1.execute_command("VIEW.REGISTER " + cct_prepare.TEST_APP_NAME_1)
    response = client1.execute_command("VIEW.SEARCH " + cct_prepare.TEST_INDEX_NAME + " * SORTBY 6 User.ID ASC User.PASSPORT DESC User.Address.ID ASC LIMIT 0 2")
    print(str(response))

    query_id = int(response[1])
    assert query_id == 1
    response_size = response[5].count('key')
    assert response_size == 2

def test_view_search_multi_sort_6():
    producer = connect_redis_with_start()
    flush_db(producer) # clean all db first
    cct_prepare.create_index(producer)

    # ADD INITIAL DATA
    for i in range(20):
        d = cct_prepare.generate_single_object(1000-i , 2000+i, "aaa")
        key = cct_prepare.TEST_INDEX_PREFIX + str(i)
        producer.json().set(key, Path.root_path(), d)

    # FIRST CLIENT
    client1 = connect_redis()
    client1.execute_command("VIEW.REGISTER " + cct_prepare.TEST_APP_NAME_1)
    response = client1.execute_command("VIEW.SEARCH " + cct_prepare.TEST_INDEX_NAME + " * SORTBY 3 User.ID User.PASSPORT User.Address.ID LIMIT 0 2")
    print(str(response))

    query_id = int(response[1])
    assert query_id == 1
    response_size = response[5].count('key')
    assert response_size == 2