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

    query_id = int(response[0])
    assert query_id == 0
    response_size = int(response[1])
    assert response_size == 1

def test_view_search_return_multi():
    producer = connect_redis_with_start()
    flush_db(producer) # clean all db first
    cct_prepare.create_index(producer)

    # ADD INITIAL DATAS
    for i in range(10):
        d = cct_prepare.generate_single_object(1000 , 2000 + i, "aaa")
        key = cct_prepare.TEST_INDEX_PREFIX + str(1 + i)
        producer.json().set(key, Path.root_path(), d)

    # FIRST CLIENT
    client1 = connect_redis()
    client1.execute_command("VIEW.REGISTER " + cct_prepare.TEST_APP_NAME_1)
    response = client1.execute_command("VIEW.SEARCH " + cct_prepare.TEST_INDEX_NAME +  " @User\\.PASSPORT:{" + "aaa" + "}" + cct_prepare.QUERY_FULL_POSTFIX)

    query_id = int(response[0])
    assert query_id == 0
    response_size = int((len(response)-2)/2)
    assert response_size == 10

def test_view_search_return_multi_excess():
    producer = connect_redis_with_start()
    flush_db(producer) # clean all db first
    cct_prepare.create_index(producer)

    # ADD INITIAL DATAS
    for i in range(30):
        d = cct_prepare.generate_single_object(1000 + i, 2000, "aaa")
        key = cct_prepare.TEST_INDEX_PREFIX + str(1 + i)
        producer.json().set(key, Path.root_path(), d)

    # FIRST CLIENT
    client1 = connect_redis()
    client1.execute_command("VIEW.REGISTER " + cct_prepare.TEST_APP_NAME_1)
    response = client1.execute_command("VIEW.SEARCH " + cct_prepare.TEST_INDEX_NAME + " @User\\.PASSPORT:{" + "aaa" + "}" + cct_prepare.QUERY_FULL_POSTFIX)
    print(str(response))

    query_id = int(response[0])
    assert query_id == 0
    response_size = int((len(response)-2)/2)
    assert response_size == 10

def test_view_search_return_multi_excess_with_limit():
    producer = connect_redis_with_start()
    flush_db(producer) # clean all db first
    cct_prepare.create_index(producer)

    # ADD INITIAL DATAS
    for i in range(30):
        d = cct_prepare.generate_single_object(1000 + i, 2000, "aaa")
        key = cct_prepare.TEST_INDEX_PREFIX + str(1 + i)
        producer.json().set(key, Path.root_path(), d)

    # FIRST CLIENT
    client1 = connect_redis()
    client1.execute_command("VIEW.REGISTER " + cct_prepare.TEST_APP_NAME_1)
    response = client1.execute_command("VIEW.SEARCH " + cct_prepare.TEST_INDEX_NAME + " @User\\.PASSPORT:{" + "aaa" + "}" + cct_prepare.QUERY_FULL_POSTFIX)
    print(str(response))

    query_id = int(response[0])
    assert query_id == 0
    response_size = int((len(response)-2)/2)
    assert response_size == 10

    response = client1.execute_command("VIEW.SEARCH " + cct_prepare.TEST_INDEX_NAME + " @User\\.PASSPORT:{" + "aaa" + "}" + cct_prepare.QUERY_FULL_POSTFIX)
    print(str(response))

    query_id = int(response[0])
    assert query_id == 1
    response_size = int((len(response)-2)/2)
    assert response_size == 10

def test_view_search_no_return():
    producer = connect_redis_with_start()
    flush_db(producer) # clean all db first
    cct_prepare.create_index(producer)

    # ADD INITIAL DATAS
    for i in range(5):
        d = cct_prepare.generate_single_object(1000 , 2000 + i, "aaa")
        key = cct_prepare.TEST_INDEX_PREFIX + str(1 + i)
        producer.json().set(key, Path.root_path(), d)

    # FIRST CLIENT
    client1 = connect_redis()
    client1.execute_command("VIEW.REGISTER " + cct_prepare.TEST_APP_NAME_1)
    response = client1.execute_command("VIEW.SEARCH " + cct_prepare.TEST_INDEX_NAME + " @User\\.PASSPORT:{" + "bbb" + "}" + cct_prepare.QUERY_FULL_POSTFIX)


    query_id = int(response[0])
    assert query_id == 0
    response_size = int(response[1])
    assert response_size == 0

def test_view_search_query_id():
    producer = connect_redis_with_start()
    flush_db(producer) # clean all db first
    cct_prepare.create_index(producer)

    # ADD INITIAL DATAS
    for i in range(5):
        d = cct_prepare.generate_single_object(1000 + i , 2000, "aaa")
        key = cct_prepare.TEST_INDEX_PREFIX + str(1 + i)
        producer.json().set(key, Path.root_path(), d)

    # FIRST CLIENT FIRST QUERY
    client1 = connect_redis()
    client1.execute_command("VIEW.REGISTER " + cct_prepare.TEST_APP_NAME_1)
    response = client1.execute_command("VIEW.SEARCH " + cct_prepare.TEST_INDEX_NAME + " @User\\.ID:{" + "1000" + "}" + cct_prepare.QUERY_FULL_POSTFIX)
    query_id = int(response[0])
    assert query_id == 0

    # FIRST CLIENT SECOND QUERY
    response = client1.execute_command("VIEW.SEARCH " + cct_prepare.TEST_INDEX_NAME + " @User\\.ID:{" + "1001" + "}" + cct_prepare.QUERY_FULL_POSTFIX)
    query_id = int(response[0])
    assert query_id == 1

    # FIRST CLIENT THIRD QUERY
    response = client1.execute_command("VIEW.SEARCH "+ cct_prepare.TEST_INDEX_NAME + " @User\\.ID:{" + "1002" + "}" + cct_prepare.QUERY_FULL_POSTFIX)
    query_id = int(response[0])
    assert query_id == 2


def test_view_search_without_limit():
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

    with pytest.raises(Exception) as e_info:
        client1.execute_command("VIEW.SEARCH " + cct_prepare.TEST_INDEX_NAME + " @User\\.PASSPORT:{" + "aaa" + "} SORTBY User.ID")
    assert "ResponseError" in str(e_info)
