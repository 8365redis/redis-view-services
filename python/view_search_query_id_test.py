from manage_redis import kill_redis, connect_redis_with_start, connect_redis
from manage_redis_data import flush_db, create_index, add_json_data, TEST_INDEX_NAME
import pytest
import cct_prepare
from redis.commands.json.path import Path
import time

@pytest.fixture(autouse=True)
def before_and_after_test():
    print("Start")
    yield
    kill_redis()
    print("End")

def test_view_query_id():
    producer = connect_redis_with_start()
    flush_db(producer) # clean all db first
    cct_prepare.create_index(producer)

    # ADD DATA
    d = cct_prepare.generate_single_object(1000 , 2000, "aaa")
    key = cct_prepare.TEST_INDEX_PREFIX + str(1)
    producer.json().set(key, Path.root_path(), d)

    id = 0

    # FIRST CLIENT
    client1 = connect_redis()
    client1.execute_command("VIEW.REGISTER " + cct_prepare.TEST_APP_NAME_1)
    result = client1.execute_command("VIEW.SEARCH " + cct_prepare.TEST_INDEX_NAME +  " @User\\.PASSPORT:{" + "aaa" + "} SORTBY User.ID")
    print(str(result))
    assert id ==  result[0]
    id = id + 1

    result = client1.execute_command("VIEW.SEARCH " + cct_prepare.TEST_INDEX_NAME + " @User\\.PASSPORT:{" + "aaa" + "} SORTBY User.ID")
    assert id ==  result[0]
    id = id + 1

    result = client1.execute_command("VIEW.SEARCH " + cct_prepare.TEST_INDEX_NAME + " @User\\.PASSPORT:{" + "aaa" + "} SORTBY User.ID")
    assert id ==  result[0]
    id = id + 1

    # SECOND CLIENT
    client2 = connect_redis()
    client2.execute_command("VIEW.REGISTER " + cct_prepare.TEST_APP_NAME_2)
    result = client2.execute_command("VIEW.SEARCH " + cct_prepare.TEST_INDEX_NAME + " @User\\.PASSPORT:{" + "aaa" + "} SORTBY User.ID")
    assert id ==  result[0]
    id = id + 1

    result = client2.execute_command("VIEW.SEARCH " + cct_prepare.TEST_INDEX_NAME + " @User\\.PASSPORT:{" + "aaa" + "} SORTBY User.ID")
    assert id ==  result[0]
    id = id + 1

    # FIRST CLIENT AGAIN
    result = client1.execute_command("VIEW.SEARCH " + cct_prepare.TEST_INDEX_NAME + " @User\\.PASSPORT:{" + "aaa" + "} SORTBY User.ID")
    assert id ==  result[0]
    id = id + 1

    # THIRD CLIENT
    client3 = connect_redis()
    client3.execute_command("VIEW.REGISTER " + cct_prepare.TEST_APP_NAME_3)
    result = client3.execute_command("VIEW.SEARCH " + cct_prepare.TEST_INDEX_NAME + " @User\\.PASSPORT:{" + "aaa" + "} SORTBY User.ID")
    assert id ==  result[0]
    id = id + 1


