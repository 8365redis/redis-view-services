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

def test_register_single():
    producer = connect_redis_with_start()
    flush_db(producer) # clean all db first

    # FIRST CLIENT
    client1 = connect_redis()
    response = client1.execute_command("VIEW.REGISTER " + cct_prepare.TEST_APP_NAME_1)
    assert response == "OK"

def test_register_multiple():
    producer = connect_redis_with_start()
    flush_db(producer) # clean all db first

    # MULTI CLIENT
    for i in range(100):
        client = connect_redis()
        response = client.execute_command("VIEW.REGISTER " + cct_prepare.TEST_APP_NAME_1 + "-" + str(i))
        assert response == "OK"

def test_re_register():
    producer = connect_redis_with_start()
    flush_db(producer) # clean all db first

    # FIRST CLIENT
    client1 = connect_redis()
    response = client1.execute_command("VIEW.REGISTER " + cct_prepare.TEST_APP_NAME_1)
    assert response == "OK"