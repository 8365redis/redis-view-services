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

#@pytest.mark.skipif(True , reason="Only run manually")
def test_hb_normal():
    client1 = connect_redis_with_start()
    flush_db(client1) # clean all db first

    # FIRST CLIENT REGISTER
    response = client1.execute_command("VIEW.REGISTER " + cct_prepare.TEST_APP_NAME_1)
    assert response == "OK"

    response = client1.execute_command("VIEW.HEARTBEAT")
    assert response == "OK"