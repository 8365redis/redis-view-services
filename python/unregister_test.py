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

def test_unregister_single():
    producer = connect_redis_with_start()
    flush_db(producer) # clean all db first

    # FIRST CLIENT REGISTER
    client1 = connect_redis()
    response = client1.execute_command("VIEW.REGISTER " + cct_prepare.TEST_APP_NAME_1)
    assert response == "OK"

    # FIRST CLIENT UNREGISTER
    response = client1.execute_command("VIEW.UNREGISTER " + cct_prepare.TEST_APP_NAME_1)
    assert response == "OK"


def test_unregister_multi():
    producer = connect_redis_with_start()
    flush_db(producer) # clean all db first

    client_cnt = 100
    clients = []
    # MULTI CLIENT REGISTER
    for i in range(client_cnt):
        client = connect_redis()
        clients.append(client)
        response = client.execute_command("VIEW.REGISTER " + cct_prepare.TEST_APP_NAME_1 + "-" + str(i))
        assert response == "OK"

    # MULTI CLIENT UNREGISTER
    for i in range(client_cnt):
        client = clients[i]
        response = client.execute_command("VIEW.UNREGISTER " + cct_prepare.TEST_APP_NAME_1 + "-" + str(i))
        assert response == "OK"


def test_unregister_single_check_stream_exist():
    producer = connect_redis_with_start()
    flush_db(producer) # clean all db first

    # FIRST CLIENT REGISTER
    client1 = connect_redis()
    response = client1.execute_command("VIEW.REGISTER " + cct_prepare.TEST_APP_NAME_1)
    assert response == "OK"

    # CHECK STREAM
    exists = client1.exists(cct_prepare.TEST_APP_NAME_1)
    assert exists == 1


    # FIRST CLIENT UNREGISTER
    response = client1.execute_command("VIEW.UNREGISTER " + cct_prepare.TEST_APP_NAME_1)
    assert response == "OK"

    time.sleep(1.0)

    # CHECK STREAM
    exists = client1.exists(cct_prepare.TEST_APP_NAME_1)
    assert exists == 0