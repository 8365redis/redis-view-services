from manage_redis import kill_redis, connect_redis_with_start, connect_redis
from manage_redis_data import flush_db, create_index, add_json_data, TEST_INDEX_NAME
import pytest
import time
import hashlib


@pytest.fixture(autouse=True)
def before_and_after_test():
    print("Start")
    yield
    kill_redis()
    print("End")

def test_add_1M_data_and_search():
    producer = connect_redis_with_start()
    flush_db(producer) # clean all db first
    create_index(producer)

    start_time = time.time()
    add_json_data(producer, 1000, 10)
    end_time = time.time()
    json_set_latency = (int)(end_time - start_time) * 1000  # time.time returns ns
    print("Module enabled latency = " + str(json_set_latency) + "ms")

def test_repetitive_search():
    client = connect_redis_with_start()
    query_value = "data1"

    print("0-10")

    for i in range(10):
        res = client.execute_command("FT.SEARCH "+ TEST_INDEX_NAME + " @base\\.data1:{" + query_value + "} LIMIT 0 10")
        print(len(str(res)))

    print("10-20")

    for i in range(10):
        res = client.execute_command("FT.SEARCH "+ TEST_INDEX_NAME + " @base\\.data1:{" + query_value + "} LIMIT 10 20")
        print(len(str(res)))
    
    print("20-30")

    for i in range(10):
        res = client.execute_command("FT.SEARCH "+ TEST_INDEX_NAME + " @base\\.data1:{" + query_value + "} LIMIT 20 30")
        print(len(str(res)))

    res = client.execute_command("INFO latencystats")
    print(str(res))
