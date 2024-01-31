from manage_redis import kill_redis, connect_redis_with_start, connect_redis
from manage_redis_data import flush_db, create_index, add_json_data, TEST_INDEX_NAME, create_perf_index, insert_perf_data, update_perf_data
import pytest
import time
import threading


@pytest.fixture(autouse=True)
def before_and_after_test():
    print("Start")
    yield
    kill_redis()
    print("End")

def test_add_perf_data_and_search():
    producer = connect_redis_with_start()
    flush_db(producer) # clean all db first
    create_index(producer)

    start_time = time.time()
    add_json_data(producer, 100, 10)
    end_time = time.time()
    json_set_latency = (int)(end_time - start_time) * 1000  # time.time returns ns
    print("Module enabled latency = " + str(json_set_latency) + "ms")

    client_count = 10
    query_value = "data1"
    for i in range(client_count):
        client = connect_redis()
        client.execute_command("VIEW.REGISTER " + "client-" + str(i))
        client.execute_command("VIEW.MAIN.SEARCH test_index @base\\.data1:{" + query_value  + "}")
    
    time.sleep(1)

    res = producer.execute_command("INFO latencystats")
    print(str(res))

