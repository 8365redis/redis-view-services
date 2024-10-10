from manage_redis import kill_redis, connect_redis_with_start, connect_redis
from manage_redis_data import flush_db, create_index, add_json_data, TEST_INDEX_NAME
import pytest
import cct_prepare
from redis.commands.json.path import Path
from redis.commands.search.query import GeoFilter, NumericFilter, Query
from redis.commands.search.result import Result
import time
import random

@pytest.fixture(autouse=True)
def before_and_after_test():
    print("Start")
    yield
    kill_redis()
    print("End")

@pytest.mark.skipif(True ,
                    reason="Only run manually")
def test_monitoring_latency():
    producer = connect_redis_with_start()
    flush_db(producer) # clean all db first
    cct_prepare.create_index(producer)
    time.sleep(5)

    producer.execute_command("FT.CONFIG SET MAXSEARCHRESULTS 1000000")
    producer.execute_command("FT.CONFIG SET MAXAGGREGATERESULTS 1000000")

    total = 50000
    # ADD INITIAL DATA
    for i in range(total):
        passport = "aaa"
        if i % 3 == 0 :
            passport = "bbb"
        elif i % 3 == 1 :
            passport = "ccc"
        d = cct_prepare.generate_single_object(1000000 + i , 20000000 - i, passport)
        key = cct_prepare.TEST_INDEX_PREFIX + str(i)
        producer.json().set(key, Path.root_path(), d)

    time.sleep(5)

    total_client = 10
    total_query = 10
    clients = []
    for i in range(total_client):
        client = connect_redis()
        client.execute_command("VIEW.REGISTER " + "app" + str(i))
        clients.append(client)
        for q in range(total_query):
            start = random.randint(0, total-1)
            end = random.randint(0, total-1)
            if start > end:
                temp = start
                start = end
                end = temp
            query = "VIEW.SEARCH " + cct_prepare.TEST_INDEX_NAME + " @User\\.PASSPORT:{" + "aaa" + "}" + " SORTBY 1 User.ID LIMIT " + str(start) + " " + str(end)
            #print(query) 
            response = client.execute_command(query)

    time.sleep(5.0)

    test_client = connect_redis()
    test_client.execute_command("VIEW.REGISTER " + "test_client")
    start_set_time = time.perf_counter_ns()
    client.execute_command("VIEW.SEARCH " + cct_prepare.TEST_INDEX_NAME + " @User\\.PASSPORT:{" + "aaa" + "}" + " SORTBY 1 User.ID LIMIT 1000 2000")
    end_set_time = time.perf_counter_ns()
    diff = end_set_time - start_set_time
    print(str(diff))