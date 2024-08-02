from manage_redis import kill_redis, connect_redis_with_start, connect_redis
from manage_redis_data import flush_db, create_index, add_json_data, TEST_INDEX_NAME
import pytest
import cct_prepare
from redis.commands.json.path import Path
from redis.commands.search.query import GeoFilter, NumericFilter, Query
from redis.commands.search.result import Result
import time

MONITORING_TS_DELIMETER = ":"

# KEEP SYNC WITH module_monitoring.h
LATENCY_TYPE_LABEL_KEY = "LATENCY_TYPE"
LATENCY_TYPE_VIEW_SEARCH = "VIEW_SEARCH"
LATENCY_TYPE_VIEW_SEARCH_HANDLER = "VIEW_SEARCH_HANDLER"
LATENCY_TYPE_VIEW_COUNT = "VIEW_COUNT"
LATENCY_TYPE_VIEW_COUNT_HANDLER = "VIEW_COUNT_HANDLER"

COUNT_TYPE_LABEL_KEY = "COUNT_TYPE"
COUNT_TYPE_VIEW_COUNT = "VIEW_COUNT"
COUNT_TYPE_VIEW_SEARCH = "VIEW_SEARCH"

CLIENT_LABEL_KEY = "CLIENT"

VIEW_COUNT_LATENCY_METRIC_STREAM_PREFIX = "VIEW_COUNT_LATENCY:"
VIEW_COUNT_HANDLER_LATENCY_METRIC_STREAM_PREFIX = "VIEW_COUNT_HANDLER_LATENCY:"

VIEW_COUNT_COUNT_METRIC_STREAM_PREFIX = "VIEW_COUNT_COUNT:"

VIEW_SEARCH_LATENCY_METRIC_STREAM_PREFIX = "VIEW_SEARCH_LATENCY:"
VIEW_SEARCH_HANDLER_LATENCY_METRIC_STREAM_PREFIX = "VIEW_SEARCH_HANDLER_LATENCY:"

VIEW_SEARCH_COUNT_METRIC_STREAM_PREFIX = "VIEW_SEARCH_COUNT:"

VIEW_SEARCH_HANDLER_TOTAL_LATENCY_METRIC_STREAM_PREFIX = "VIEW_SEARCH_HANDLER_TOTAL_LATENCY"
VIEW_COUNT_HANDLER_TOTAL_LATENCY_METRIC_STREAM_PREFIX = "VIEW_COUNT_HANDLER_TOTAL_LATENCY"
# KEEP SYNC WITH module_monitoring.h


@pytest.fixture(autouse=True)
def before_and_after_test():
    print("Start")
    yield
    kill_redis()
    print("End")

def test_streams_exist_with_new_register():
    producer = connect_redis_with_start()
    flush_db(producer) # clean all db first
    cct_prepare.create_index(producer)

    # ADD INITIAL DATA
    for i in range(1, 101):
        d = cct_prepare.generate_single_object(1000 + i, 2000 + i, "aaa")
        key = cct_prepare.TEST_INDEX_PREFIX + str(i)
        producer.json().set(key, Path.root_path(), d)

    # FIRST CLIENT
    client1 = connect_redis()
    response = client1.execute_command("VIEW.REGISTER " + cct_prepare.TEST_APP_NAME_1)
    assert response == "OK"

    # SECOND CLIENT
    client2 = connect_redis()
    response = client2.execute_command("VIEW.REGISTER " + cct_prepare.TEST_APP_NAME_2)
    assert response == "OK"

    assert client1.exists(VIEW_SEARCH_HANDLER_TOTAL_LATENCY_METRIC_STREAM_PREFIX)
    assert client1.exists(VIEW_COUNT_HANDLER_TOTAL_LATENCY_METRIC_STREAM_PREFIX)


def test_search_latency_metric_is_added():
    producer = connect_redis_with_start()
    flush_db(producer) # clean all db first
    cct_prepare.create_index(producer)

    total_count = 100

    # ADD INITIAL DATA
    for i in range(1, total_count):
        d = cct_prepare.generate_single_object(1000 + i, 2000 + i, "aaa")
        key = cct_prepare.TEST_INDEX_PREFIX + str(i)
        producer.json().set(key, Path.root_path(), d)

    # FIRST CLIENT
    client1 = connect_redis()
    response = client1.execute_command("VIEW.REGISTER " + cct_prepare.TEST_APP_NAME_1)

    query = cct_prepare.TEST_INDEX_NAME + " * SORTBY 1 User.ID LIMIT 0 3"
    response = client1.execute_command("VIEW.SEARCH " + query)
    print(str(response))

    metric_key = 'VIEW_SEARCH_LATENCY:app1:test_index * LOAD 1 @__key SORTBY 3 @User.ID @__key ASC LIMIT 0 3'
    last_value = client1.execute_command('TS.GET' , metric_key )
    assert last_value
    print(str(last_value))

    time.sleep(1.1)

    metric_key_handler = 'VIEW_SEARCH_HANDLER_LATENCY:app1:test_index * LOAD 1 @__key SORTBY 3 @User.ID @__key ASC LIMIT 0 3'
    last_value = client1.execute_command('TS.GET' , metric_key_handler )
    assert last_value
    print(str(last_value))


def test_count_latency_metric_is_added():
    producer = connect_redis_with_start()
    flush_db(producer) # clean all db first
    cct_prepare.create_index(producer)

    total_count = 10000

    # ADD INITIAL DATA
    for i in range(1, total_count):
        d = cct_prepare.generate_single_object(1000 + i, 2000 + i, "aaa")
        key = cct_prepare.TEST_INDEX_PREFIX + str(i)
        producer.json().set(key, Path.root_path(), d)

    # FIRST CLIENT
    client1 = connect_redis()
    response = client1.execute_command("VIEW.REGISTER " + cct_prepare.TEST_APP_NAME_1)

    response = client1.execute_command("VIEW.COUNT " + cct_prepare.TEST_INDEX_NAME + " *")
    print(str(response))

    metric_key = 'VIEW_COUNT_LATENCY:app1:test_index *'
    last_value = client1.execute_command("TS.GET ", metric_key)
    assert last_value
    print(str(last_value))

    time.sleep(1.1)

    metric_key = 'VIEW_COUNT_HANDLER_LATENCY:app1:test_index *'
    last_value = client1.execute_command("TS.GET ", metric_key)
    assert last_value
    print(str(last_value))

    res = producer.execute_command("INFO latencystats")
    print("INFO latencystats output : \n" + str(res) )


def test_search_count_metric_is_added():
    producer = connect_redis_with_start()
    flush_db(producer) # clean all db first
    cct_prepare.create_index(producer)

    total_count = 100

    # ADD INITIAL DATA
    for i in range(1, total_count):
        d = cct_prepare.generate_single_object(1000 + i, 2000 + i, "aaa")
        key = cct_prepare.TEST_INDEX_PREFIX + str(i)
        producer.json().set(key, Path.root_path(), d)

    # FIRST CLIENT
    client1 = connect_redis()
    response = client1.execute_command("VIEW.REGISTER " + cct_prepare.TEST_APP_NAME_1)

    response = client1.execute_command("VIEW.SEARCH " + cct_prepare.TEST_INDEX_NAME + " * SORTBY 1 User.ID LIMIT 0 3")
    #print(str(response))

    last_value = client1.execute_command("TS.GET " + (VIEW_SEARCH_COUNT_METRIC_STREAM_PREFIX + cct_prepare.TEST_APP_NAME_1))
    assert last_value[1] == '1'
    #print(str(last_value))

    response = client1.execute_command("VIEW.SEARCH " + cct_prepare.TEST_INDEX_NAME + " * SORTBY 1 User.ID LIMIT 0 2")
    #print(str(response))

    last_value = client1.execute_command("TS.GET " + (VIEW_SEARCH_COUNT_METRIC_STREAM_PREFIX + cct_prepare.TEST_APP_NAME_1))
    assert last_value[1] == '2'
    #print(str(last_value))

def test_count_count_metric_is_added():
    producer = connect_redis_with_start()
    flush_db(producer) # clean all db first
    cct_prepare.create_index(producer)

    total_count = 100

    # ADD INITIAL DATA
    for i in range(1, total_count):
        d = cct_prepare.generate_single_object(1000 + i, 2000 + i, "aaa")
        key = cct_prepare.TEST_INDEX_PREFIX + str(i)
        producer.json().set(key, Path.root_path(), d)

    # FIRST CLIENT
    client1 = connect_redis()
    response = client1.execute_command("VIEW.REGISTER " + cct_prepare.TEST_APP_NAME_1)

    response = client1.execute_command("VIEW.COUNT " + cct_prepare.TEST_INDEX_NAME + " *" )
    print(str(response))

    last_value = client1.execute_command("TS.GET " + (VIEW_COUNT_COUNT_METRIC_STREAM_PREFIX + cct_prepare.TEST_APP_NAME_1))
    assert last_value[1] == '1'
    print(str(last_value))

    response = client1.execute_command("VIEW.COUNT " + cct_prepare.TEST_INDEX_NAME + " @User\\.PASSPORT:{" + "aaa" + "}")
    print(str(response))

    last_value = client1.execute_command("TS.GET " + (VIEW_COUNT_COUNT_METRIC_STREAM_PREFIX + cct_prepare.TEST_APP_NAME_1))
    assert last_value[1] == '2'
    print(str(last_value))


def test_count_count_metric_is_reset_after_unsubscribe():
    producer = connect_redis_with_start()
    flush_db(producer) # clean all db first
    cct_prepare.create_index(producer)

    total_count = 100

    # ADD INITIAL DATA
    for i in range(1, total_count):
        d = cct_prepare.generate_single_object(1000 + i, 2000 + i, "aaa")
        key = cct_prepare.TEST_INDEX_PREFIX + str(i)
        producer.json().set(key, Path.root_path(), d)

    # FIRST CLIENT
    client1 = connect_redis()
    response = client1.execute_command("VIEW.REGISTER " + cct_prepare.TEST_APP_NAME_1)

    response = client1.execute_command("VIEW.COUNT " + cct_prepare.TEST_INDEX_NAME + " *" )
    print(str(response))

    last_value = client1.execute_command("TS.GET " + (VIEW_COUNT_COUNT_METRIC_STREAM_PREFIX + cct_prepare.TEST_APP_NAME_1))
    assert last_value[1] == '1'
    print(str(last_value))

    response = client1.execute_command("VIEW.COUNT " + cct_prepare.TEST_INDEX_NAME + " @User\\.PASSPORT:{" + "aaa" + "}")
    print(str(response))

    last_value = client1.execute_command("TS.GET " + (VIEW_COUNT_COUNT_METRIC_STREAM_PREFIX + cct_prepare.TEST_APP_NAME_1))
    assert last_value[1] == '2'
    print(str(last_value))

    #UNSUB COUNT
    resp = client1.execute_command("VIEW.COUNT.UNSUBSCRIBE 1")
    assert resp == "OK 1 unsubscribed"

    time.sleep(1.1)

    last_value = client1.execute_command("TS.GET " + (VIEW_COUNT_COUNT_METRIC_STREAM_PREFIX + cct_prepare.TEST_APP_NAME_1))
    assert last_value[1] == '1'
    print(str(last_value))

    time.sleep(1.1)

    response = client1.execute_command("VIEW.COUNT " + cct_prepare.TEST_INDEX_NAME + " *" )
    print(str(response))

    last_value = client1.execute_command("TS.GET " + (VIEW_COUNT_COUNT_METRIC_STREAM_PREFIX + cct_prepare.TEST_APP_NAME_1))
    assert last_value[1] == '2'
    print(str(last_value))


def test_count_count_metric_is_reset_after_unregister():
    producer = connect_redis_with_start()
    flush_db(producer) # clean all db first
    cct_prepare.create_index(producer)

    total_count = 100

    # ADD INITIAL DATA
    for i in range(1, total_count):
        d = cct_prepare.generate_single_object(1000 + i, 2000 + i, "aaa")
        key = cct_prepare.TEST_INDEX_PREFIX + str(i)
        producer.json().set(key, Path.root_path(), d)

    # FIRST CLIENT
    client1 = connect_redis()
    response = client1.execute_command("VIEW.REGISTER " + cct_prepare.TEST_APP_NAME_1)

    response = client1.execute_command("VIEW.COUNT " + cct_prepare.TEST_INDEX_NAME + " *" )
    print(str(response))

    last_value = client1.execute_command("TS.GET " + (VIEW_COUNT_COUNT_METRIC_STREAM_PREFIX + cct_prepare.TEST_APP_NAME_1))
    assert last_value[1] == '1'
    print(str(last_value))

    response = client1.execute_command("VIEW.COUNT " + cct_prepare.TEST_INDEX_NAME + " @User\\.PASSPORT:{" + "aaa" + "}")
    print(str(response))

    last_value = client1.execute_command("TS.GET " + (VIEW_COUNT_COUNT_METRIC_STREAM_PREFIX + cct_prepare.TEST_APP_NAME_1))
    assert last_value[1] == '2'
    print(str(last_value))

    #UNREG
    response = client1.execute_command("VIEW.UNREGISTER " + cct_prepare.TEST_APP_NAME_1)
    assert response == "OK"

    time.sleep(2)

    last_value = client1.execute_command("TS.GET " + (VIEW_COUNT_COUNT_METRIC_STREAM_PREFIX + cct_prepare.TEST_APP_NAME_1))
    assert last_value[1] == '0'
    print(str(last_value))

    response = client1.execute_command("VIEW.REGISTER " + cct_prepare.TEST_APP_NAME_1)
    assert response == "OK"

    response = client1.execute_command("VIEW.COUNT " + cct_prepare.TEST_INDEX_NAME + " *" )
    print(str(response))

    last_value = client1.execute_command("TS.GET " + (VIEW_COUNT_COUNT_METRIC_STREAM_PREFIX + cct_prepare.TEST_APP_NAME_1))
    assert last_value[1] == '1'
    print(str(last_value))


def test_count_count_metric_is_reset_after_disconnect():
    producer = connect_redis_with_start()
    flush_db(producer) # clean all db first
    cct_prepare.create_index(producer)

    total_count = 100

    # ADD INITIAL DATA
    for i in range(1, total_count):
        d = cct_prepare.generate_single_object(1000 + i, 2000 + i, "aaa")
        key = cct_prepare.TEST_INDEX_PREFIX + str(i)
        producer.json().set(key, Path.root_path(), d)

    # FIRST CLIENT
    client1 = connect_redis()
    response = client1.execute_command("VIEW.REGISTER " + cct_prepare.TEST_APP_NAME_1)

    response = client1.execute_command("VIEW.COUNT " + cct_prepare.TEST_INDEX_NAME + " *" )
    print(str(response))

    last_value = client1.execute_command("TS.GET " + (VIEW_COUNT_COUNT_METRIC_STREAM_PREFIX + cct_prepare.TEST_APP_NAME_1))
    assert last_value[1] == '1'
    print(str(last_value))

    response = client1.execute_command("VIEW.COUNT " + cct_prepare.TEST_INDEX_NAME + " @User\\.PASSPORT:{" + "aaa" + "}")
    print(str(response))

    last_value = client1.execute_command("TS.GET " + (VIEW_COUNT_COUNT_METRIC_STREAM_PREFIX + cct_prepare.TEST_APP_NAME_1))
    assert last_value[1] == '2'
    print(str(last_value))

    # DISCONNECT
    client1.connection_pool.disconnect()
    time.sleep(2)

    last_value = client1.execute_command("TS.GET " + (VIEW_COUNT_COUNT_METRIC_STREAM_PREFIX + cct_prepare.TEST_APP_NAME_1))
    assert last_value[1] == '0'
    print(str(last_value))

    # FIRST CLIENT REGISTER AGAIN AFTER DISC
    client1 = connect_redis()
    response = client1.execute_command("VIEW.REGISTER " + cct_prepare.TEST_APP_NAME_1)
    assert response == "OK"

    response = client1.execute_command("VIEW.COUNT " + cct_prepare.TEST_INDEX_NAME + " *" )
    print(str(response))

    last_value = client1.execute_command("TS.GET " + (VIEW_COUNT_COUNT_METRIC_STREAM_PREFIX + cct_prepare.TEST_APP_NAME_1))
    assert last_value[1] == '1'
    print(str(last_value))


@pytest.mark.skip(reason="Will be fixed")
def test_search_count_metric_is_reset_after_unsubscribe():
    producer = connect_redis()
    time.sleep(1000)
    flush_db(producer) # clean all db first
    time.sleep(1000)
    cct_prepare.create_index(producer)

    total_count = 100

    # ADD INITIAL DATA
    for i in range(1, total_count):
        d = cct_prepare.generate_single_object(1000 + i, 2000 + i, "aaa")
        key = cct_prepare.TEST_INDEX_PREFIX + str(i)
        producer.json().set(key, Path.root_path(), d)

    # FIRST CLIENT
    client1 = connect_redis()
    response = client1.execute_command("VIEW.REGISTER " + cct_prepare.TEST_APP_NAME_1)

    response = client1.execute_command("VIEW.SEARCH " + cct_prepare.TEST_INDEX_NAME + " * SORTBY 1 User.ID LIMIT 0 3")
    #print(str(response))

    last_value = client1.execute_command("TS.GET " + (VIEW_SEARCH_COUNT_METRIC_STREAM_PREFIX + cct_prepare.TEST_APP_NAME_1))
    assert last_value[1] == '1'
    #print(str(last_value))

    response = client1.execute_command("VIEW.SEARCH " + cct_prepare.TEST_INDEX_NAME + " * SORTBY 1 User.ID LIMIT 0 2")
    #print(str(response))

    last_value = client1.execute_command("TS.GET " + (VIEW_SEARCH_COUNT_METRIC_STREAM_PREFIX + cct_prepare.TEST_APP_NAME_1))
    assert last_value[1] == '2'
    #print(str(last_value))

    #UNSUB SEARCH 1
    resp = client1.execute_command("VIEW.SEARCH.UNSUBSCRIBE 1")
    assert resp == "OK 1 unsubscribed"

    time.sleep(1.1)

    last_value = client1.execute_command("TS.GET " + (VIEW_SEARCH_COUNT_METRIC_STREAM_PREFIX + cct_prepare.TEST_APP_NAME_1))
    assert last_value[1] == '1'

    #UNSUB SEARCH 2
    resp = client1.execute_command("VIEW.SEARCH.UNSUBSCRIBE 2")
    assert resp == "OK 2 unsubscribed"

    time.sleep(1.1)

    last_value = client1.execute_command("TS.GET " + (VIEW_SEARCH_COUNT_METRIC_STREAM_PREFIX + cct_prepare.TEST_APP_NAME_1))
    assert last_value[1] == '0'


def test_search_count_metric_is_reset_after_unregister():
    producer = connect_redis_with_start()
    flush_db(producer) # clean all db first
    cct_prepare.create_index(producer)

    total_count = 100

    # ADD INITIAL DATA
    for i in range(1, total_count):
        d = cct_prepare.generate_single_object(1000 + i, 2000 + i, "aaa")
        key = cct_prepare.TEST_INDEX_PREFIX + str(i)
        producer.json().set(key, Path.root_path(), d)

    # FIRST CLIENT
    client1 = connect_redis()
    response = client1.execute_command("VIEW.REGISTER " + cct_prepare.TEST_APP_NAME_1)

    response = client1.execute_command("VIEW.SEARCH " + cct_prepare.TEST_INDEX_NAME + " * SORTBY 1 User.ID LIMIT 0 3")
    #print(str(response))

    last_value = client1.execute_command("TS.GET " + (VIEW_SEARCH_COUNT_METRIC_STREAM_PREFIX + cct_prepare.TEST_APP_NAME_1))
    assert last_value[1] == '1'
    #print(str(last_value))

    response = client1.execute_command("VIEW.SEARCH " + cct_prepare.TEST_INDEX_NAME + " * SORTBY 1 User.ID LIMIT 0 2")
    #print(str(response))

    last_value = client1.execute_command("TS.GET " + (VIEW_SEARCH_COUNT_METRIC_STREAM_PREFIX + cct_prepare.TEST_APP_NAME_1))
    assert last_value[1] == '2'
    #print(str(last_value))

    #UNREG
    response = client1.execute_command("VIEW.UNREGISTER " + cct_prepare.TEST_APP_NAME_1)
    assert response == "OK"

    time.sleep(2)

    last_value = client1.execute_command("TS.GET " + (VIEW_SEARCH_COUNT_METRIC_STREAM_PREFIX + cct_prepare.TEST_APP_NAME_1))
    assert last_value[1] == '0'

    response = client1.execute_command("VIEW.REGISTER " + cct_prepare.TEST_APP_NAME_1)
    assert response == "OK"

    response = client1.execute_command("VIEW.SEARCH " + cct_prepare.TEST_INDEX_NAME + " * SORTBY 1 User.ID LIMIT 0 3")
    #print(str(response))

    time.sleep(1.1)

    last_value = client1.execute_command("TS.GET " + (VIEW_SEARCH_COUNT_METRIC_STREAM_PREFIX + cct_prepare.TEST_APP_NAME_1))
    assert last_value[1] == '1'


def test_search_count_metric_is_reset_after_disconnect():
    producer = connect_redis_with_start()
    flush_db(producer) # clean all db first
    cct_prepare.create_index(producer)

    total_count = 100

    # ADD INITIAL DATA
    for i in range(1, total_count):
        d = cct_prepare.generate_single_object(1000 + i, 2000 + i, "aaa")
        key = cct_prepare.TEST_INDEX_PREFIX + str(i)
        producer.json().set(key, Path.root_path(), d)

    # FIRST CLIENT
    client1 = connect_redis()
    response = client1.execute_command("VIEW.REGISTER " + cct_prepare.TEST_APP_NAME_1)

    response = client1.execute_command("VIEW.SEARCH " + cct_prepare.TEST_INDEX_NAME + " * SORTBY 1 User.ID LIMIT 0 3")
    #print(str(response))

    last_value = client1.execute_command("TS.GET " + (VIEW_SEARCH_COUNT_METRIC_STREAM_PREFIX + cct_prepare.TEST_APP_NAME_1))
    assert last_value[1] == '1'
    #print(str(last_value))

    response = client1.execute_command("VIEW.SEARCH " + cct_prepare.TEST_INDEX_NAME + " * SORTBY 1 User.ID LIMIT 0 2")
    #print(str(response))

    last_value = client1.execute_command("TS.GET " + (VIEW_SEARCH_COUNT_METRIC_STREAM_PREFIX + cct_prepare.TEST_APP_NAME_1))
    assert last_value[1] == '2'
    #print(str(last_value))

    # DISCONNECT
    client1.connection_pool.disconnect()
    time.sleep(2)

    time.sleep(2)

    last_value = client1.execute_command("TS.GET " + (VIEW_SEARCH_COUNT_METRIC_STREAM_PREFIX + cct_prepare.TEST_APP_NAME_1))
    assert last_value[1] == '0'

    response = client1.execute_command("VIEW.REGISTER " + cct_prepare.TEST_APP_NAME_1)
    assert response == "OK"

    response = client1.execute_command("VIEW.SEARCH " + cct_prepare.TEST_INDEX_NAME + " * SORTBY 1 User.ID LIMIT 0 3")
    #print(str(response))

    time.sleep(1.1)

    last_value = client1.execute_command("TS.GET " + (VIEW_SEARCH_COUNT_METRIC_STREAM_PREFIX + cct_prepare.TEST_APP_NAME_1))
    assert last_value[1] == '1'

    response = client1.execute_command("VIEW.SEARCH " + cct_prepare.TEST_INDEX_NAME + " * SORTBY 1 User.ID LIMIT 0 2")
    #print(str(response))

    time.sleep(1.1)

    last_value = client1.execute_command("TS.GET " + (VIEW_SEARCH_COUNT_METRIC_STREAM_PREFIX + cct_prepare.TEST_APP_NAME_1))
    assert last_value[1] == '2'


def test_search_latency_metric_query_multi():
    producer = connect_redis_with_start()
    flush_db(producer) # clean all db first
    cct_prepare.create_index(producer)

    total_count = 100

    # ADD INITIAL DATA
    for i in range(1, total_count):
        d = cct_prepare.generate_single_object(1000 + i, 2000 + i, "aaa")
        key = cct_prepare.TEST_INDEX_PREFIX + str(i)
        producer.json().set(key, Path.root_path(), d)

    # FIRST CLIENT
    client1 = connect_redis()
    response = client1.execute_command("VIEW.REGISTER " + cct_prepare.TEST_APP_NAME_1)

    response = client1.execute_command("VIEW.SEARCH " + cct_prepare.TEST_INDEX_NAME + " * SORTBY 1 User.ID LIMIT 0 1")
    metric_key_1 = 'VIEW_SEARCH_LATENCY:app1:test_index * LOAD 1 @__key SORTBY 3 @User.ID @__key ASC LIMIT 0 1'
    last_value = client1.execute_command("TS.GET " , metric_key_1)
    assert last_value
    print(str(last_value))

    response = client1.execute_command("VIEW.SEARCH " + cct_prepare.TEST_INDEX_NAME + " * SORTBY 1 User.ID LIMIT 0 2")
    metric_key_2 = 'VIEW_SEARCH_LATENCY:app1:test_index * LOAD 1 @__key SORTBY 3 @User.ID @__key ASC LIMIT 0 2'
    last_value = client1.execute_command("TS.GET ", metric_key_2)
    assert last_value
    print(str(last_value))

    response = client1.execute_command("VIEW.SEARCH " + cct_prepare.TEST_INDEX_NAME + " * SORTBY 1 User.ID LIMIT 0 3")
    metric_key_3 = 'VIEW_SEARCH_LATENCY:app1:test_index * LOAD 1 @__key SORTBY 3 @User.ID @__key ASC LIMIT 0 3'
    last_value = client1.execute_command("TS.GET " , metric_key_3)
    assert last_value

    last_multi_value = client1.execute_command("TS.MGET FILTER CLIENT=app1 LATENCY_TYPE=VIEW_SEARCH")
    print(str(last_multi_value[0]))
    assert last_multi_value[0][0] == metric_key_1
    assert last_multi_value[0][2][1]
    print(str(last_multi_value[1]))
    assert last_multi_value[1][0] == metric_key_2
    assert last_multi_value[1][2][1] 
    print(str(last_multi_value[2]))
    assert last_multi_value[2][0] == metric_key_3
    assert last_multi_value[2][2][1] 

    time.sleep(1.1)

    metric_handler_key_1 = 'VIEW_SEARCH_HANDLER_LATENCY:app1:test_index * LOAD 1 @__key SORTBY 3 @User.ID @__key ASC LIMIT 0 1'
    metric_handler_key_2 = 'VIEW_SEARCH_HANDLER_LATENCY:app1:test_index * LOAD 1 @__key SORTBY 3 @User.ID @__key ASC LIMIT 0 2'
    metric_handler_key_3 = 'VIEW_SEARCH_HANDLER_LATENCY:app1:test_index * LOAD 1 @__key SORTBY 3 @User.ID @__key ASC LIMIT 0 3'

    last_multi_value = client1.execute_command("TS.MGET FILTER CLIENT=app1 LATENCY_TYPE=VIEW_SEARCH_HANDLER")
    print(str(last_multi_value[0]))
    assert last_multi_value[0][0] == metric_handler_key_1
    assert last_multi_value[0][2][1]
    print(str(last_multi_value[1]))
    assert last_multi_value[1][0] == metric_handler_key_2
    assert last_multi_value[1][2][1] 
    print(str(last_multi_value[2]))
    assert last_multi_value[2][0] == metric_handler_key_3
    assert last_multi_value[2][2][1] 


def test_add_data_and_queries():
    producer = connect_redis_with_start()
    time.sleep(1)
    flush_db(producer) # clean all db first
    time.sleep(1)
    cct_prepare.create_index(producer)
    total = 10000
    # ADD INITIAL DATA
    for i in range(1, total):
        d = cct_prepare.generate_single_object(1000 + i, 2000 + i, "aaa")
        key = cct_prepare.TEST_INDEX_PREFIX + str(i)
        producer.json().set(key, Path.root_path(), d)

