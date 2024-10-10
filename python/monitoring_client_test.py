from manage_redis import kill_redis, connect_redis_with_start, connect_redis
from manage_redis_data import flush_db, create_index, add_json_data, TEST_INDEX_NAME
import pytest
import cct_prepare
from redis.commands.json.path import Path
from redis.commands.search.query import GeoFilter, NumericFilter, Query
from redis.commands.search.result import Result
import time
import json

MONITORING_TS_DELIMETER = ":"

# KEEP SYNC WITH module_monitoring.h

OPENTRACING_STREAM_NAME = "OPENTRACING" 
OPENTRACING_KEY_NAME = "TRACING"

METRIC_TAGS_KEY = "tags"
METRIC_METRIC_KEY = "metric"

COMMAND_TYPE_KEY = 'command_type'
COMMAND_KEY = "command"
INDEX_NAME_KEY = 'index_name'
METRIC_NAME_KEY = 'name'
METRIC_VALUE_KEY  = 'value'

LATENCY_TYPE_VIEW_SEARCH = "VIEW.SEARCH"
LATENCY_TYPE_VIEW_SEARCH_HANDLER = "VIEW.SEARCH.HANDLER"
LATENCY_TYPE_VIEW_COUNT = "VIEW.COUNT"
LATENCY_TYPE_VIEW_COUNT_HANDLER = "VIEW.COUNT.HANDLER"

# KEEP SYNC WITH module_monitoring.h


@pytest.fixture(autouse=True)
def before_and_after_test():
    print("Start")
    yield
    kill_redis()
    print("End")

def test_stream_exist_with_new_register():
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

    time.sleep(1.1)

    assert client1.exists(OPENTRACING_STREAM_NAME)


def test_stream_exist_with_multiple_new_register():
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

    # THIRD CLIENT
    client3 = connect_redis()
    response = client2.execute_command("VIEW.REGISTER " + cct_prepare.TEST_APP_NAME_3)
    assert response == "OK"

    time.sleep(1.1)

    assert client1.exists(OPENTRACING_STREAM_NAME)


def test_search_latency_metric_is_added():
    producer = connect_redis_with_start()
    flush_db(producer) # clean all db first
    cct_prepare.create_index(producer)

    total_count = 5

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
    assert str(response[5]) == '''['key', 'test_data:1', 'value', ['path', '$', 'data', '{"User":{"Address":{"ID":"2001"},"ID":1001,"PASSPORT":"aaa"}}'], 'key', 'test_data:2', 'value', ['path', '$', 'data', '{"User":{"Address":{"ID":"2002"},"ID":1002,"PASSPORT":"aaa"}}'], 'key', 'test_data:3', 'value', ['path', '$', 'data', '{"User":{"Address":{"ID":"2003"},"ID":1003,"PASSPORT":"aaa"}}']]'''

    # CHECK MONITORING STREAM
    from_stream = client1.xread( streams={OPENTRACING_STREAM_NAME:0} )
    #json_data = str(from_stream[0][1][0][1][OPENTRACING_KEY_NAME])
    #d = json.loads(json_data)
    print(from_stream)
    #assert d[METRIC_METRIC_KEY][METRIC_NAME_KEY] == '''OPENTRACING:app1:VIEW.SEARCH test_index * LOAD 1 @__key SORTBY 3 @User.ID @__key ASC LIMIT 0 3'''



def test_count_latency_metric_is_added():
    producer = connect_redis_with_start()
    flush_db(producer) # clean all db first
    cct_prepare.create_index(producer)

    total_count = 5

    # ADD INITIAL DATA
    for i in range(1, total_count):
        d = cct_prepare.generate_single_object(1000 + i, 2000 + i, "aaa")
        key = cct_prepare.TEST_INDEX_PREFIX + str(i)
        producer.json().set(key, Path.root_path(), d)

    # FIRST CLIENT
    client1 = connect_redis()
    response = client1.execute_command("VIEW.REGISTER " + cct_prepare.TEST_APP_NAME_1)

    response = client1.execute_command("VIEW.COUNT " + cct_prepare.TEST_INDEX_NAME + " *")
    assert str(response) == '''['count_query_id', 1, 'count_query', 'test_index *', 'count', 4]'''

    # CHECK MONITORING STREAM
    from_stream = client1.xread( streams={OPENTRACING_STREAM_NAME:0} )
    json_data = str(from_stream[0][1][0][1][OPENTRACING_KEY_NAME])
    d = json.loads(json_data)
    #print(d)
    assert d[METRIC_METRIC_KEY][METRIC_NAME_KEY] == '''OPENTRACING:app1:VIEW.COUNT test_index * LIMIT 0 0'''


def test_search_handler_latency_metric_is_added():
    producer = connect_redis_with_start()
    flush_db(producer) # clean all db first
    cct_prepare.create_index(producer)

    total_count = 5

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
    assert str(response[5]) == '''['key', 'test_data:1', 'value', ['path', '$', 'data', '{"User":{"Address":{"ID":"2001"},"ID":1001,"PASSPORT":"aaa"}}'], 'key', 'test_data:2', 'value', ['path', '$', 'data', '{"User":{"Address":{"ID":"2002"},"ID":1002,"PASSPORT":"aaa"}}'], 'key', 'test_data:3', 'value', ['path', '$', 'data', '{"User":{"Address":{"ID":"2003"},"ID":1003,"PASSPORT":"aaa"}}']]'''

    time.sleep(1.1)

    last_id = '0'
    try:
        while True:
            entries = client1.xread({OPENTRACING_STREAM_NAME: last_id}, count=1)
            if entries:
                for stream, entry_list in entries:
                    for entry_id, entry_data in entry_list:
                        print(f"ID: {entry_id}, Data: {entry_data}")
                        last_id = entry_id
            else:
                print("No more entries available.")
                break
    except KeyboardInterrupt:
        print("Stopped reading stream.")

def test_count_handler_latency_metric_is_added():
    producer = connect_redis_with_start()
    flush_db(producer) # clean all db first
    cct_prepare.create_index(producer)

    total_count = 5

    # ADD INITIAL DATA
    for i in range(1, total_count):
        d = cct_prepare.generate_single_object(1000 + i, 2000 + i, "aaa")
        key = cct_prepare.TEST_INDEX_PREFIX + str(i)
        producer.json().set(key, Path.root_path(), d)

    # FIRST CLIENT
    client1 = connect_redis()
    response = client1.execute_command("VIEW.REGISTER " + cct_prepare.TEST_APP_NAME_1)

    response = client1.execute_command("VIEW.COUNT " + cct_prepare.TEST_INDEX_NAME + " *")
    assert str(response) == '''['count_query_id', 1, 'count_query', 'test_index *', 'count', 4]'''

    time.sleep(1.1)

    last_id = '0'
    try:
        while True:
            entries = client1.xread({OPENTRACING_STREAM_NAME: last_id}, count=1)
            if entries:
                for stream, entry_list in entries:
                    for entry_id, entry_data in entry_list:
                        print(f"ID: {entry_id}, Data: {entry_data}")
                        last_id = entry_id
            else:
                print("No more entries available.")
                break
    except KeyboardInterrupt:
        print("Stopped reading stream.")

