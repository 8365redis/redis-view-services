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

def test_view_count_return_single():
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
    count_command_args = cct_prepare.TEST_INDEX_NAME + " @User\\.PASSPORT:{" + "aaa" + "}"
    response = client1.execute_command("VIEW.SEARCH.COUNT " + count_command_args)
    assert response == 1

    time.sleep(1.1)

    # CHECK STREAM
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    assert from_stream == []

    # ADD SECOND DATA
    d = cct_prepare.generate_single_object(1001 , 2001, "aaa")
    key = cct_prepare.TEST_INDEX_PREFIX + str(2)
    producer.json().set(key, Path.root_path(), d)

    time.sleep(1.1)

    # CHECK STREAM
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    assert len(from_stream[0][1]) == 1
    assert from_stream[0][1][0][1]['count'] == '2'

    time.sleep(1.1)

    # CHECK STREAM
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    #print("After Second Data Stream No Change: " + str(from_stream))
    assert len(from_stream[0][1]) == 1
    assert from_stream[0][1][0][1]['count'] == '2'


    #UNSUB COUNT
    resp = client1.execute_command("VIEW.COUNT.UNSUBSCRIBE " + count_command_args)
    assert resp == "OK"
    
    # ADD THIRD DATA
    d = cct_prepare.generate_single_object(1002 , 2002, "aaa")
    key = cct_prepare.TEST_INDEX_PREFIX + str(3)
    producer.json().set(key, Path.root_path(), d)

    time.sleep(1.1)

    # CHECK STREAM
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    assert len(from_stream[0][1]) == 1
    assert from_stream[0][1][0][1]['count'] == '2'


def test_client_has_no_count():
    producer = connect_redis_with_start()
    flush_db(producer) # clean all db first
    cct_prepare.create_index(producer)

    # ADD INITIAL DATA
    d = cct_prepare.generate_single_object(1000 , 2000, "aaa")
    key = cct_prepare.TEST_INDEX_PREFIX + str(1)
    producer.json().set(key, Path.root_path(), d)

    count_command_args = cct_prepare.TEST_INDEX_NAME + " @User\\.PASSPORT:{" + "aaa" + "}"

    # FIRST CLIENT
    client1 = connect_redis()
    client1.execute_command("VIEW.REGISTER " + cct_prepare.TEST_APP_NAME_1)

    #UNSUB COUNT
    resp = client1.execute_command("VIEW.COUNT.UNSUBSCRIBE " + count_command_args)
    assert resp == "Client doesnt have count operation"

def test_client_has_no_matching_count():
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
    count_command_args = cct_prepare.TEST_INDEX_NAME + " @User\\.PASSPORT:{" + "aaa" + "}"
    response = client1.execute_command("VIEW.SEARCH.COUNT " + count_command_args)
    assert response == 1

    time.sleep(1.1)

    # CHECK STREAM
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    assert from_stream == []

    # ADD SECOND DATA
    d = cct_prepare.generate_single_object(1001 , 2001, "aaa")
    key = cct_prepare.TEST_INDEX_PREFIX + str(2)
    producer.json().set(key, Path.root_path(), d)

    time.sleep(1.1)

    # CHECK STREAM
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    assert len(from_stream[0][1]) == 1
    assert from_stream[0][1][0][1]['count'] == '2'

    time.sleep(1.1)

    # CHECK STREAM
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    #print("After Second Data Stream No Change: " + str(from_stream))
    assert len(from_stream[0][1]) == 1
    assert from_stream[0][1][0][1]['count'] == '2'


    #UNSUB COUNT
    non_existing_count_command_args = cct_prepare.TEST_INDEX_NAME + " I DO NOT EXIST"
    resp = client1.execute_command("VIEW.COUNT.UNSUBSCRIBE " + non_existing_count_command_args)
    assert resp == "Client doesnt have matching count operation"
    
    # ADD THIRD DATA
    d = cct_prepare.generate_single_object(1002 , 2002, "aaa")
    key = cct_prepare.TEST_INDEX_PREFIX + str(3)
    producer.json().set(key, Path.root_path(), d)

    time.sleep(1.1)

    # CHECK STREAM
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    print(str(from_stream))
    assert len(from_stream[0][1]) == 2
    assert from_stream[0][1][1][1]['count'] == '3'
