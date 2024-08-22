from manage_redis import kill_redis, connect_redis_with_start, connect_redis, connect_redis_replica_with_start, kill_replica_redis
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
    #Even no replica try to kill anyway
    kill_replica_redis(6380)
    kill_replica_redis(6381)
    kill_replica_redis(6382)
    print("End")


def test_view_count_stream_write_new_start_single():
    producer = connect_redis_with_start()
    flush_db(producer) # clean all db first
    cct_prepare.create_index(producer)

    #time.sleep(1.0)
    replica_port = 6380
    replica = connect_redis_replica_with_start(replica_port)
    resp = replica.execute_command("PING")
    assert resp == True

    time.sleep(2.0)

    # ADD INITIAL DATA
    for i in range(10):
        d = cct_prepare.generate_single_object(1000 + i , 2000, "aaa")
        key = cct_prepare.TEST_INDEX_PREFIX + str(1 + i)
        producer.json().set(key, Path.root_path(), d)
    
    # FIRST CLIENT
    client1 = connect_redis()
    client1.execute_command("VIEW.REGISTER " + cct_prepare.TEST_APP_NAME_1)
    response = client1.execute_command("VIEW.COUNT " + cct_prepare.TEST_INDEX_NAME + " *")
    print("VIEW.COUNT RESPONSE : " + str(response))
    assert response[5] == 10

    time.sleep(1.1)
    
    # CHECK STREAM
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    print("STREAM BEFORE DELETE: " + str(from_stream))
    assert from_stream == []

    # DELETE DATA 
    key = cct_prepare.TEST_INDEX_PREFIX + str(1)
    producer.delete(key)

    time.sleep(1.1)

    # CHECK STREAM
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    print("STREAM AFTER DELETE: " + str(from_stream))
    assert from_stream[0][1][0][1]["count"] == '9'

    # TRIM STREAM
    client1.xtrim(cct_prepare.TEST_APP_NAME_1 , 0)

    # CHECK STREAM AFTER TRIM 
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    print("STREAM AFTER TRIM: " + str(from_stream))
    assert from_stream == []

    time.sleep(1.1)

    # CHECK STREAM
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    print("STREAM AFTER TRIM and DELAY: " + str(from_stream))
    assert from_stream == []

    # DELETE DATA AGAIN
    key = cct_prepare.TEST_INDEX_PREFIX + str(2)
    producer.delete(key)

    time.sleep(1.1)

    # CHECK STREAM
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    print("STREAM AFTER SECOND DELETE: " + str(from_stream))
    assert from_stream[0][1][0][1]["count"] == '8'

    time.sleep(1.1)

    # CHECK STREAM
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    print("STREAM AFTER DELAY: " + str(from_stream))
    assert len(from_stream[0]) == 2


def test_view_count_stream_write_new_start_with_three():
    producer = connect_redis_with_start()
    flush_db(producer) # clean all db first
    cct_prepare.create_index(producer)

    #time.sleep(1.0)
    replica_port = 6380
    replica1 = connect_redis_replica_with_start(replica_port)
    resp = replica1.execute_command("PING")
    assert resp == True
    replica_port = 6381
    replica2 = connect_redis_replica_with_start(replica_port)
    resp = replica2.execute_command("PING")
    assert resp == True
    replica_port = 6382
    replica2 = connect_redis_replica_with_start(replica_port)
    resp = replica2.execute_command("PING")
    assert resp == True

    time.sleep(3.0)

    total = 10
    # ADD INITIAL DATA
    for i in range(total):
        d = cct_prepare.generate_single_object(1000 + i , 2000, "aaa")
        key = cct_prepare.TEST_INDEX_PREFIX + str(i)
        producer.json().set(key, Path.root_path(), d)

    # ADD INITIAL DATA
    for i in range(total):
        d = cct_prepare.generate_single_object(1000 + i , 2000, "bbb")
        key = cct_prepare.TEST_INDEX_PREFIX + str(total + i + 1)
        producer.json().set(key, Path.root_path(), d)

    time.sleep(1.0)
    
    # FIRST CLIENT
    client1 = connect_redis()
    client1.execute_command("VIEW.REGISTER " + cct_prepare.TEST_APP_NAME_1)
    response = client1.execute_command("VIEW.COUNT " + cct_prepare.TEST_INDEX_NAME + " @User\\.PASSPORT:{" + "aaa" + "} ")
    print("VIEW.COUNT RESPONSE 1: " + str(response))

    # SECOND QUERY
    response = client1.execute_command("VIEW.COUNT " + cct_prepare.TEST_INDEX_NAME + " @User\\.PASSPORT:{" + "bbb" + "} ")
    print("VIEW.COUNT RESPONSE 2: " + str(response))

    # THIRD QUERY
    response = client1.execute_command("VIEW.COUNT " + cct_prepare.TEST_INDEX_NAME + " *")
    print("VIEW.COUNT RESPONSE 3: " + str(response))

    time.sleep(1.1)
    
    # CHECK STREAM
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    print("STREAM BEFORE DELETE: " + str(from_stream))
    assert from_stream == []

    #DELETE DATA 
    key = cct_prepare.TEST_INDEX_PREFIX + str(1)
    producer.delete(key)

    time.sleep(1.1)

    #DELETE DATA 
    key = cct_prepare.TEST_INDEX_PREFIX + str(12)
    producer.delete(key)

    time.sleep(1.1)

    # CHECK STREAM
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    print("STREAM AFTER DELETE: " + str(from_stream))
    assert from_stream[0][1][0][1]["count"] == '19'
    assert from_stream[0][1][1][1]["count"] == '9'
    assert from_stream[0][1][2][1]["count"] == '18'
    assert from_stream[0][1][3][1]["count"] == '9'


    # TRIM STREAM
    client1.xtrim(cct_prepare.TEST_APP_NAME_1 , 0)

    # CHECK STREAM AFTER TRIM 
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    print("STREAM AFTER TRIM: " + str(from_stream))
    assert from_stream == []

    time.sleep(1.1)

    # CHECK STREAM
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    print("STREAM AFTER TRIM and DELAY: " + str(from_stream))
    assert from_stream == []


def test_view_count_stream_write_new_start_with_three_2():
    producer = connect_redis_with_start()
    flush_db(producer) # clean all db first
    cct_prepare.create_index(producer)

    #time.sleep(1.0)
    replica_port = 6380
    replica1 = connect_redis_replica_with_start(replica_port)
    resp = replica1.execute_command("PING")
    assert resp == True
    replica_port = 6381
    replica2 = connect_redis_replica_with_start(replica_port)
    resp = replica2.execute_command("PING")
    assert resp == True
    replica_port = 6382
    replica2 = connect_redis_replica_with_start(replica_port)
    resp = replica2.execute_command("PING")
    assert resp == True

    time.sleep(3.0)

    total = 10
    # ADD INITIAL DATA
    for i in range(total):
        d = cct_prepare.generate_single_object(1000 + i , 2000, "aaa")
        key = cct_prepare.TEST_INDEX_PREFIX + str(i)
        producer.json().set(key, Path.root_path(), d)

    # ADD INITIAL DATA
    for i in range(total):
        d = cct_prepare.generate_single_object(1000 + i , 2000, "bbb")
        key = cct_prepare.TEST_INDEX_PREFIX + str(total + i + 1)
        producer.json().set(key, Path.root_path(), d)

    time.sleep(1.0)
    
    # FIRST CLIENT
    client1 = connect_redis()
    client1.execute_command("VIEW.REGISTER " + cct_prepare.TEST_APP_NAME_1)
    response = client1.execute_command("VIEW.COUNT " + cct_prepare.TEST_INDEX_NAME + " @User\\.PASSPORT:{" + "aaa" + "} ")
    print("VIEW.COUNT RESPONSE 1: " + str(response))

    # SECOND QUERY
    response = client1.execute_command("VIEW.COUNT " + cct_prepare.TEST_INDEX_NAME + " @User\\.PASSPORT:{" + "bbb" + "} ")
    print("VIEW.COUNT RESPONSE 2: " + str(response))

    # THIRD QUERY
    response = client1.execute_command("VIEW.COUNT " + cct_prepare.TEST_INDEX_NAME + " *")
    print("VIEW.COUNT RESPONSE 3: " + str(response))

    time.sleep(1.1)
    
    # CHECK STREAM
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    print("STREAM BEFORE DELETE: " + str(from_stream))
    assert from_stream == []

    #DELETE DATA 
    key = cct_prepare.TEST_INDEX_PREFIX + str(1)
    producer.delete(key)

    #DELETE DATA 
    key = cct_prepare.TEST_INDEX_PREFIX + str(12)
    producer.delete(key)

    time.sleep(1.1)

    # CHECK STREAM
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    print("STREAM AFTER DELETE: " + str(from_stream))
    assert from_stream[0][1][0][1]["count"] == '18'
    assert from_stream[0][1][1][1]["count"] == '9'
    assert from_stream[0][1][2][1]["count"] == '9'

    # TRIM STREAM
    client1.xtrim(cct_prepare.TEST_APP_NAME_1 , 0)

    # CHECK STREAM AFTER TRIM 
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    print("STREAM AFTER TRIM: " + str(from_stream))
    assert from_stream == []

    time.sleep(1.1)

    # CHECK STREAM
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    print("STREAM AFTER TRIM and DELAY: " + str(from_stream))
    assert from_stream == []