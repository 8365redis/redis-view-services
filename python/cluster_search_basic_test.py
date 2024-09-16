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

@pytest.mark.skipif(True ,
                    reason="Only run manually")
def test_view_search_stream_write_new_start_single():
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
    response = client1.execute_command("VIEW.SEARCH " + cct_prepare.TEST_INDEX_NAME + " @User\\.PASSPORT:{" + "aaa" + "} SORTBY 2 User.ID ASC LIMIT 0 3")
    print("VIEW.SEARCH RESPONSE : " + str(response))
    assert response[5][1] == 'test_data:1'
    assert response[5][5] == 'test_data:2'
    assert response[5][9] == 'test_data:3'

    time.sleep(1.1)
    
    # CHECK STREAM
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    print("STREAM BEFORE UPDATE: " + str(from_stream))
    assert from_stream == []

    #UPDATE DATA 
    d = cct_prepare.generate_single_object(1000  , 2100, "aaa")
    key = cct_prepare.TEST_INDEX_PREFIX + str(1)
    producer.json().set(key, Path.root_path(), d)

    time.sleep(1.1)

    # CHECK STREAM
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    print("STREAM AFTER UPDATE: " + str(from_stream))
    assert from_stream[0][1][0][1]["query_id"] == '1'

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


@pytest.mark.skipif(True ,
                    reason="Only run manually")
def test_view_search_stream_write_new_start_double():
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

    time.sleep(2.0)

    # ADD INITIAL DATA
    for i in range(10):
        d = cct_prepare.generate_single_object(1000 + i , 2000, "aaa")
        key = cct_prepare.TEST_INDEX_PREFIX + str(1 + i)
        producer.json().set(key, Path.root_path(), d)

    # ADD INITIAL DATA
    for i in range(10):
        d = cct_prepare.generate_single_object(1000 + i , 2000, "bbb")
        key = cct_prepare.TEST_INDEX_PREFIX + str(11 + i)
        producer.json().set(key, Path.root_path(), d)
    
    # FIRST CLIENT
    client1 = connect_redis()
    client1.execute_command("VIEW.REGISTER " + cct_prepare.TEST_APP_NAME_1)
    response = client1.execute_command("VIEW.SEARCH " + cct_prepare.TEST_INDEX_NAME + " @User\\.PASSPORT:{" + "aaa" + "} SORTBY 2 User.ID ASC LIMIT 0 3")
    print("VIEW.SEARCH RESPONSE : " + str(response))

    # SECOND QUERY
    response = client1.execute_command("VIEW.SEARCH " + cct_prepare.TEST_INDEX_NAME + " @User\\.PASSPORT:{" + "bbb" + "} SORTBY 2 User.ID ASC LIMIT 0 3")
    print("VIEW.SEARCH RESPONSE : " + str(response))

    time.sleep(1.1)
    
    # CHECK STREAM
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    print("STREAM BEFORE UPDATE: " + str(from_stream))
    assert from_stream == []

    #UPDATE DATA 
    d = cct_prepare.generate_single_object(1000  , 2111, "aaa")
    key = cct_prepare.TEST_INDEX_PREFIX + str(0)
    producer.json().set(key, Path.root_path(), d)

    #UPDATE DATA 
    d = cct_prepare.generate_single_object(1000  , 2222, "bbb")
    key = cct_prepare.TEST_INDEX_PREFIX + str(11)
    producer.json().set(key, Path.root_path(), d)


    time.sleep(1.1)

    # CHECK STREAM
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    print("STREAM AFTER UPDATE: " + str(from_stream))

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

