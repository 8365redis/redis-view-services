from manage_redis import kill_redis, connect_redis_with_start, connect_redis
from manage_redis_data import flush_db, create_index, add_json_data, TEST_INDEX_NAME
import pytest
import cct_prepare
from redis.commands.json.path import Path
import time

@pytest.fixture(autouse=True)
def before_and_after_test():
    print("Start")
    yield
    kill_redis()
    print("End")

def test_view_search_stream_write_new_start_single():
    producer = connect_redis_with_start()
    flush_db(producer) # clean all db first
    cct_prepare.create_index(producer)

    # ADD INITIAL DATA
    for i in range(3):
        d = cct_prepare.generate_single_object(1000 + i , 2000, "aaa")
        key = cct_prepare.TEST_INDEX_PREFIX + str(1 + i)
        producer.json().set(key, Path.root_path(), d)
    
    # FIRST CLIENT
    client1 = connect_redis()
    client1.execute_command("VIEW.REGISTER " + cct_prepare.TEST_APP_NAME_1)
    client1.execute_command("VIEW.SEARCH " + cct_prepare.TEST_INDEX_NAME + " @User\\.PASSPORT:{" + "aaa" + "} SORTBY User.ID ")
 
    time.sleep(1.1)
    
    # CHECK STREAM
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    print(str(from_stream))

    # TRIM STREAM
    client1.xtrim(cct_prepare.TEST_APP_NAME_1 , 0)

    # CHECK STREAM AFTER TRIM 
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    print(str(from_stream))

    time.sleep(1.1)

    # CHECK STREAM
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    print(str(from_stream))

def test_view_search_stream_write_update_single():
    producer = connect_redis_with_start()
    flush_db(producer) # clean all db first
    cct_prepare.create_index(producer)

    # ADD INITIAL DATA
    for i in range(1):
        d = cct_prepare.generate_single_object(1000 + i , 2000, "aaa")
        key = cct_prepare.TEST_INDEX_PREFIX + str(0 + i)
        producer.json().set(key, Path.root_path(), d)
    
    # FIRST CLIENT
    client1 = connect_redis()
    client1.execute_command("VIEW.REGISTER " + cct_prepare.TEST_APP_NAME_1)
    client1.execute_command("VIEW.SEARCH " + cct_prepare.TEST_INDEX_NAME + " @User\\.PASSPORT:{" + "aaa" + "} SORTBY User.ID ")

    time.sleep(1.1)
    
    # CHECK STREAM
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    print(str(from_stream))

    #TRIM STREAM
    client1.xtrim(cct_prepare.TEST_APP_NAME_1 , 0)
    #CHECK STREAM AFTER TRIM 
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    print(str(from_stream))

    time.sleep(0.1)

    #UPDATE DATA 
    d = cct_prepare.generate_single_object(1000  , 2001, "aaa")
    key = cct_prepare.TEST_INDEX_PREFIX + str(0)
    producer.json().set(key, Path.root_path(), d)
    #d = cct_prepare.generate_single_object(1001  , 2002, "aaa")
    #key = cct_prepare.TEST_INDEX_PREFIX + str(1)
    #producer.json().set(key, Path.root_path(), d)

    time.sleep(1.1)

    # CHECK STREAM
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    print(str(from_stream))


def test_view_search_stream_write_delete_single():
    producer = connect_redis_with_start()
    flush_db(producer) # clean all db first
    cct_prepare.create_index(producer)

    # ADD INITIAL DATA
    for i in range(1):
        d = cct_prepare.generate_single_object(1000 + i , 2000, "aaa")
        key = cct_prepare.TEST_INDEX_PREFIX + str(0 + i)
        producer.json().set(key, Path.root_path(), d)
    
    # FIRST CLIENT
    client1 = connect_redis()
    client1.execute_command("VIEW.REGISTER " + cct_prepare.TEST_APP_NAME_1)
    client1.execute_command("VIEW.SEARCH " + cct_prepare.TEST_INDEX_NAME + " @User\\.PASSPORT:{" + "aaa" + "} SORTBY User.ID ")

    time.sleep(1.1)
    
    # CHECK STREAM
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    print(str(from_stream))

    #TRIM STREAM
    client1.xtrim(cct_prepare.TEST_APP_NAME_1 , 0)
    #CHECK STREAM AFTER TRIM 
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    print(str(from_stream))

    time.sleep(0.1)

    #DELETE DATA 
    key = cct_prepare.TEST_INDEX_PREFIX + str(0)
    producer.json().delete(key)


    time.sleep(1.1)

    # CHECK STREAM
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    print(str(from_stream))


def test_view_search_stream_write_new_added_single():
    producer = connect_redis_with_start()
    flush_db(producer) # clean all db first
    cct_prepare.create_index(producer)

    # ADD INITIAL DATA
    for i in range(1):
        d = cct_prepare.generate_single_object(1000 + i , 2000, "aaa")
        key = cct_prepare.TEST_INDEX_PREFIX + str(0 + i)
        producer.json().set(key, Path.root_path(), d)
    
    # FIRST CLIENT
    client1 = connect_redis()
    client1.execute_command("VIEW.REGISTER " + cct_prepare.TEST_APP_NAME_1)
    client1.execute_command("VIEW.SEARCH " + cct_prepare.TEST_INDEX_NAME + " @User\\.PASSPORT:{" + "aaa" + "} SORTBY User.ID ASC")

    time.sleep(1.1)
    
    # CHECK STREAM
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    print(str(from_stream))

    #TRIM STREAM
    client1.xtrim(cct_prepare.TEST_APP_NAME_1 , 0)
    #CHECK STREAM AFTER TRIM 
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    print(str(from_stream))

    time.sleep(0.1)

    #UPDATE DATA 
    d = cct_prepare.generate_single_object(1001  , 2002, "aaa")
    key = cct_prepare.TEST_INDEX_PREFIX + str(1)
    producer.json().set(key, Path.root_path(), d)

    time.sleep(1.1)

    # CHECK STREAM
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    print(str(from_stream))

