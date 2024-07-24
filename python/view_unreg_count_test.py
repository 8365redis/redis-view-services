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

def test_single_client_single_count_after_unreg():
    producer = connect_redis_with_start()
    flush_db(producer) # clean all db first
    cct_prepare.create_index(producer)

    # ADD INITIAL DATA
    d = cct_prepare.generate_single_object(1000 , 2000, "aaa")
    key = cct_prepare.TEST_INDEX_PREFIX + str(1)
    producer.json().set(key, Path.root_path(), d)

    query_id = 1

    # FIRST CLIENT
    client1 = connect_redis()
    client1.execute_command("VIEW.REGISTER " + cct_prepare.TEST_APP_NAME_1)
    result = client1.execute_command("VIEW.COUNT " + cct_prepare.TEST_INDEX_NAME +  " * ")
    print("VIEW.COUNT response : " + str(result))
    assert query_id ==  result[1]

    # CHECK STREAM
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    print("FIRST STREAM : " + str(from_stream))
    #TRIM STREAM
    client1.xtrim(cct_prepare.TEST_APP_NAME_1 , 0)
    #CHECK STREAM AFTER TRIM 
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    print("AFTER TRIM STREAM : " +  str(from_stream))

    #UPDATE DATA 
    d = cct_prepare.generate_single_object(1001  , 2001, "bbb")
    key = cct_prepare.TEST_INDEX_PREFIX + str(2)
    producer.json().set(key, Path.root_path(), d)

    time.sleep(1.1)

    # CHECK STREAM
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    print("AFTER APPEND STREAM : " + str(from_stream))

    # FIRST CLIENT UNREGISTER
    response = client1.execute_command("VIEW.UNREGISTER " + cct_prepare.TEST_APP_NAME_1)
    assert response == "OK"

    time.sleep(1.1)

    # CHECK STREAM
    exists = client1.exists(cct_prepare.TEST_APP_NAME_1)
    assert exists == 0

    time.sleep(1.1)

    # CHECK STREAM
    exists = client1.exists(cct_prepare.TEST_APP_NAME_1)
    assert exists == 0