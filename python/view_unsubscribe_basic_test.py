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

def test_single_client_single_query_unsubscribe():
    producer = connect_redis_with_start()
    flush_db(producer) # clean all db first
    cct_prepare.create_index(producer)

    # ADD INITIAL DATA
    d = cct_prepare.generate_single_object(1000 , 2000, "aaa")
    key = cct_prepare.TEST_INDEX_PREFIX + str(1)
    producer.json().set(key, Path.root_path(), d)

    query_id = 0

    # FIRST CLIENT
    client1 = connect_redis()
    client1.execute_command("VIEW.REGISTER " + cct_prepare.TEST_APP_NAME_1)
    result = client1.execute_command("VIEW.SEARCH " + cct_prepare.TEST_INDEX_NAME +  " @User\\.PASSPORT:{" + "aaa" + "}" + cct_prepare.QUERY_FULL_POSTFIX)
    print("VIEW.SEARCH response : " + str(result))
    assert query_id ==  result[0]

    # CHECK STREAM
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    print("FIRST STREAM : " + str(from_stream))
    #TRIM STREAM
    client1.xtrim(cct_prepare.TEST_APP_NAME_1 , 0)
    #CHECK STREAM AFTER TRIM 
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    print("AFTER TRIM STREAM : " +  str(from_stream))

    #UPDATE DATA 
    d = cct_prepare.generate_single_object(1001  , 2000, "aaa")
    key = cct_prepare.TEST_INDEX_PREFIX + str(1)
    producer.json().set(key, Path.root_path(), d)

    time.sleep(1.1)

    # CHECK STREAM
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    print("AFTER UPDATE STREAM : " + str(from_stream))

    #TRIM STREAM
    client1.xtrim(cct_prepare.TEST_APP_NAME_1 , 0)
    #CHECK STREAM AFTER TRIM 
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    print("AFTER TRIM STREAM : " +  str(from_stream))

    result = client1.execute_command("VIEW.SEARCH.UNSUBSCRIBE " +  str(query_id))
    print("VIEW.SEARCH.UNSUBSCRIBE response : " +  str(result))

    time.sleep(1.1)

    #UPDATE DATA 
    d = cct_prepare.generate_single_object(1002  , 2000, "aaa")
    key = cct_prepare.TEST_INDEX_PREFIX + str(1)
    producer.json().set(key, Path.root_path(), d)

    time.sleep(0.1)

    # CHECK STREAM
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    print("AFTER UPDATE and UNSUBSCRIBE STREAM : " + str(from_stream))




