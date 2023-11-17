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

def test_basic_view_query():
    producer = connect_redis()
    flush_db(producer) # clean all db first
    cct_prepare.create_index(producer)


    # ADD INITIAL DATA
    d = cct_prepare.generate_single_object(1000 , 2000, "aaa")
    key = cct_prepare.TEST_INDEX_PREFIX + str(1)
    producer.json().set(key, Path.root_path(), d)

    # FIRST CLIENT
    client1 = connect_redis()
    client1.execute_command("VIEW.REGISTER " + cct_prepare.TEST_APP_NAME_1)

    client1.execute_command("VIEW.SEARCH ",cct_prepare.TEST_INDEX_NAME , "@User\\.PASSPORT:{" + "aaa" + "}")

    res =  client1.execute_command("FT.SEARCH",cct_prepare.TEST_INDEX_NAME , "@User\\.PASSPORT:{" + "aaa" + "}")
    print(str(res))

    time.sleep(1.5)

    # Check stream 
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    print(from_stream)

    # UPDATE DATA
    d = cct_prepare.generate_single_object(1001 , 2001, "aaa")
    producer.json().set(key, Path.root_path(), d)

    res =  client1.execute_command("FT.SEARCH ",cct_prepare.TEST_INDEX_NAME , "@User\\.PASSPORT:{" + "aaa" + "}")
    print(str(res))

    time.sleep(1.5)

    # Check stream 
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    print(from_stream)

    # UPDATE DATA
    d = cct_prepare.generate_single_object(1002 , 2002, "aaa")
    producer.json().set(key, Path.root_path(), d)

    time.sleep(1.5)

    res =  client1.execute_command("FT.SEARCH ",cct_prepare.TEST_INDEX_NAME , "@User\\.PASSPORT:{" + "aaa" + "}")
    print(str(res))

    # Check stream 
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    print(from_stream)

    time.sleep(1.5)
