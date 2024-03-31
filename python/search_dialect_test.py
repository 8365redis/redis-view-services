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

def test_dialect_version():
    producer = connect_redis_with_start()
    flush_db(producer) # clean all db first

    # FIRST CLIENT
    client1 = connect_redis()
    resp = client1.execute_command("FT.CONFIG GET DEFAULT_DIALECT")
    assert resp[0][1] == '1'

def test_dialect_4_is_added():
    producer = connect_redis_with_start()
    flush_db(producer) # clean all db first
    cct_prepare.create_index(producer)

    # ADD INITIAL DATA
    for i in range(30):
        d = cct_prepare.generate_single_object(1000 + i , 2000 - i, "aaa")
        key = cct_prepare.TEST_INDEX_PREFIX + str(0 + i)
        producer.json().set(key, Path.root_path(), d)
    
    # FIRST QUERY
    client1 = connect_redis()
    client1.execute_command("VIEW.REGISTER " + cct_prepare.TEST_APP_NAME_1)
    response = client1.execute_command("VIEW.SEARCH " + cct_prepare.TEST_INDEX_NAME + " @User\\.ID:{" + "1001" + "} SORTBY User.ID ASC LIMIT 0 3")
    print("Response for query 1:", str(response))
    response = client1.execute_command("VIEW.SEARCH " + cct_prepare.TEST_INDEX_NAME + " @User\\.ID:{" + "1001" + "} SORTBY User.ID DESC LIMIT 0 3")
    print("Response for query 2:", str(response))

    response = client1.execute_command("VIEW.SEARCH " + cct_prepare.TEST_INDEX_NAME + " @User\\.ID:{" + "1002" + "} SORTBY User.PASSPORT ASC LIMIT 0 3")    
    print("Response for query 3:", str(response))
    response = client1.execute_command("VIEW.SEARCH " + cct_prepare.TEST_INDEX_NAME + " @User\\.ID:{" + "1002" + "} SORTBY User.PASSPORT DESC LIMIT 0 3")    
    print("Response for query 4:", str(response))

    response = client1.execute_command("VIEW.SEARCH " + cct_prepare.TEST_INDEX_NAME + " @User\\.ID:{" + "1003" + "} SORTBY User.Address.ID ASC LIMIT 0 3")    
    print("Response for query 5:", str(response))
    response = client1.execute_command("VIEW.SEARCH " + cct_prepare.TEST_INDEX_NAME + " @User\\.ID:{" + "1003" + "} SORTBY User.Address.ID DESC LIMIT 0 3")    
    print("Response for query 6:", str(response))


    # SECOND QUERY
    response = client1.execute_command("VIEW.SEARCH " + cct_prepare.TEST_INDEX_NAME + " @User\\.PASSPORT:{" + "aaa" + "}" + cct_prepare.QUERY_FULL_POSTFIX)
    print(str(response))