from manage_redis import kill_redis, connect_redis_with_start, connect_redis
from manage_redis_data import flush_db, create_index, add_json_data, TEST_INDEX_NAME
import pytest
import cct_prepare
from redis.commands.json.path import Path
from redis.commands.search.query import GeoFilter, NumericFilter, Query
from redis.commands.search.result import Result
import time
import json

@pytest.fixture(autouse=True)
def before_and_after_test():
    print("Start")
    yield
    kill_redis()
    print("End")

def test_view_scroll_return_basic():
    producer = connect_redis_with_start()
    flush_db(producer) # clean all db first
    cct_prepare.create_index(producer)

    # ADD INITIAL DATA
    for i in range(20):
        d = cct_prepare.generate_single_object(1000 + i , 2000, "aaa")
        key = cct_prepare.TEST_INDEX_PREFIX + str(1 + i)
        producer.json().set(key, Path.root_path(), d)
    
    # FIRST CLIENT
    client1 = connect_redis()
    client1.execute_command("VIEW.REGISTER " + cct_prepare.TEST_APP_NAME_1)
    
    response = client1.execute_command("VIEW.SEARCH " + cct_prepare.TEST_INDEX_NAME + " @User\\.PASSPORT:{" + "aaa" + "} SORTBY 2 User.ID DESC LIMIT 0 3")
    print("\nVIEW.SEARCH RESPONSE : " + str(response))

    query_id = int(response[1])
    assert query_id == 1
    response_size = int(len(response[5])/4)
    assert response_size == 3

    response = client1.execute_command("VIEW.SEARCH.SCROLL " + str(query_id) + " 10 3")
    print("\nVIEW.SEARCH.SCROLL RESPONSE : " +str(response))


def test_view_scroll_and_search_same_format():
    producer = connect_redis_with_start()
    flush_db(producer) # clean all db first
    cct_prepare.create_index(producer)

    # ADD INITIAL DATA
    for i in range(20):
        d = cct_prepare.generate_single_object(1000 + i , 2000 - i, "aaa")
        key = cct_prepare.TEST_INDEX_PREFIX + str(1 + i)
        producer.json().set(key, Path.root_path(), d)
    
    # FIRST CLIENT
    client1 = connect_redis()
    client1.execute_command("VIEW.REGISTER " + cct_prepare.TEST_APP_NAME_1)
    response = client1.execute_command("VIEW.SEARCH " + cct_prepare.TEST_INDEX_NAME + " @User\\.PASSPORT:{" + "aaa" + "}" + " SORTBY 1 User.ID LIMIT 0 3")
    print(str(response))

    response = client1.execute_command("VIEW.SEARCH.SCROLL " + str(1) + " 3 3")
    print(str(response))

    response = client1.execute_command("VIEW.SEARCH.SCROLL " + str(1) + " 6 3")
    print(str(response))


def test_view_scroll_return_non_existing_query():
    producer = connect_redis_with_start()
    flush_db(producer) # clean all db first
    cct_prepare.create_index(producer)

    # ADD INITIAL DATA
    for i in range(20):
        d = cct_prepare.generate_single_object(1000 + i , 2000, "aaa")
        key = cct_prepare.TEST_INDEX_PREFIX + str(1 + i)
        producer.json().set(key, Path.root_path(), d)
    
    # FIRST CLIENT
    client1 = connect_redis()
    client1.execute_command("VIEW.REGISTER " + cct_prepare.TEST_APP_NAME_1)
    
    response = client1.execute_command("VIEW.SEARCH " + cct_prepare.TEST_INDEX_NAME + " @User\\.PASSPORT:{" + "aaa" + "} SORTBY 2 INVALID DESC LIMIT 0 3 DIALECT 4")
    print("\nVIEW.SEARCH RESPONSE : " + str(response))

    query_id = int(response[0])
    assert query_id == 1
    response_size = int((len(response)-2)/2)
    assert response_size == 3

    with pytest.raises(Exception) as e_info:
        response = client1.execute_command("VIEW.SEARCH.SCROLL " + str(query_id+1) + " 10 3")
    assert "ResponseError" in str(e_info)
    print("\nVIEW.SEARCH.SCROLL RESPONSE : " +str(response))


def test_view_scroll_return_json_format_dialect_4():
    producer = connect_redis_with_start()
    flush_db(producer) # clean all db first
    cct_prepare.create_index(producer)

    # ADD INITIAL DATA
    for i in range(20):
        d = cct_prepare.generate_single_object(1000 + i , 2000-i, "aaa")
        key = cct_prepare.TEST_INDEX_PREFIX + str(i)
        producer.json().set(key, Path.root_path(), d)
    
    # FIRST CLIENT
    client1 = connect_redis()
    client1.execute_command("VIEW.REGISTER " + cct_prepare.TEST_APP_NAME_1)
    
    response = client1.execute_command("VIEW.SEARCH " + cct_prepare.TEST_INDEX_NAME + " @User\\.PASSPORT:{" + "aaa" + "} SORTBY 2 User.ID DESC LIMIT 0 3")
    print("\nVIEW.SEARCH RESPONSE        : " + str(response))

    query_id = int(response[0])
    assert query_id == 1
    response_size = int((len(response)-2)/2)
    assert response_size == 3

    response = client1.execute_command("VIEW.SEARCH.SCROLL " + str(query_id) + " 10 3")
    print("\nVIEW.SEARCH.SCROLL RESPONSE : " +str(response))


def test_view_scroll_return_json_format_dialect_1():
    producer = connect_redis_with_start()
    flush_db(producer) # clean all db first
    cct_prepare.create_index(producer)

    # ADD INITIAL DATA
    for i in range(20):
        d = cct_prepare.generate_single_object(1000 + i , 2000-i, "aaa")
        key = cct_prepare.TEST_INDEX_PREFIX + str(i)
        producer.json().set(key, Path.root_path(), d)
    
    # FIRST CLIENT
    client1 = connect_redis()
    client1.execute_command("VIEW.REGISTER " + cct_prepare.TEST_APP_NAME_1)
    
    response = client1.execute_command("VIEW.SEARCH " + cct_prepare.TEST_INDEX_NAME + " @User\\.PASSPORT:{" + "aaa" + "} SORTBY 2 User.Address.ID DESC LIMIT 0 3")
    print("\nVIEW.SEARCH RESPONSE        : " + str(response))

    query_id = int(response[0])
    assert query_id == 1
    response_size = int((len(response)-2)/2)
    assert response_size == 3

    response = client1.execute_command("VIEW.SEARCH.SCROLL " + str(query_id) + " 10 3")
    print("\nVIEW.SEARCH.SCROLL RESPONSE : " +str(response))


def test_view_scroll_return_doesnt_write_stream():
    producer = connect_redis_with_start()
    flush_db(producer) # clean all db first
    cct_prepare.create_index(producer)

    # ADD INITIAL DATA
    for i in range(20):
        d = cct_prepare.generate_single_object(1000 + i , 2000, "aaa")
        key = cct_prepare.TEST_INDEX_PREFIX + str(1 + i)
        producer.json().set(key, Path.root_path(), d)
    
    # FIRST CLIENT
    client1 = connect_redis()
    client1.execute_command("VIEW.REGISTER " + cct_prepare.TEST_APP_NAME_1)

    time.sleep(1.1)

    # CHECK STREAM
    from_stream = client1.xread( count=4, streams={cct_prepare.TEST_APP_NAME_1:1} )
    print("\nSTREAM BEFORE SEARCH : " +str(from_stream))
    
    response = client1.execute_command("VIEW.SEARCH " + cct_prepare.TEST_INDEX_NAME + " * SORTBY 2 User.ID ASC LIMIT 0 1")
    print("\nVIEW.SEARCH RESPONSE : " + str(response))

    time.sleep(1.1)

    # CHECK STREAM
    from_stream = client1.xread( count=4,  streams={cct_prepare.TEST_APP_NAME_1:1} )
    print("\nSTREAM AFTER SEARCH : " +str(from_stream))

    response = client1.execute_command("VIEW.SEARCH.SCROLL " + str(1) + " 1 1")
    print("\nVIEW.SEARCH.SCROLL RESPONSE : " +str(response))

    time.sleep(2.2)

    # CHECK STREAM
    from_stream = client1.xread( count=4, streams={cct_prepare.TEST_APP_NAME_1:1} )
    print("\nnSTREAM AFTER SCROLL : " +str(from_stream))
    assert  str(from_stream) == "[]"



def test_view_multi_scroll_return_doesnt_write_stream():
    producer = connect_redis_with_start()
    time.sleep(1.1)
    flush_db(producer) # clean all db first
    time.sleep(1.1)
    cct_prepare.create_index(producer)

    # ADD INITIAL DATA
    for i in range(100000):
        d = cct_prepare.generate_single_object(1000 + i , 2000, "aaa")
        key = cct_prepare.TEST_INDEX_PREFIX + str(1 + i)
        producer.json().set(key, Path.root_path(), d)
    

    time.sleep(1.1)

    producer.execute_command("FT.CONFIG SET MAXSEARCHRESULTS 1000000")
    producer.execute_command("FT.CONFIG SET MAXAGGREGATERESULTS 1000000")

    time.sleep(1.1)

    # FIRST CLIENT
    client1 = connect_redis()
    client1.execute_command("VIEW.REGISTER " + cct_prepare.TEST_APP_NAME_1)

    time.sleep(1.1)

    # CHECK STREAM
    from_stream = client1.xread( count=4, streams={cct_prepare.TEST_APP_NAME_1:1} )
    #print("\nSTREAM BEFORE SEARCH : " +str(from_stream))
    
    response = client1.execute_command("VIEW.SEARCH " + cct_prepare.TEST_INDEX_NAME + " * SORTBY 2 User.ID ASC LIMIT 90000 1")
    #print("\nVIEW.SEARCH RESPONSE : " + str(response))

    time.sleep(1.1)

    # CHECK STREAM
    from_stream = client1.xread( count=4,  streams={cct_prepare.TEST_APP_NAME_1:1} )
    #print("\nSTREAM AFTER SEARCH : " +str(from_stream))


    for i in range(15):
        query = "VIEW.SEARCH.SCROLL 1 " + str(90000+i) + " 1"
        response = client1.execute_command(query)
        #print("VIEW.SEARCH.SCROLL RESPONSE : " +str(response))

    time.sleep(1.1)

    # CHECK STREAM
    from_stream = client1.xread( count=4,  streams={cct_prepare.TEST_APP_NAME_1:1} )
    assert from_stream == []


def test_view_multi_scroll_return_doesnt_write_stream_result_check():
    producer = connect_redis_with_start()
    time.sleep(1.1)
    flush_db(producer) # clean all db first
    time.sleep(1.1)
    cct_prepare.create_index(producer)
    
    total = 1000
    # ADD INITIAL DATA
    for i in range(1000):
        d = cct_prepare.generate_single_object(i , 2000, "aaa")
        key = cct_prepare.TEST_INDEX_PREFIX + str(1 + i)
        producer.json().set(key, Path.root_path(), d)
    

    #time.sleep(1.1)

    #producer.execute_command("FT.CONFIG SET MAXSEARCHRESULTS 1000000")
    #producer.execute_command("FT.CONFIG SET MAXAGGREGATERESULTS 1000000")

    #time.sleep(1.1)

    # FIRST CLIENT
    client1 = connect_redis()
    client1.execute_command("VIEW.REGISTER " + cct_prepare.TEST_APP_NAME_1)

    time.sleep(1.1)

    # CHECK STREAM
    from_stream = client1.xread( count=4, streams={cct_prepare.TEST_APP_NAME_1:1} )
    #print("\nSTREAM BEFORE SEARCH : " +str(from_stream))
    
    response = client1.execute_command("VIEW.SEARCH " + cct_prepare.TEST_INDEX_NAME + " * SORTBY 2 User.ID ASC LIMIT 0 1")
    #print("\nVIEW.SEARCH RESPONSE : " + str(response))

    time.sleep(1.1)

    # CHECK STREAM
    from_stream = client1.xread( count=4,  streams={cct_prepare.TEST_APP_NAME_1:1} )
    #print("\nSTREAM AFTER SEARCH : " +str(from_stream))

    for i in range(total):
        query = "VIEW.SEARCH.SCROLL 1 " + str(i) + " 1"
        response = client1.execute_command(query)
        assert response[1] == 1
        dictionary = json.loads(response[5][3][3])
        assert dictionary["User"]["ID"] == i
        

    time.sleep(1.1)

    # CHECK STREAM
    from_stream = client1.xread( count=4,  streams={cct_prepare.TEST_APP_NAME_1:1} )
    assert from_stream == []