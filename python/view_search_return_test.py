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

def test_view_search_return_single():
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
    response = client1.execute_command("VIEW.SEARCH " + cct_prepare.TEST_INDEX_NAME + " @User\\.PASSPORT:{" + "aaa" + "}" + cct_prepare.QUERY_FULL_POSTFIX)
    assert str(response) == """['query_id', 1, 'count', 1, 'result', ['key', 'test_data:1', 'value', ['path', '$', 'data', '{"User":{"Address":{"ID":"2000"},"ID":1000,"PASSPORT":"aaa"}}']]]"""

def test_view_search_return_multi():
    producer = connect_redis_with_start()
    flush_db(producer) # clean all db first
    cct_prepare.create_index(producer)

    # ADD INITIAL DATA
    for i in range(10):
        d = cct_prepare.generate_single_object(1000 , 2000 + i, "aaa")
        key = cct_prepare.TEST_INDEX_PREFIX + str(1 + i)
        producer.json().set(key, Path.root_path(), d)

    # FIRST CLIENT
    client1 = connect_redis()
    client1.execute_command("VIEW.REGISTER " + cct_prepare.TEST_APP_NAME_1)
    response = client1.execute_command("VIEW.SEARCH " + cct_prepare.TEST_INDEX_NAME +  " @User\\.PASSPORT:{" + "aaa" + "}" + cct_prepare.QUERY_FULL_POSTFIX)
    #print(str(response))
    assert response[3] == 10
    assert str(response[5][3][3]) == '''{"User":{"Address":{"ID":"2000"},"ID":1000,"PASSPORT":"aaa"}}'''
    assert str(response[5][7][3]) == '''{"User":{"Address":{"ID":"2009"},"ID":1000,"PASSPORT":"aaa"}}'''
    assert str(response[5][11][3]) == '''{"User":{"Address":{"ID":"2001"},"ID":1000,"PASSPORT":"aaa"}}'''
    assert str(response[5][15][3]) == '''{"User":{"Address":{"ID":"2002"},"ID":1000,"PASSPORT":"aaa"}}'''


def test_view_search_return_multi_excess():
    producer = connect_redis_with_start()
    flush_db(producer) # clean all db first
    cct_prepare.create_index(producer)

    # ADD INITIAL DATA
    for i in range(30):
        d = cct_prepare.generate_single_object(1000 + i, 2000, "aaa")
        key = cct_prepare.TEST_INDEX_PREFIX + str(1 + i)
        producer.json().set(key, Path.root_path(), d)

    # FIRST CLIENT
    client1 = connect_redis()
    client1.execute_command("VIEW.REGISTER " + cct_prepare.TEST_APP_NAME_1)
    response = client1.execute_command("VIEW.SEARCH " + cct_prepare.TEST_INDEX_NAME + " @User\\.PASSPORT:{" + "aaa" + "}" + cct_prepare.QUERY_FULL_POSTFIX)
    #print(str(response))
    assert response[3] == 30
    assert str(response[5][3][3]) == '''{"User":{"Address":{"ID":"2000"},"ID":1000,"PASSPORT":"aaa"}}'''
    assert str(response[5][7][3]) == '''{"User":{"Address":{"ID":"2000"},"ID":1001,"PASSPORT":"aaa"}}'''
    assert str(response[5][11][3]) == '''{"User":{"Address":{"ID":"2000"},"ID":1002,"PASSPORT":"aaa"}}'''
    assert str(response[5][15][3]) == '''{"User":{"Address":{"ID":"2000"},"ID":1003,"PASSPORT":"aaa"}}'''

def test_view_search_return_multi_excess_with_limit():
    producer = connect_redis_with_start()
    flush_db(producer) # clean all db first
    cct_prepare.create_index(producer)

    # ADD INITIAL DATAS
    for i in range(30):
        d = cct_prepare.generate_single_object(1000 + i, 2000, "aaa")
        key = cct_prepare.TEST_INDEX_PREFIX + str(1 + i)
        producer.json().set(key, Path.root_path(), d)

    # FIRST CLIENT
    client1 = connect_redis()
    client1.execute_command("VIEW.REGISTER " + cct_prepare.TEST_APP_NAME_1)
    response = client1.execute_command("VIEW.SEARCH " + cct_prepare.TEST_INDEX_NAME + " @User\\.PASSPORT:{" + "aaa" + "}" + cct_prepare.QUERY_FULL_POSTFIX)
    #print(str(response))
    assert response[1] == 1
    assert response[3] == 30
    assert str(response[5][3][3]) == '''{"User":{"Address":{"ID":"2000"},"ID":1000,"PASSPORT":"aaa"}}'''
    assert str(response[5][7][3]) == '''{"User":{"Address":{"ID":"2000"},"ID":1001,"PASSPORT":"aaa"}}'''
    assert str(response[5][11][3]) == '''{"User":{"Address":{"ID":"2000"},"ID":1002,"PASSPORT":"aaa"}}'''
    assert str(response[5][15][3]) == '''{"User":{"Address":{"ID":"2000"},"ID":1003,"PASSPORT":"aaa"}}'''

    time.sleep(2.1)

    response = client1.execute_command("VIEW.SEARCH " + cct_prepare.TEST_INDEX_NAME + " @User\\.PASSPORT:{" + "aaa" + "}" + cct_prepare.QUERY_FULL_POSTFIX)
    #print(str(response))
    assert response[1] == 2
    assert response[3] == 30
    assert str(response[5][3][3]) == '''{"User":{"Address":{"ID":"2000"},"ID":1000,"PASSPORT":"aaa"}}'''
    assert str(response[5][7][3]) == '''{"User":{"Address":{"ID":"2000"},"ID":1001,"PASSPORT":"aaa"}}'''
    assert str(response[5][11][3]) == '''{"User":{"Address":{"ID":"2000"},"ID":1002,"PASSPORT":"aaa"}}'''
    assert str(response[5][15][3]) == '''{"User":{"Address":{"ID":"2000"},"ID":1003,"PASSPORT":"aaa"}}'''


def test_view_search_no_return():
    producer = connect_redis_with_start()
    flush_db(producer) # clean all db first
    cct_prepare.create_index(producer)

    # ADD INITIAL DATA
    for i in range(5):
        d = cct_prepare.generate_single_object(1000 , 2000 + i, "aaa")
        key = cct_prepare.TEST_INDEX_PREFIX + str(1 + i)
        producer.json().set(key, Path.root_path(), d)

    # FIRST CLIENT
    client1 = connect_redis()
    client1.execute_command("VIEW.REGISTER " + cct_prepare.TEST_APP_NAME_1)
    response = client1.execute_command("VIEW.SEARCH " + cct_prepare.TEST_INDEX_NAME + " @User\\.PASSPORT:{" + "bbb" + "}" + cct_prepare.QUERY_FULL_POSTFIX)

    #print(response)
    assert response[1] == 1
    assert response[3] == 0
    assert response[5] == []


def test_view_search_query_id():
    producer = connect_redis_with_start()
    flush_db(producer) # clean all db first
    cct_prepare.create_index(producer)

    # ADD INITIAL DATA
    for i in range(5):
        d = cct_prepare.generate_single_object(1000 + i , 2000, "aaa")
        key = cct_prepare.TEST_INDEX_PREFIX + str(1 + i)
        producer.json().set(key, Path.root_path(), d)

    # FIRST CLIENT FIRST QUERY
    client1 = connect_redis()
    client1.execute_command("VIEW.REGISTER " + cct_prepare.TEST_APP_NAME_1)
    response = client1.execute_command("VIEW.SEARCH " + cct_prepare.TEST_INDEX_NAME + " @User\\.PASSPORT:{" + "1000" + "}" + cct_prepare.QUERY_FULL_POSTFIX)
    assert response[1] == 1
    assert response[3] == 0
    assert response[5] == []

    # FIRST CLIENT SECOND QUERY
    response = client1.execute_command("VIEW.SEARCH " + cct_prepare.TEST_INDEX_NAME + " @User\\.PASSPORT:{" + "1001" + "}" + cct_prepare.QUERY_FULL_POSTFIX)
    assert response[1] == 2
    assert response[3] == 0
    assert response[5] == []

    # FIRST CLIENT THIRD QUERY
    response = client1.execute_command("VIEW.SEARCH "+ cct_prepare.TEST_INDEX_NAME + " @User\\.PASSPORT:{" + "1002" + "}" + cct_prepare.QUERY_FULL_POSTFIX)
    assert response[1] == 3
    assert response[3] == 0
    assert response[5] == []

def test_view_search_without_limit():
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

    with pytest.raises(Exception) as e_info:
        client1.execute_command("VIEW.SEARCH " + cct_prepare.TEST_INDEX_NAME + " @User\\.PASSPORT:{" + "aaa" + "} SORTBY 1 User.ID")
    assert "ResponseError" in str(e_info)

def test_view_search_failed_query_cleaned():
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

    with pytest.raises(Exception) as e_info:
        client1.execute_command("VIEW.SEARCH " + cct_prepare.TEST_INDEX_NAME + " @User\\.PASSPORT:{" + "aaa" + "} SORTBY 1 User.ID")
    assert "ResponseError" in str(e_info)

    response = client1.execute_command("VIEW.SEARCH " + cct_prepare.TEST_INDEX_NAME + " @User\\.PASSPORT:{" + "aaa" + "}" + cct_prepare.QUERY_FULL_POSTFIX)
    assert response[1] == 1
    assert response[3] == 1
    assert str(response[5]) == '''['key', 'test_data:1', 'value', ['path', '$', 'data', '{"User":{"Address":{"ID":"2000"},"ID":1000,"PASSPORT":"aaa"}}']]'''

    with pytest.raises(Exception) as e_info:
        client1.execute_command("VIEW.SEARCH " + "NON_EXISTING_INDEX" + " @FAIL\\.ERROR:{" + "SHOULD_FAIL" + "}" + cct_prepare.QUERY_FULL_POSTFIX)
    assert "ResponseError" in str(e_info)

    response = client1.execute_command("VIEW.SEARCH " + cct_prepare.TEST_INDEX_NAME + " @User\\.PASSPORT:{" + "bbb" + "}" + cct_prepare.QUERY_FULL_POSTFIX)
    assert response[1] == 2
    assert response[3] == 0
    assert response[5] == []

def test_view_search_return_sort_limit():
    producer = connect_redis_with_start()
    flush_db(producer) # clean all db first
    cct_prepare.create_index(producer)

    # ADD INITIAL DATAS
    for i in range(20):
        d = cct_prepare.generate_single_object(1000 + i , 2000, "aaa")
        key = cct_prepare.TEST_INDEX_PREFIX + str(1 + i)
        producer.json().set(key, Path.root_path(), d)

    #FIRST QUERY
    client1 = connect_redis()
    client1.execute_command("VIEW.REGISTER " + cct_prepare.TEST_APP_NAME_1)

    response = client1.execute_command("ft.config get default_dialect")
    assert response[0][1] == '1'
    
    #print("QUERY : " + "VIEW.SEARCH " + cct_prepare.TEST_INDEX_NAME + " @User\\.PASSPORT:{" + "aaa" + "} SORTBY 1 User.ID LIMIT 0 3")
    #print("\nWith dialect 4 implicit\n")
    #print("VIEW.SEARCH:")
    response1 = client1.execute_command("VIEW.SEARCH " + cct_prepare.TEST_INDEX_NAME + " @User\\.PASSPORT:{" + "aaa" + "} SORTBY 1 User.ID LIMIT 0 3")
    #print(str(response1))
    assert str(response1[5][3][3]) == '''{"User":{"Address":{"ID":"2000"},"ID":1000,"PASSPORT":"aaa"}}'''
    assert str(response1[5][7][3]) == '''{"User":{"Address":{"ID":"2000"},"ID":1001,"PASSPORT":"aaa"}}'''
    assert str(response1[5][11][3]) == '''{"User":{"Address":{"ID":"2000"},"ID":1002,"PASSPORT":"aaa"}}'''
    assert response1[1] == 1
    resp1_val = response1[5]
    
    #print("\nWith dialect 4 explicit\n")
    #print("VIEW.SEARCH:")
    response2 = client1.execute_command("VIEW.SEARCH " + cct_prepare.TEST_INDEX_NAME + " @User\\.PASSPORT:{" + "aaa" + "} SORTBY 1 User.ID LIMIT 0 3 DIALECT 4")
    #print(str(response2))
    assert str(response2[5][3][3]) == '''{"User":{"Address":{"ID":"2000"},"ID":1000,"PASSPORT":"aaa"}}'''
    assert str(response2[5][7][3]) == '''{"User":{"Address":{"ID":"2000"},"ID":1001,"PASSPORT":"aaa"}}'''
    assert str(response2[5][11][3]) == '''{"User":{"Address":{"ID":"2000"},"ID":1002,"PASSPORT":"aaa"}}'''
    assert response2[1] == 2
    resp2_val = response2[5]

    assert resp1_val == resp2_val

    #print("\nWith DESC\n")
    #print("VIEW.SEARCH:")
    response3 = client1.execute_command("VIEW.SEARCH " + cct_prepare.TEST_INDEX_NAME + " @User\\.PASSPORT:{" + "aaa" + "} SORTBY 2 User.ID DESC LIMIT 0 3")
    #print(str(response3))
    assert str(response3[5][3][3]) == '''{"User":{"Address":{"ID":"2000"},"ID":1019,"PASSPORT":"aaa"}}'''
    assert str(response3[5][7][3]) == '''{"User":{"Address":{"ID":"2000"},"ID":1018,"PASSPORT":"aaa"}}'''
    assert str(response3[5][11][3]) == '''{"User":{"Address":{"ID":"2000"},"ID":1017,"PASSPORT":"aaa"}}'''
    assert response3[1] == 3
    resp3_val = response3[5]

    #print("\nWith DESC DIALECT 1\n")
    #print("VIEW.SEARCH:")
    response4 = client1.execute_command("VIEW.SEARCH " + cct_prepare.TEST_INDEX_NAME + " @User\\.PASSPORT:{" + "aaa" + "} SORTBY 2 User.ID DESC LIMIT 0 3 DIALECT 1")
    #print(str(response4))
    assert str(response4[5][3][3]) == '''{"User":{"Address":{"ID":"2000"},"ID":1019,"PASSPORT":"aaa"}}'''
    assert str(response4[5][7][3]) == '''{"User":{"Address":{"ID":"2000"},"ID":1018,"PASSPORT":"aaa"}}'''
    assert str(response4[5][11][3]) == '''{"User":{"Address":{"ID":"2000"},"ID":1017,"PASSPORT":"aaa"}}'''
    assert response4[1] == 4
    resp4_val = response4[5]

    assert resp3_val == resp4_val
    

def test_view_search_return_multi_with_wrong_sortby():
    producer = connect_redis_with_start()
    flush_db(producer) # clean all db first
    cct_prepare.create_index(producer)

    # ADD INITIAL DATAS
    for i in range(10):
        d = cct_prepare.generate_single_object(1000 , 2000 + i, "aaa")
        key = cct_prepare.TEST_INDEX_PREFIX + str(1 + i)
        producer.json().set(key, Path.root_path(), d)

    # FIRST CLIENT
    client1 = connect_redis()
    client1.execute_command("VIEW.REGISTER " + cct_prepare.TEST_APP_NAME_1)
    response = ""
    try:
        response = client1.execute_command("VIEW.SEARCH " + cct_prepare.TEST_INDEX_NAME +  " @User\\.PASSPORT:{" + "aaa" + "} SORTBY INVALID_SORTBY LIMIT 0 10 DIALECT 4" )
    except Exception as e :
        #print(e)
        assert str(e) == "Sortby argument count is wrong"
    assert response == ""

    try:
        response = client1.execute_command("VIEW.SEARCH " + cct_prepare.TEST_INDEX_NAME +  " @User\\.PASSPORT:{" + "aaa" + "} SORTBY INVALID_SORTBY LIMIT 0 10" )
    except Exception as e :
        #print(e)
        assert str(e) == "Sortby argument count is wrong"
    assert response == ""

    try:
        response = client1.execute_command("VIEW.SEARCH " + cct_prepare.TEST_INDEX_NAME +  " @User\\.PASSPORT:{" + "aaa" + "} SORTBY 1 INVALID_SORTBY LIMIT 0 10 DIALECT 4" )
    except Exception as e :
        #print(e)
        assert str(e) == "Inner FT.AGGREGATE call has failed"
    assert response == ""

    try:
        response = client1.execute_command("VIEW.SEARCH " + cct_prepare.TEST_INDEX_NAME +  " @User\\.PASSPORT:{" + "aaa" + "} SORTBY 1 INVALID_SORTBY LIMIT 0 10" )
    except Exception as e :
        #print(e)
        assert str(e) == "Inner FT.AGGREGATE call has failed"
    assert response == ""

def test_view_search_is_same_with_ft_aggr_with_text_sortby_field():
    producer = connect_redis_with_start()
    flush_db(producer) # clean all db first
    cct_prepare.create_index(producer)

    # ADD INITIAL DATA
    for i in range(3):
        d = cct_prepare.generate_single_object(1000 + i , 2000-i, "aaa")
        key = cct_prepare.TEST_INDEX_PREFIX + str(1 + i)
        producer.json().set(key, Path.root_path(), d)

    # FIRST CLIENT
    client1 = connect_redis()
    client1.execute_command("VIEW.REGISTER " + cct_prepare.TEST_APP_NAME_1)
    view_search_response = client1.execute_command("VIEW.SEARCH " + cct_prepare.TEST_INDEX_NAME + " @User\\.PASSPORT:{" + "aaa" + "} SORTBY 2 User.Address.ID DESC LIMIT 0 3")
    #print("VIEW:" + str(view_search_response))
    ft_aggr_response = client1.execute_command("FT.AGGREGATE " + cct_prepare.TEST_INDEX_NAME + " @User\\.PASSPORT:{" + "aaa" + "} LOAD 1 @__key SORTBY 4 @User.Address.ID DESC @__key ASC LIMIT 0 3 ")
    #print("FT:" + str(ft_aggr_response))

    assert view_search_response[5][1] == ft_aggr_response[1][1]
    assert view_search_response[5][5] == ft_aggr_response[2][1]
    assert view_search_response[5][9] == ft_aggr_response[3][1]
    



def test_view_search_is_same_with_ft_aggr_with_num_sortby_field():
    producer = connect_redis_with_start()
    flush_db(producer) # clean all db first
    cct_prepare.create_index(producer)

    # ADD INITIAL DATA
    for i in range(3):
        d = cct_prepare.generate_single_object(1000 + i , 2000-i, "aaa")
        key = cct_prepare.TEST_INDEX_PREFIX + str(1 + i)
        producer.json().set(key, Path.root_path(), d)

    # FIRST CLIENT
    client1 = connect_redis()
    client1.execute_command("VIEW.REGISTER " + cct_prepare.TEST_APP_NAME_1)
    view_search_response = client1.execute_command("VIEW.SEARCH " + cct_prepare.TEST_INDEX_NAME + " @User\\.PASSPORT:{" + "aaa" + "} SORTBY 2 User.ID DESC LIMIT 0 3")
    #print("VIEW:" + str(view_search_response))
    ft_aggr_response = client1.execute_command("FT.AGGREGATE " + cct_prepare.TEST_INDEX_NAME + " @User\\.PASSPORT:{" + "aaa" + "} LOAD 1 @__key SORTBY 4 @User.ID DESC @__key ASC LIMIT 0 3 ")
    #print("FT:" + str(ft_aggr_response))

    assert view_search_response[5][1] == ft_aggr_response[1][1]
    assert view_search_response[5][5] == ft_aggr_response[2][1]
    assert view_search_response[5][9] == ft_aggr_response[3][1]
    

def test_view_search_without_innerkey():
    producer = connect_redis_with_start()
    flush_db(producer) # clean all db first
    cct_prepare.create_index(producer)

    # ADD INITIAL DATA
    for i in range(10):
        d = cct_prepare.generate_single_object(1000 , 2000 + i, "aaa")
        key = cct_prepare.TEST_INDEX_PREFIX + str(1 + i)
        producer.json().set(key, Path.root_path(), d)

    # FIRST QUERY
    client1 = connect_redis()
    client1.execute_command("VIEW.REGISTER " + cct_prepare.TEST_APP_NAME_1)
    response = client1.execute_command("VIEW.SEARCH " + cct_prepare.TEST_INDEX_NAME +  " @User\\.PASSPORT:{" + "aaa" + "}" + " SORTBY 1 User.ID LIMIT 0 2")
    #print("\nVIEW.SEARCH (aaa) result : " + str(response))
    assert response[1] == 1
    assert response[3] == 10
    assert str(response[5]) == '''['key', 'test_data:1', 'value', ['path', '$', 'data', '{"User":{"Address":{"ID":"2000"},"ID":1000,"PASSPORT":"aaa"}}'], 'key', 'test_data:10', 'value', ['path', '$', 'data', '{"User":{"Address":{"ID":"2009"},"ID":1000,"PASSPORT":"aaa"}}']]'''

    time.sleep(1.1)

    # CHECK STREAM
    from_stream = client1.xread(streams={cct_prepare.TEST_APP_NAME_1:1} )
    #print("\nSTREAM AFTER SEARCH : " +str(from_stream))
    assert from_stream == []


    # SECOND QUERY
    client2 = connect_redis()
    client2.execute_command("VIEW.REGISTER " + cct_prepare.TEST_APP_NAME_2)
    response = client2.execute_command("VIEW.SEARCH " + cct_prepare.TEST_INDEX_NAME +  " @User\\.PASSPORT:{" + "bbb" + "}" + " SORTBY 1 User.ID LIMIT 0 2")
    #print("\nVIEW.SEARCH (bbb) result : " + str(response))
    assert response[1] == 2
    assert response[3] == 0
    assert response[5] == []


    time.sleep(1.1)

    # CHECK STREAM
    from_stream = client2.xread(streams={cct_prepare.TEST_APP_NAME_2:1} )
    #print("\nSTREAM AFTER SEARCH : " +str(from_stream))
    assert from_stream == []

