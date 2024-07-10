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
    response = client1.execute_command("VIEW.SEARCH.COUNT " + cct_prepare.TEST_INDEX_NAME + " @User\\.PASSPORT:{" + "aaa" + "}")
    assert response[1] == 1
    assert response[3] == 'test_index @User\\.PASSPORT:{aaa}'
    assert response[5] == 1

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
    print("After Second Data Stream : " + str(from_stream))
    assert len(from_stream[0][1]) == 1
    assert from_stream[0][1][0][1]['count'] == '2'
    assert from_stream[0][1][0][1]['count_query_id'] == '1'

    time.sleep(1.1)

    # CHECK STREAM
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    #print("After Second Data Stream No Change: " + str(from_stream))
    assert len(from_stream[0][1]) == 1
    assert from_stream[0][1][0][1]['count'] == '2'
    assert from_stream[0][1][0][1]['count_query_id'] == '1'



def test_view_count_return_none():
    producer = connect_redis_with_start()
    flush_db(producer) # clean all db first
    cct_prepare.create_index(producer)

    # FIRST CLIENT
    client1 = connect_redis()
    client1.execute_command("VIEW.REGISTER " + cct_prepare.TEST_APP_NAME_1)
    response = client1.execute_command("VIEW.SEARCH.COUNT " + cct_prepare.TEST_INDEX_NAME + " @User\\.PASSPORT:{" + "aaa" + "}")
    assert response[1] == 1
    assert response[3] == 'test_index @User\\.PASSPORT:{aaa}'
    assert response[5] == 0

def test_view_count_return_multi_exceed():
    producer = connect_redis_with_start()
    flush_db(producer) # clean all db first
    cct_prepare.create_index(producer)

    # ADD INITIAL DATA
    for i in range(1, 101):
        d = cct_prepare.generate_single_object(1000 + i, 2000 + i, "aaa")
        key = cct_prepare.TEST_INDEX_PREFIX + str(i)
        producer.json().set(key, Path.root_path(), d)

    # FIRST CLIENT
    client1 = connect_redis()
    client1.execute_command("VIEW.REGISTER " + cct_prepare.TEST_APP_NAME_1)
    response = client1.execute_command("VIEW.SEARCH.COUNT " + cct_prepare.TEST_INDEX_NAME + " @User\\.PASSPORT:{" + "aaa" + "}")
    assert response[1] == 1
    assert response[3] == 'test_index @User\\.PASSPORT:{aaa}'
    assert response[5] == 100

def test_view_count_doesnt_write_stream():
    producer = connect_redis_with_start()
    flush_db(producer) # clean all db first
    cct_prepare.create_index(producer)

    # ADD INITIAL DATA
    for i in range(1, 101):
        d = cct_prepare.generate_single_object(1000 + i, 2000 + i, "aaa")
        key = cct_prepare.TEST_INDEX_PREFIX + str(i)
        producer.json().set(key, Path.root_path(), d)

    # FIRST CLIENT
    client1 = connect_redis()
    client1.execute_command("VIEW.REGISTER " + cct_prepare.TEST_APP_NAME_1)
    response = client1.execute_command("VIEW.SEARCH.COUNT " + cct_prepare.TEST_INDEX_NAME + " @User\\.PASSPORT:{" + "aaa" + "}")
    assert response[1] == 1
    assert response[3] == 'test_index @User\\.PASSPORT:{aaa}'
    assert response[5] == 100

    # CHECK STREAM
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    assert from_stream == []

    time.sleep(1.1)

    # CHECK STREAM
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    assert from_stream == []


def test_view_count_write_stream_only_once_after_delete_1():
    producer = connect_redis_with_start()
    flush_db(producer) # clean all db first
    cct_prepare.create_index(producer)
    total = 100
    # ADD INITIAL DATA
    for i in range(1, total + 1):
        d = cct_prepare.generate_single_object(1000 + i, 2000 + i, "aaa")
        key = cct_prepare.TEST_INDEX_PREFIX + str(i)
        producer.json().set(key, Path.root_path(), d)

    # FIRST CLIENT
    client1 = connect_redis()
    client1.execute_command("VIEW.REGISTER " + cct_prepare.TEST_APP_NAME_1)
    response = client1.execute_command("VIEW.SEARCH.COUNT " + cct_prepare.TEST_INDEX_NAME + " @User\\.PASSPORT:{" + "aaa" + "}")
    assert response[1] == 1
    assert response[3] == 'test_index @User\\.PASSPORT:{aaa}'
    assert response[5] == total

    # CHECK STREAM
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    assert from_stream == []

    time.sleep(1.1)

    # DELETE ONE KEY
    key = cct_prepare.TEST_INDEX_PREFIX + str(1)
    producer.json().delete(key, Path.root_path())

    time.sleep(1.1)

    # CHECK STREAM
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    assert 1 == len(from_stream[0][1])
    assert "99" == from_stream[0][1][0][1]["count"]
    assert from_stream[0][1][0][1]['count_query_id'] == '1'

    time.sleep(1.1)

    # CHECK STREAM
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    assert 1 == len(from_stream[0][1])
    assert "99" == from_stream[0][1][0][1]["count"]
    assert from_stream[0][1][0][1]['count_query_id'] == '1'

    time.sleep(1.1)

    # CHECK STREAM
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    assert 1 == len(from_stream[0][1])
    assert "99" == from_stream[0][1][0][1]["count"]
    assert from_stream[0][1][0][1]['count_query_id'] == '1'


def test_view_count_write_stream_only_once_after_delete_2():
    producer = connect_redis_with_start()
    flush_db(producer) # clean all db first
    cct_prepare.create_index(producer)
    total = 100
    # ADD INITIAL DATA
    for i in range(1, total + 1):
        d = cct_prepare.generate_single_object(1000 + i, 2000 + i, "aaa")
        key = cct_prepare.TEST_INDEX_PREFIX + str(i)
        producer.json().set(key, Path.root_path(), d)

    # FIRST CLIENT
    client1 = connect_redis()
    client1.execute_command("VIEW.REGISTER " + cct_prepare.TEST_APP_NAME_1)
    response = client1.execute_command("VIEW.SEARCH.COUNT " + cct_prepare.TEST_INDEX_NAME + " * ")
    assert response[1] == 1
    assert response[3] == 'test_index *'
    assert response[5] == total

    # CHECK STREAM
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    assert from_stream == []

    time.sleep(1.1)

    # DELETE ONE KEY
    key = cct_prepare.TEST_INDEX_PREFIX + str(1)
    producer.json().delete(key, Path.root_path())

    time.sleep(1.1)

    # CHECK STREAM
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    assert 1 == len(from_stream[0][1])
    #print(str(from_stream[0][1][0][1]["count"]))
    assert "99" == from_stream[0][1][0][1]["count"]

    # DELETE ONE KEY
    key = cct_prepare.TEST_INDEX_PREFIX + str(2)
    producer.json().delete(key, Path.root_path())

    time.sleep(1.1)

    # CHECK STREAM
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    assert 2 == len(from_stream[0][1])
    assert "98" == from_stream[0][1][1][1]["count"]

    time.sleep(1.1)

    # CHECK STREAM
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    assert 2 == len(from_stream[0][1])
    assert "98" == from_stream[0][1][1][1]["count"]

def test_view_count_write_stream_only_once_after_delete_3():
    producer = connect_redis_with_start()
    flush_db(producer) # clean all db first
    cct_prepare.create_index(producer)
    total = 100
    # ADD INITIAL DATA
    for i in range(1, total + 1):
        d = cct_prepare.generate_single_object(1000 + i, 2000 + i, "aaa")
        key = cct_prepare.TEST_INDEX_PREFIX + str(i)
        producer.json().set(key, Path.root_path(), d)

    # FIRST CLIENT
    client1 = connect_redis()
    client1.execute_command("VIEW.REGISTER " + cct_prepare.TEST_APP_NAME_1)
    response = client1.execute_command("VIEW.SEARCH.COUNT " + cct_prepare.TEST_INDEX_NAME + " * ")
    assert response[1] == 1
    assert response[3] == 'test_index *'
    assert response[5] == total

    # CHECK STREAM
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    assert from_stream == []

    time.sleep(1.1)

    # DELETE ONE KEY
    key = cct_prepare.TEST_INDEX_PREFIX + str(1)
    producer.delete(key)

    time.sleep(1.1)

    # CHECK STREAM
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    assert 1 == len(from_stream[0][1])
    #print(str(from_stream[0][1][0][1]["count"]))
    assert str(total-1) == from_stream[0][1][0][1]["count"]

    time.sleep(1.1)

    # CHECK STREAM
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    assert 1 == len(from_stream[0][1])

    time.sleep(1.1)

    # CHECK STREAM
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    assert 1 == len(from_stream[0][1])

    time.sleep(1.1)

    # CHECK STREAM
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    assert 1 == len(from_stream[0][1])

def test_view_count_write_stream_only_once_after_delete_4():
    producer = connect_redis_with_start()
    flush_db(producer) # clean all db first
    cct_prepare.create_index(producer)
    total = 100
    # ADD INITIAL DATA
    for i in range(1, total + 1):
        d = cct_prepare.generate_single_object(1000 + i, 2000 + i, "aaa")
        key = cct_prepare.TEST_INDEX_PREFIX + str(i)
        producer.json().set(key, Path.root_path(), d)

    # FIRST CLIENT
    client1 = connect_redis()
    client1.execute_command("VIEW.REGISTER " + cct_prepare.TEST_APP_NAME_1)
    response = client1.execute_command("VIEW.SEARCH.COUNT " + cct_prepare.TEST_INDEX_NAME + " * ")
    assert response[1] == 1
    assert response[3] == 'test_index *'
    assert response[5] == total

    # CHECK STREAM
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    assert from_stream == []

    time.sleep(1.1)

    # DELETE ONE KEY
    deleted = 10
    for i in range(1, deleted + 1):
        key = cct_prepare.TEST_INDEX_PREFIX + str(i) 
        producer.delete(key)

    time.sleep(1.1)

    # CHECK STREAM
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    assert 1 == len(from_stream[0][1])
    assert str(total-deleted) == from_stream[0][1][0][1]["count"]

    time.sleep(1.1)

    # CHECK STREAM
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    assert 1 == len(from_stream[0][1])

    time.sleep(1.1)

    # CHECK STREAM
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    assert 1 == len(from_stream[0][1])

    time.sleep(1.1)

    # CHECK STREAM
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    assert 1 == len(from_stream[0][1])