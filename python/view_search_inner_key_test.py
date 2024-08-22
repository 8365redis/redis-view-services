from manage_redis import kill_redis, connect_redis_with_start, connect_redis
from manage_redis_data import flush_db, create_index, add_json_data, TEST_INDEX_NAME
import pytest
import cct_prepare
from redis.commands.json.path import Path
from redis.commands.search.query import GeoFilter, NumericFilter, Query
from redis.commands.search.field import TextField, NumericField, TagField
from redis.commands.search.indexDefinition import IndexDefinition, IndexType
from redis.commands.search.result import Result
import time

@pytest.fixture(autouse=True)
def before_and_after_test():
    print("Start")
    yield
    kill_redis()
    print("End")

def test_no_inner_key():
    producer = connect_redis_with_start()
    flush_db(producer) # clean all db first
    cct_prepare.create_index(producer)

    # ADD INITIAL DATA
    for i in range(20):
        d = cct_prepare.generate_single_object(1000 , 2000-i, "aaa")
        key = cct_prepare.TEST_INDEX_PREFIX + str(1 + i)
        producer.json().set(key, Path.root_path(), d)
    
    # FIRST CLIENT
    client1 = connect_redis()
    client1.execute_command("VIEW.REGISTER app1")

    response = client1.execute_command("VIEW.SEARCH test_index * SORTBY 2 User.ID DESC LIMIT 0 3")
    print("\nVIEW.SEARCH RESPONSE : " + str(response))
    query_id = int(response[1])
    assert query_id == 1
    time.sleep(2)

def test_no_inner_key_with_scroll():
    producer = connect_redis_with_start()
    flush_db(producer) # clean all db first
    cct_prepare.create_index(producer)

    # ADD INITIAL DATA
    for i in range(20):
        d = cct_prepare.generate_single_object(1000 , 2000-i, "aaa")
        key = cct_prepare.TEST_INDEX_PREFIX + str(1 + i)
        producer.json().set(key, Path.root_path(), d)
    
    # FIRST CLIENT
    client1 = connect_redis()
    client1.execute_command("VIEW.REGISTER app1")

    response = client1.execute_command("VIEW.SEARCH test_index * SORTBY User.ID DESC LIMIT 0 3")
    print("\nVIEW.SEARCH RESPONSE : " + str(response))
    query_id = int(response[1])
    assert query_id == 1

    response = client1.execute_command(f"VIEW.SEARCH.SCROLL 1 3 3")
    print("\nVIEW.SEARCH.SCROLL RESPONSE : " + str(response))

    #response = client1.execute_command(f"VIEW.SEARCH.SCROLL 1 6 3")
    #print("\VIEW.SEARCH.SCROLL RESPONSE : " + str(response))
    time.sleep(2)

    response = client1.execute_command(f"VIEW.SEARCH.SCROLL 1 6 3")
    print("\nVIEW.SEARCH.SCROLL RESPONSE : " + str(response))

def test_no_inner_key_with_scroll_with_different_data_type():
    producer = connect_redis_with_start()
    flush_db(producer) # clean all db first 
    schema = (NumericField("$.aaa", as_name="aaa", sortable=True), TagField("$.bbb", as_name="bbb", sortable=True),  \
              TextField("$.ccc", as_name="ccc", sortable=True) )
    producer.ft(TEST_INDEX_NAME).create_index(schema, definition=IndexDefinition(prefix=[cct_prepare.TEST_INDEX_PREFIX], index_type=IndexType.JSON)) 

    # ADD INITIAL DATA
    for i in range(20):
        d = {}
        d["aaa"] = 100-i
        d["bbb"] = str(100+i)
        d["ccc"] = "ccc"
        key = cct_prepare.TEST_INDEX_PREFIX + str(1 + i)
        producer.json().set(key, Path.root_path(), d)

        # FIRST CLIENT
    client1 = connect_redis()
    client1.execute_command("VIEW.REGISTER app1")

    #QUERY
    response = client1.execute_command("VIEW.SEARCH test_index * SORTBY aaa DESC LIMIT 0 3")
    print("VIEW.SEARCH RESPONSE (aaa) : " + str(response))
    response = client1.execute_command(f"VIEW.SEARCH.SCROLL 1 3 3")
    print("VIEW.SEARCH.SCROLL RESPONSE (aaa): " + str(response))
    time.sleep(1)
    #QUERY
    response = client1.execute_command("VIEW.SEARCH test_index * SORTBY bbb DESC LIMIT 0 3")
    print("\nVIEW.SEARCH RESPONSE (bbb) : " + str(response))
    response = client1.execute_command(f"VIEW.SEARCH.SCROLL 2 3 3")
    print("VIEW.SEARCH.SCROLL RESPONSE (bbb): " + str(response))
    time.sleep(1)
    #QUERY
    response = client1.execute_command("VIEW.SEARCH test_index * SORTBY ccc DESC LIMIT 0 3")
    print("\nVIEW.SEARCH RESPONSE (ccc) : " + str(response))
    response = client1.execute_command(f"VIEW.SEARCH.SCROLL 3 3 3")
    print("VIEW.SEARCH.SCROLL RESPONSE (ccc): " + str(response))
    

def test_with_inner_key():
    producer = connect_redis_with_start()
    flush_db(producer) # clean all db first
    cct_prepare.create_index(producer)

    # ADD INITIAL DATA
    for i in range(20):
        key = cct_prepare.TEST_INDEX_PREFIX + str(1 + i)
        d = cct_prepare.generate_single_object(1000 , key, "aaa")
        producer.json().set(key, Path.root_path(), d)
    
    # FIRST CLIENT
    client1 = connect_redis()
    client1.execute_command("VIEW.REGISTER app1")

    response = client1.execute_command("VIEW.SEARCH test_index * SORTBY User.ID DESC LIMIT 0 3 INNERKEY User.Address.ID")
    print("\nVIEW.SEARCH RESPONSE 1: " + str(response))
    query_id = int(response[1])
    assert query_id == 1
    time.sleep(2)

    response = client1.execute_command("VIEW.SEARCH test_index * SORTBY User.ID DESC LIMIT 3 5 INNERKEY User.Address.ID")
    print("\nVIEW.SEARCH RESPONSE 2: " + str(response))

def test_wrong_inner_key():
    producer = connect_redis_with_start()
    flush_db(producer) # clean all db first
    cct_prepare.create_index(producer)

    # ADD INITIAL DATA
    for i in range(20):
        key = cct_prepare.TEST_INDEX_PREFIX + str(1 + i)
        d = cct_prepare.generate_single_object(1000 , key, "aaa")
        producer.json().set(key, Path.root_path(), d)
    
    # FIRST CLIENT
    client1 = connect_redis()
    client1.execute_command("VIEW.REGISTER app1")

    response = client1.execute_command("VIEW.SEARCH test_index * SORTBY User.ID DESC LIMIT 0 3 INNERKEY not.valid.inner.key")
    print("\nNOT VALID INNERKEY : " + str(response))
    query_id = int(response[1])
    assert query_id == 1

    time.sleep(2)

    response = client1.execute_command("VIEW.SEARCH test_index * SORTBY User.ID DESC LIMIT 0 3")
    print("\nDEFAULT                : " + str(response))
    query_id = int(response[1])
    assert query_id == 2

    time.sleep(1)

    response = client1.execute_command("VIEW.SEARCH test_index * SORTBY User.ID DESC LIMIT 5 5")
    print("\nDEFAULT                : " + str(response))

def test_sortby_and_inner_key_same():
    producer = connect_redis_with_start()
    flush_db(producer) # clean all db first
    cct_prepare.create_index(producer)

    # ADD INITIAL DATA
    for i in range(20):
        key = cct_prepare.TEST_INDEX_PREFIX + str(1 + i)
        d = cct_prepare.generate_single_object(1000+i , key, "aaa")
        producer.json().set(key, Path.root_path(), d)

    # FIRST CLIENT
    client1 = connect_redis()
    client1.execute_command("VIEW.REGISTER app1")

    response = client1.execute_command("VIEW.SEARCH test_index * SORTBY User.ID DESC LIMIT 0 3 INNERKEY User.Address.ID")
    print("\nVIEW.SEARCH RESPONSE 1: " + str(response))

    time.sleep(1.1)

    response = client1.execute_command("VIEW.SEARCH test_index * SORTBY User.Address.ID DESC LIMIT 0 3 INNERKEY User.Address.ID")
    print("\nVIEW.SEARCH RESPONSE 2: " + str(response))

    time.sleep(1.1)

    response = client1.execute_command("VIEW.SEARCH test_index * SORTBY User.Address.ID DESC LIMIT 0 3")
    print("\nVIEW.SEARCH RESPONSE 3: " + str(response))

def test_sortby_and_inner_key_same_different_order():
    producer = connect_redis_with_start()
    flush_db(producer) # clean all db first
    cct_prepare.create_index(producer)

    # ADD INITIAL DATA
    for i in range(20):
        key = cct_prepare.TEST_INDEX_PREFIX + str(1 + i)
        d = cct_prepare.generate_single_object(1000+i , 1000, key)
        producer.json().set(key, Path.root_path(), d)

    # FIRST CLIENT
    client1 = connect_redis()
    client1.execute_command("VIEW.REGISTER app1")

    response = client1.execute_command("VIEW.SEARCH test_index * SORTBY User.Address.ID DESC LIMIT 0 3 INNERKEY User.PASSPORT")
    print("\nVIEW.SEARCH RESPONSE 1: " + str(response))

    time.sleep(1.1)

    response = client1.execute_command("VIEW.SEARCH test_index * SORTBY User.ID DESC LIMIT 0 3 INNERKEY User.PASSPORT")
    print("\nVIEW.SEARCH RESPONSE 2: " + str(response))

    time.sleep(1.1)

    response = client1.execute_command("VIEW.SEARCH test_index * SORTBY User.Address.ID DESC LIMIT 0 3")
    print("\nVIEW.SEARCH RESPONSE 3: " + str(response))

def test_sortby_and_inner_key_same_different_order_with_text_1():
    producer = connect_redis_with_start()
    flush_db(producer) # clean all db first 
    schema = (TextField("$.aaa", as_name="aaa", sortable=True), TextField("$.bbb", as_name="bbb", sortable=True),  \
              TextField("$.ccc", as_name="ccc", sortable=True) )
    producer.ft(TEST_INDEX_NAME).create_index(schema, definition=IndexDefinition(prefix=[cct_prepare.TEST_INDEX_PREFIX], index_type=IndexType.JSON)) 

    # ADD INITIAL DATA
    for i in range(20):
        key = cct_prepare.TEST_INDEX_PREFIX + str(1 + i)
        d = {}
        d["aaa"] = key
        d["bbb"] = str(100+i)
        d["ccc"] = "ccc"
        producer.json().set(key, Path.root_path(), d)
    

    # FIRST CLIENT
    client1 = connect_redis()
    client1.execute_command("VIEW.REGISTER app1")

    response = client1.execute_command("VIEW.SEARCH test_index * SORTBY bbb DESC LIMIT 0 3 INNERKEY aaa")
    print("\nVIEW.SEARCH RESPONSE 1: " + str(response))

    time.sleep(1.1)

    response = client1.execute_command("VIEW.SEARCH test_index * SORTBY ccc DESC LIMIT 0 3 INNERKEY aaa")
    print("\nVIEW.SEARCH RESPONSE 2: " + str(response))

    time.sleep(1.1)

    response = client1.execute_command("VIEW.SEARCH test_index * SORTBY aaa DESC LIMIT 0 3")
    print("\nVIEW.SEARCH RESPONSE 3: " + str(response))


def test_sortby_and_inner_key_same_different_order_with_text_2():
    producer = connect_redis_with_start()
    flush_db(producer) # clean all db first 
    schema = (TextField("$.aaa", as_name="aaa", sortable=True), TextField("$.bbb", as_name="bbb", sortable=True),  \
              TextField("$.ccc", as_name="ccc", sortable=True) )
    producer.ft(TEST_INDEX_NAME).create_index(schema, definition=IndexDefinition(prefix=[cct_prepare.TEST_INDEX_PREFIX], index_type=IndexType.JSON)) 

    # ADD INITIAL DATA
    for i in range(20):
        key = cct_prepare.TEST_INDEX_PREFIX + str(1 + i)
        d = {}
        d["aaa"] = str(100-i)
        d["bbb"] = str(100+i)
        d["ccc"] = key
        producer.json().set(key, Path.root_path(), d)
    

    # FIRST CLIENT
    client1 = connect_redis()
    client1.execute_command("VIEW.REGISTER app1")

    response = client1.execute_command("VIEW.SEARCH test_index * SORTBY bbb DESC LIMIT 0 3 INNERKEY ccc")
    print("\nVIEW.SEARCH RESPONSE 1: " + str(response))

    time.sleep(1.1)

    response = client1.execute_command("VIEW.SEARCH test_index * SORTBY ccc DESC LIMIT 0 3 INNERKEY ccc")
    print("\nVIEW.SEARCH RESPONSE 2: " + str(response))

    time.sleep(1.1)

    response = client1.execute_command("VIEW.SEARCH test_index * SORTBY ccc DESC LIMIT 0 3")
    print("\nVIEW.SEARCH RESPONSE 3: " + str(response))

    time.sleep(1.1)

    response = client1.execute_command("VIEW.SEARCH test_index * SORTBY aaa DESC LIMIT 0 3")
    print("\nVIEW.SEARCH RESPONSE 3: " + str(response))

def test_complex_inner_key_1():
    producer = connect_redis_with_start()
    flush_db(producer) # clean all db first 
    schema = (TextField("$.field1", as_name="field1", sortable=True), TextField("$.field2", as_name="field2", sortable=True),  \
              TextField("$.field3", as_name="field3", sortable=True) )
    producer.ft(TEST_INDEX_NAME).create_index(schema, definition=IndexDefinition(prefix=[cct_prepare.TEST_INDEX_PREFIX], index_type=IndexType.JSON)) 

    # ADD INITIAL DATA
    for i in range(20):
        key = cct_prepare.TEST_INDEX_PREFIX + "complexKEYWIthCamelCASE|data_D|null|null|permanent|null|" + str(i)
        d = {}
        d["field1"] = str(100-i)
        d["field2"] = str(100+i)
        d["field3"] = key
        producer.json().set(key, Path.root_path(), d)
    

    # FIRST CLIENT
    client1 = connect_redis()
    client1.execute_command("VIEW.REGISTER app1")

    response = client1.execute_command("VIEW.SEARCH test_index * SORTBY field3 DESC LIMIT 0 2")
    print("\nVIEW.SEARCH SORTYBY INNERKEY: " + str(response))

    response = client1.execute_command("VIEW.SEARCH test_index * SORTBY field1 DESC LIMIT 0 2 INNERKEY field3")
    print("\nVIEW.SEARCH SORTYBY field1 INNERKEY GIVEN: " + str(response))

def test_complex_inner_key_2():
    producer = connect_redis_with_start()
    flush_db(producer) # clean all db first 
    schema = (TextField("$.field3", as_name="field3", sortable=True), TextField("$.field2", as_name="field2", sortable=True),  \
              TextField("$.field1", as_name="field1", sortable=True) )
    producer.ft(TEST_INDEX_NAME).create_index(schema, definition=IndexDefinition(prefix=[cct_prepare.TEST_INDEX_PREFIX], index_type=IndexType.JSON)) 

    # ADD INITIAL DATA
    for i in range(20):
        key = cct_prepare.TEST_INDEX_PREFIX + "complexKEYWIthCamelCASE|data_D|null|null|permanent|null|" + str(i)
        d = {}
        d["field1"] = str(100-i)
        d["field2"] = str(100+i)
        d["field3"] = key
        producer.json().set(key, Path.root_path(), d)
    

    # FIRST CLIENT
    client1 = connect_redis()
    client1.execute_command("VIEW.REGISTER app1")

    response = client1.execute_command("VIEW.SEARCH test_index * SORTBY field3 DESC LIMIT 0 2")
    print("\nVIEW.SEARCH SORTYBY INNERKEY: " + str(response))

    response = client1.execute_command("VIEW.SEARCH test_index * SORTBY field1 DESC LIMIT 0 2 INNERKEY field3")
    print("\nVIEW.SEARCH SORTYBY field1 INNERKEY GIVEN: " + str(response))

def test_complex_inner_key_3():
    producer = connect_redis()
    flush_db(producer) # clean all db first 
    schema = (TextField("$.field3", as_name="field3", sortable=True), TextField("$.field2", as_name="field2", sortable=True),  \
              TextField("$.field1", as_name="field1", sortable=True) , TextField("$.field4", as_name="field4", sortable=True)  )
    producer.ft(TEST_INDEX_NAME).create_index(schema, definition=IndexDefinition(prefix=[cct_prepare.TEST_INDEX_PREFIX], index_type=IndexType.JSON)) 

    # ADD INITIAL DATA
    for i in range(20):
        key = cct_prepare.TEST_INDEX_PREFIX + "complexKEYWIthCamelCASE|data_D|null|null|permanent|null|" + str(i)
        d = {}
        d["field1"] = key
        d["field2"] = key
        d["field3"] = key
        d["field4"] = str(10)
        producer.json().set(key, Path.root_path(), d)
    

    # FIRST CLIENT
    client1 = connect_redis()
    client1.execute_command("VIEW.REGISTER app1")

    response = client1.execute_command("VIEW.SEARCH test_index * SORTBY field3 DESC LIMIT 0 2")
    print("\nVIEW.SEARCH SORTYBY INNERKEY: " + str(response))

    response = client1.execute_command("VIEW.SEARCH test_index * SORTBY field4 DESC LIMIT 0 2 INNERKEY field3")
    print("\nVIEW.SEARCH SORTYBY field3 INNERKEY GIVEN: " + str(response))

    response = client1.execute_command("VIEW.SEARCH test_index * SORTBY field4 DESC LIMIT 0 2 INNERKEY field2")
    print("\nVIEW.SEARCH SORTYBY field2 INNERKEY GIVEN: " + str(response))

    response = client1.execute_command("VIEW.SEARCH test_index * SORTBY field4 DESC LIMIT 0 2 INNERKEY field1")
    print("\nVIEW.SEARCH SORTYBY field1 INNERKEY GIVEN: " + str(response))


def test_using_inner_key_dont_write_stream():
    producer = connect_redis_with_start()
    flush_db(producer) # clean all db first 
    schema = (TextField("$.field1", as_name="field1", sortable=True), TextField("$.field2", as_name="field2", sortable=True),  \
              TextField("$.field3", as_name="field3", sortable=True) )
    producer.ft(TEST_INDEX_NAME).create_index(schema, definition=IndexDefinition(prefix=[cct_prepare.TEST_INDEX_PREFIX], index_type=IndexType.JSON)) 

    # ADD INITIAL DATA
    for i in range(20):
        key = cct_prepare.TEST_INDEX_PREFIX + "complexKEYWIthCamelCASE|data_D|null|null|permanent|null|" + str(i)
        d = {}
        d["field1"] = str(100-i)
        d["field2"] = str(100+i)
        d["field3"] = key
        producer.json().set(key, Path.root_path(), d)
    

    # FIRST CLIENT
    client1 = connect_redis()
    client1.execute_command("VIEW.REGISTER app1")

    #CHECK STREAM BEFORE 
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    print("\nBEFORE : " +  str(from_stream))

    response = client1.execute_command("VIEW.SEARCH test_index * SORTBY field1 DESC LIMIT 0 2 INNERKEY field3")
    print("\nVIEW.SEARCH SORTYBY INNERKEY: " + str(response))

    time.sleep(1.1)

    #CHECK STREAM AFTER 
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    print("\nAFTER : " +  str(from_stream))

def test_using_inner_key_with_scroll():
    producer = connect_redis_with_start()
    flush_db(producer) # clean all db first 
    schema = (TextField("$.field1", as_name="field1", sortable=True), TextField("$.field2", as_name="field2", sortable=True),  \
              TextField("$.field3", as_name="field3", sortable=True) )
    producer.ft(TEST_INDEX_NAME).create_index(schema, definition=IndexDefinition(prefix=[cct_prepare.TEST_INDEX_PREFIX], index_type=IndexType.JSON)) 

    # ADD INITIAL DATA
    for i in range(20):
        key = cct_prepare.TEST_INDEX_PREFIX + "complexKEYWIthCamelCASE|data_D|null|null|permanent|null|" + str(i)
        d = {}
        d["field1"] = str(100-i)
        d["field2"] = str(100+i)
        d["field3"] = key
        producer.json().set(key, Path.root_path(), d)
    

    # FIRST CLIENT
    client1 = connect_redis()
    client1.execute_command("VIEW.REGISTER app1")

    response = client1.execute_command("VIEW.SEARCH test_index * SORTBY field1 DESC LIMIT 0 1 INNERKEY field3")
    print("\nVIEW.SEARCH SORTYBY INNERKEY: " + str(response))

    # CHECK STREAM
    time.sleep(1.1)
    from_stream = client1.xread(streams={cct_prepare.TEST_APP_NAME_1:1} )
    print("\nSTREAM AFTER SEARCH : " +str(from_stream))

    response = client1.execute_command("VIEW.SEARCH.SCROLL " + str(1) + " 1 1")
    print("\nVIEW.SEARCH.SCROLL RESPONSE : " +str(response))

    # CHECK STREAM
    time.sleep(1.1)
    from_stream = client1.xread(streams={cct_prepare.TEST_APP_NAME_1:1} )
    print("\nSTREAM AFTER SCROLL : " +str(from_stream))


def test_result_order_with_inner_key_1():
    producer = connect_redis_with_start()
    flush_db(producer) # clean all db first 
    schema = (TextField("$.aaa", as_name="aaa", sortable=True), TextField("$.bbb", as_name="bbb", sortable=True),  \
              TextField("$.ccc", as_name="ccc", sortable=True) , TextField("$.ddd", as_name="ddd", sortable=True)  )
    producer.ft(TEST_INDEX_NAME).create_index(schema, definition=IndexDefinition(prefix=[cct_prepare.TEST_INDEX_PREFIX], index_type=IndexType.JSON)) 

    # ADD INITIAL DATA
    for i in range(20):
        key = cct_prepare.TEST_INDEX_PREFIX + str(i)
        d = {}
        d["aaa"] = str(i)
        d["bbb"] = key
        d["ccc"] = key
        d["ddd"] = str(10)
        producer.json().set(key, Path.root_path(), d)
    

    # FIRST CLIENT
    client1 = connect_redis()
    client1.execute_command("VIEW.REGISTER app1")

    response = client1.execute_command("VIEW.SEARCH test_index * SORTBY 2 aaa DESC LIMIT 0 3 INNERKEY ccc")
    print("\nVIEW.SEARCH SORTYBY field3 INNERKEY GIVEN: " + str(response))

    first_key = response[5][1]
    assert first_key == 'test_data:9'
    second_key = response[5][5]
    assert second_key == 'test_data:8'
    third_key = response[5][9]
    assert third_key == 'test_data:7'


def test_result_order_with_inner_key_2():
    producer = connect_redis_with_start()
    flush_db(producer) # clean all db first 
    schema = (TextField("$.aaa", as_name="aaa", sortable=True), TextField("$.bbb", as_name="bbb", sortable=True),  \
              TextField("$.ccc", as_name="ccc", sortable=True) , TextField("$.ddd", as_name="ddd", sortable=True)  )
    producer.ft(TEST_INDEX_NAME).create_index(schema, definition=IndexDefinition(prefix=[cct_prepare.TEST_INDEX_PREFIX], index_type=IndexType.JSON)) 

    # ADD INITIAL DATA
    for i in range(20):
        key = cct_prepare.TEST_INDEX_PREFIX + str(i)
        d = {}
        d["aaa"] = str(i)
        d["bbb"] = key
        d["ccc"] = key
        d["ddd"] = str(10)
        producer.json().set(key, Path.root_path(), d)
    

    # FIRST CLIENT
    client1 = connect_redis()
    client1.execute_command("VIEW.REGISTER app1")

    response = client1.execute_command("VIEW.SEARCH test_index * SORTBY 2 ddd DESC LIMIT 0 3 INNERKEY ccc")
    print("\nVIEW.SEARCH SORTYBY field3 INNERKEY GIVEN: " + str(response))

    first_key = response[5][1]
    assert first_key == 'test_data:0'
    second_key = response[5][5]
    assert second_key == 'test_data:1'
    third_key = response[5][9]
    assert third_key == 'test_data:10'


def test_result_order_with_inner_key_3():
    producer = connect_redis_with_start()
    flush_db(producer) # clean all db first 
    schema = (TextField("$.aaa", as_name="aaa", sortable=True), TextField("$.bbb", as_name="bbb", sortable=True),  \
              TextField("$.ccc", as_name="ccc", sortable=True) , TextField("$.ddd", as_name="ddd", sortable=True)  )
    producer.ft(TEST_INDEX_NAME).create_index(schema, definition=IndexDefinition(prefix=[cct_prepare.TEST_INDEX_PREFIX], index_type=IndexType.JSON)) 

    # ADD INITIAL DATA
    for i in range(20):
        key = cct_prepare.TEST_INDEX_PREFIX + str(i)
        d = {}
        d["aaa"] = str(i)
        d["bbb"] = key
        d["ccc"] = key
        d["ddd"] = str(10)
        producer.json().set(key, Path.root_path(), d)
    

    # FIRST CLIENT
    client1 = connect_redis()
    client1.execute_command("VIEW.REGISTER app1")

    response = client1.execute_command("VIEW.SEARCH test_index * SORTBY 6 aaa ASC bbb DESC ccc DESC LIMIT 0 3 INNERKEY ccc")
    print("\nVIEW.SEARCH SORTYBY field3 INNERKEY GIVEN: " + str(response))

    first_key = response[5][1]
    assert first_key == 'test_data:0'
    second_key = response[5][5]
    assert second_key == 'test_data:1'
    third_key = response[5][9]
    assert third_key == 'test_data:10'

def test_result_order_with_inner_key_4():
    producer = connect_redis_with_start()
    flush_db(producer) # clean all db first 
    schema = (TextField("$.aaa", as_name="aaa", sortable=True), TextField("$.bbb", as_name="bbb", sortable=True),  \
              TextField("$.ccc", as_name="ccc", sortable=True) , TextField("$.ddd", as_name="ddd", sortable=True)  )
    producer.ft(TEST_INDEX_NAME).create_index(schema, definition=IndexDefinition(prefix=[cct_prepare.TEST_INDEX_PREFIX], index_type=IndexType.JSON)) 

    # ADD INITIAL DATA
    for i in range(20):
        key = cct_prepare.TEST_INDEX_PREFIX + str(i)
        d = {}
        d["aaa"] = str(i)
        d["bbb"] = key
        d["ccc"] = key
        d["ddd"] = str(10)
        producer.json().set(key, Path.root_path(), d)
    

    # FIRST CLIENT
    client1 = connect_redis()
    client1.execute_command("VIEW.REGISTER app1")

    response = client1.execute_command("VIEW.SEARCH test_index * SORTBY 3 aaa bbb ccc LIMIT 0 3 INNERKEY ccc")
    print("\nVIEW.SEARCH SORTYBY field3 INNERKEY GIVEN: " + str(response))

    first_key = response[5][1]
    assert first_key == 'test_data:0'
    second_key = response[5][5]
    assert second_key == 'test_data:1'
    third_key = response[5][9]
    assert third_key == 'test_data:10'


def test_innerkey_prefix():
    producer = connect_redis_with_start()
    flush_db(producer) # clean all db first 
    schema = (TextField("$.aaa", as_name="aaa", sortable=True), TextField("$.bbb", as_name="bbb", sortable=True),  \
              TextField("$.ccc", as_name="ccc", sortable=True) , TextField("$.ddd", as_name="ddd", sortable=True)  )
    producer.ft(TEST_INDEX_NAME).create_index(schema, definition=IndexDefinition(prefix=[cct_prepare.TEST_INDEX_PREFIX], index_type=IndexType.JSON)) 

    # ADD INITIAL DATA
    for i in range(20):
        key = i
        d = {}
        d["aaa"] = str(i)
        d["bbb"] = str(key)
        d["ccc"] = str(key)
        d["ddd"] = str(10)
        producer.json().set(cct_prepare.TEST_INDEX_PREFIX + str(key), Path.root_path(), d)
    

    # FIRST CLIENT
    client1 = connect_redis()
    client1.execute_command("VIEW.REGISTER app1")

    response = client1.execute_command("VIEW.SEARCH test_index * SORTBY 2 aaa DESC LIMIT 0 3 INNERKEY ccc " + cct_prepare.TEST_INDEX_PREFIX)
    print("\nVIEW.SEARCH SORTYBY field3 INNERKEY GIVEN: " + str(response))

    first_key = response[5][1]
    assert first_key == 'test_data:9'
    second_key = response[5][5]
    assert second_key == 'test_data:8'
    third_key = response[5][9]
    assert third_key == 'test_data:7'

def test_innerkey_prefix_scroll():
    producer = connect_redis_with_start()
    flush_db(producer) # clean all db first 
    schema = (TextField("$.aaa", as_name="aaa", sortable=True), TextField("$.bbb", as_name="bbb", sortable=True),  \
              TextField("$.ccc", as_name="ccc", sortable=True) , TextField("$.ddd", as_name="ddd", sortable=True)  )
    producer.ft(TEST_INDEX_NAME).create_index(schema, definition=IndexDefinition(prefix=[cct_prepare.TEST_INDEX_PREFIX], index_type=IndexType.JSON)) 

    # ADD INITIAL DATA
    for i in range(20):
        key = i
        d = {}
        d["aaa"] = str(i)
        d["bbb"] = str(key)
        d["ccc"] = str(key)
        d["ddd"] = str(10)
        producer.json().set(cct_prepare.TEST_INDEX_PREFIX + str(key), Path.root_path(), d)
    

    # FIRST CLIENT
    client1 = connect_redis()
    client1.execute_command("VIEW.REGISTER app1")

    response = client1.execute_command("VIEW.SEARCH test_index * SORTBY 2 aaa DESC LIMIT 0 2 INNERKEY ccc " + cct_prepare.TEST_INDEX_PREFIX)
    print("\nVIEW.SEARCH SORTYBY field3 INNERKEY GIVEN: " + str(response))

    first_key = response[5][1]
    assert first_key == 'test_data:9'
    second_key = response[5][5]
    assert second_key == 'test_data:8'

    response = client1.execute_command("VIEW.SEARCH.SCROLL " + str(1) + " 2 2")
    print("\nVIEW.SEARCH.SCROLL : " + str(response))

    first_key = response[5][1]
    assert first_key == 'test_data:7'
    second_key = response[5][5]
    assert second_key == 'test_data:6'


def test_innerkey_prefix_stream_write_test():
    producer = connect_redis_with_start()
    flush_db(producer) # clean all db first 
    schema = (TextField("$.aaa", as_name="aaa", sortable=True), TextField("$.bbb", as_name="bbb", sortable=True),  \
              TextField("$.ccc", as_name="ccc", sortable=True) , TextField("$.ddd", as_name="ddd", sortable=True)  )
    producer.ft(TEST_INDEX_NAME).create_index(schema, definition=IndexDefinition(prefix=[cct_prepare.TEST_INDEX_PREFIX], index_type=IndexType.JSON)) 

    # ADD INITIAL DATA
    for i in range(20):
        key = i
        d = {}
        d["aaa"] = str(i)
        d["bbb"] = str(key)
        d["ccc"] = str(key)
        d["ddd"] = str(10)
        producer.json().set(cct_prepare.TEST_INDEX_PREFIX + str(key), Path.root_path(), d)
    

    # FIRST CLIENT
    client1 = connect_redis()
    client1.execute_command("VIEW.REGISTER app1")

    response = client1.execute_command("VIEW.SEARCH test_index * SORTBY 2 aaa DESC LIMIT 0 2 INNERKEY ccc " + cct_prepare.TEST_INDEX_PREFIX)
    print("\nVIEW.SEARCH SORTYBY field3 INNERKEY GIVEN: " + str(response))

    time.sleep(1.1)

    # CHECK STREAM
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    print("BEFORE UPDATE: " + str(from_stream))

    #UPDATE DATA
    key = 9
    d = {}
    d["aaa"] = str(21)
    d["bbb"] = str(key)
    d["ccc"] = str(key)
    d["ddd"] = str(10)
    producer.json().set(cct_prepare.TEST_INDEX_PREFIX + str(key), Path.root_path(), d)

    time.sleep(1.1)

    # CHECK STREAM
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    print("AFTER UPDATE: " + str(from_stream))

