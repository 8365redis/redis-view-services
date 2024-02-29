from manage_redis import kill_redis, connect_redis_with_start, connect_redis, connect_redis_with_start_without_module
from manage_redis_data import flush_db, create_index, add_json_data, TEST_INDEX_NAME
from redis.commands.search.field import TextField, NumericField, TagField
from redis.commands.search.indexDefinition import IndexDefinition, IndexType
import pytest
from redis.commands.json.path import Path
from redis.commands.search.query import GeoFilter, NumericFilter, Query
from redis.commands.search.result import Result
import time
import random
import cct_prepare
from statistics import mean

TEST_INDEX_TAG = "test_index_tag"
TEST_INDEX_TAG_PREFIX = "test_tag_data:"
TEST_INDEX_TEXT = "test_index_text"
TEST_INDEX_TEXT_PREFIX = "test_text_data:"
TEST_INDEX_NUM = "test_index_num"
TEST_INDEX_NUM_PREFIX = "test_num_data:"

pytest.skip("Skip during test , remove this during manual run", allow_module_level=True)

def create_index_as_tag(r):
    schema = (TagField("$.User.ID", as_name="User.ID", sortable=True), TagField("$.User.PASSPORT", as_name="User.PASSPORT"),  \
              TagField("$.User.Address.ID", as_name="User.Address.ID", sortable=True) )
    r.ft(TEST_INDEX_TAG).create_index(schema, definition=IndexDefinition(prefix=[TEST_INDEX_TAG_PREFIX], index_type=IndexType.JSON))


def create_index_as_text(r):
    schema = (TextField("$.User.ID", as_name="User.ID", sortable=True), TagField("$.User.PASSPORT", as_name="User.PASSPORT"),  \
              TagField("$.User.Address.ID", as_name="User.Address.ID", sortable=True) )
    r.ft(TEST_INDEX_TEXT).create_index(schema, definition=IndexDefinition(prefix=[TEST_INDEX_TEXT_PREFIX], index_type=IndexType.JSON))


def create_index_as_numeric(r):
    schema = (NumericField("$.User.ID", as_name="User.ID", sortable=True), TagField("$.User.PASSPORT", as_name="User.PASSPORT"),  \
              TagField("$.User.Address.ID", as_name="User.Address.ID", sortable=True) )
    r.ft(TEST_INDEX_NUM).create_index(schema, definition=IndexDefinition(prefix=[TEST_INDEX_NUM_PREFIX], index_type=IndexType.JSON))



def generate_single_object_as_text(id, addr_id, passport):
    d = {}
    d["User"] = {}
    d["User"]["ID"] = str(id)
    d["User"]["PASSPORT"] = passport
    d["User"]["Address"] = {}
    d["User"]["Address"]["ID"] = str(addr_id)
    return d

def generate_single_object_as_num(id, addr_id, passport):
    d = {}
    d["User"] = {}
    d["User"]["ID"] = id
    d["User"]["PASSPORT"] = passport
    d["User"]["Address"] = {}
    d["User"]["Address"]["ID"] = str(addr_id)
    return d

@pytest.fixture(autouse=True)
def before_and_after_test():
    print("Start")
    yield
    kill_redis()
    print("End")

def test_different_index_dialect_perf():
    producer = connect_redis_with_start()
    #flush_db(producer) # clean all db first
    
    time.sleep(30)

    response = producer.execute_command("ft.config set default_dialect 4")
    response = producer.execute_command("ft.config get default_dialect")
    print("Current DIALECT : " + str(response))

    TOTAL_KEY_COUNT = 500000
    OFFSET = random.randint(0 , TOTAL_KEY_COUNT)
    NUM =  random.randint(50 , 100)

    response = producer.execute_command("FT.CONFIG SET MAXSEARCHRESULTS " + str(TOTAL_KEY_COUNT))
    #response = producer.execute_command("FT.CONFIG SET TIMEOUT  " + str(5000) )

    print("Current OFFSET : " + str(OFFSET))
    print("Current NUM : " + str(NUM))

    
    '''
    #####################################################
    # FIRST TAG
    prod1 = connect_redis()
    create_index_as_tag(prod1)
    # ADD INITIAL DATA
    for i in range(TOTAL_KEY_COUNT):
        d = generate_single_object_as_text(random.randint(0 , TOTAL_KEY_COUNT) , random.randint(0 , TOTAL_KEY_COUNT), "aaa")
        key = TEST_INDEX_TAG_PREFIX + str(i)
        prod1.json().set(key, Path.root_path(), d)

    #####################################################
    # SECOND TEXT
    prod2 = connect_redis()
    create_index_as_text(prod2)
    # ADD INITIAL DATA
    for i in range(TOTAL_KEY_COUNT):
        d = generate_single_object_as_text(random.randint(0 , TOTAL_KEY_COUNT) , random.randint(0 , TOTAL_KEY_COUNT), "aaa")
        key = TEST_INDEX_TEXT_PREFIX + str(i)
        prod2.json().set(key, Path.root_path(), d)
    
    #####################################################
    # THIRD NUM
    prod3 = connect_redis()
    create_index_as_numeric(prod3)
    # ADD INITIAL DATA
    for i in range(TOTAL_KEY_COUNT):
        d = generate_single_object_as_num(random.randint(0 , TOTAL_KEY_COUNT) , random.randint(0 , TOTAL_KEY_COUNT), "aaa")
        key = TEST_INDEX_NUM_PREFIX + str(i)
        prod3.json().set(key, Path.root_path(), d)
    '''
    # FIRST CLIENT
    client1 = connect_redis()

    tag_search_dialect_1_times = []
    text_search_dialect_1_times = []
    num_search_dialect_1_times = []

    tag_search_dialect_4_times = []
    text_search_dialect_4_times = []
    num_search_dialect_4_times = []

    for i in range(100):
        
        # TAG SEARCH
        start_set_time = time.time_ns()
        response = client1.execute_command("FT.SEARCH " + TEST_INDEX_TAG + " @User\\.PASSPORT:{" + "aaa" + "} SORTBY User.ID LIMIT " + str(OFFSET) + " " + str(NUM) + " DIALECT 1")
        #print('\nTAG DIALECT 1 :' + str(response))
        end_set_time = time.time_ns()
        naive_latency = (end_set_time - start_set_time) // 1000
        tag_search_dialect_1_times.append(naive_latency)
        # TEXT SEARCH
        start_set_time = time.time_ns()
        response = client1.execute_command("FT.SEARCH " + TEST_INDEX_TEXT + " @User\\.PASSPORT:{" + "aaa" + "} SORTBY User.ID LIMIT " + str(OFFSET) + " " + str(NUM) + " DIALECT 1")
        #print('\nTEXT DIALECT 1 :' + str(response))
        end_set_time = time.time_ns()
        naive_latency = (end_set_time - start_set_time) // 1000
        text_search_dialect_1_times.append(naive_latency)
        # NUM SEARCH
        start_set_time = time.time_ns()
        response = client1.execute_command("FT.SEARCH " + TEST_INDEX_NUM + " @User\\.PASSPORT:{" + "aaa" + "} SORTBY User.ID LIMIT " + str(OFFSET) + " " + str(NUM) + " DIALECT 1")
        #print('\nNUM DIALECT 1 :' + str(response))
        end_set_time = time.time_ns()
        naive_latency = (end_set_time - start_set_time) // 1000
        num_search_dialect_1_times.append(naive_latency)


        # TAG SEARCH
        start_set_time = time.time_ns()
        response = client1.execute_command("FT.SEARCH " + TEST_INDEX_TAG + " @User\\.PASSPORT:{" + "aaa" + "} SORTBY User.ID LIMIT " + str(OFFSET) + " " + str(NUM) + " DIALECT 4")
        #print('\nTAG DIALECT 4 :' + str(response))
        end_set_time = time.time_ns()
        naive_latency = (end_set_time - start_set_time) // 1000
        tag_search_dialect_4_times.append(naive_latency)
        # TEXT SEARCH
        start_set_time = time.time_ns()
        response = client1.execute_command("FT.SEARCH " + TEST_INDEX_TEXT + " @User\\.PASSPORT:{" + "aaa" + "} SORTBY User.ID LIMIT " + str(OFFSET) + " " + str(NUM) + " DIALECT 4")
        #print('\nTEXT DIALECT 4 :' + str(response))
        end_set_time = time.time_ns()
        naive_latency = (end_set_time - start_set_time) // 1000
        text_search_dialect_4_times.append(naive_latency)
        # NUM SEARCH
        start_set_time = time.time_ns()
        response = client1.execute_command("FT.SEARCH " + TEST_INDEX_NUM + " @User\\.PASSPORT:{" + "aaa" + "} SORTBY User.ID LIMIT " + str(OFFSET) + " " + str(NUM)+ " DIALECT 4" )
        #print('\nNUM DIALECT 4 :' + str(response))
        end_set_time = time.time_ns()
        naive_latency = (end_set_time - start_set_time) // 1000
        num_search_dialect_4_times.append(naive_latency)

    print("\nTAG SEARCH DIALECT 1: " + str(mean(tag_search_dialect_1_times)))
    print("TEXT SEARCH DIALECT 1: " + str(mean(text_search_dialect_1_times)))
    print("NUM SEARCH DIALECT 1: " + str(mean(num_search_dialect_1_times)))
    print("TAG SEARCH DIALECT 4: " + str(mean(tag_search_dialect_4_times)))
    print("TEXT SEARCH DIALECT 4: " + str(mean(text_search_dialect_4_times)))
    print("NUM SEARCH DIALECT 4: " + str(mean(num_search_dialect_4_times)))