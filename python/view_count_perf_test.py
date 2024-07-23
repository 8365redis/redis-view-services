from manage_redis import kill_redis, connect_redis_with_start, connect_redis
from manage_redis_data import flush_db, create_index, add_json_data, TEST_INDEX_NAME
import time
import threading
import random
import uuid
import time
from redis.commands.json.path import Path
from redis.commands.search.field import NumericField, TagField
from redis.commands.search.indexDefinition import IndexDefinition, IndexType
from statistics import mean
import re
import pytest
import cct_prepare
from redis.commands.search.field import TextField, NumericField, TagField
from redis.commands.search.indexDefinition import IndexDefinition, IndexType
import string

def get_random_string(length):
    letters = string.ascii_lowercase
    result_str = ''.join(random.choice(letters) for i in range(length))
    return result_str

def add_data_only():
    producer = connect_redis_with_start()
    time.sleep(2)
    flush_db(producer)
    time.sleep(2) 

    schema = (TextField("$.field1", as_name="field1", sortable=True), TextField("$.field2", as_name="field2", sortable=True),  \
              TextField("$.field3", as_name="field3", sortable=True) , TextField("$.field4", as_name="field4", sortable=True)  )
    producer.ft(TEST_INDEX_NAME).create_index(schema, definition=IndexDefinition(prefix=[cct_prepare.TEST_INDEX_PREFIX], index_type=IndexType.JSON)) 

    total_count = 100000
    iter_count = 1
    for i in range(total_count):
        key = cct_prepare.TEST_INDEX_PREFIX + "text_key:" + str(i)
        d = {}
        d["field1"] = str(i) + "-text"
        d["field2"] = get_random_string(10)
        d["field3"] = get_random_string(10)
        d["field4"] = key
        producer.json().set(key, Path.root_path(), d)
    

def main():
    print("Perf Test starts")
    #ft_info_test()
    #sortby_text_innerkey_text_test()
    #sortby_text_innerkey_text_with_offset_test()
    #sortby_numeric_innerkey_numeric_test()
    #sortby_numeric_innerkey_numeric_with_offset_test()
    #sortby_numeric_innerkey_text_test()
    #sortby_numeric_innerkey_text_with_offset_test()
    #sortby_text_innerkey_numeric_test()
    #sortby_text_innerkey_numeric_with_offset_test()
    #ft_aggreagate_with_sortby_and_innerkey_variation_test()
    add_data_only()
    print("Perf Test ends")

def ft_info_test():
    producer = connect_redis_with_start()
    time.sleep(1)
    flush_db(producer)
    time.sleep(1)

    cct_prepare.create_index(producer)
    total_count = 10
    iter_count = 1

    # ADD INITIAL DATA
    for i in range(total_count):
        d = cct_prepare.generate_single_object(10000000-i , 20000000+i, "aaa")
        key = cct_prepare.TEST_INDEX_PREFIX + str(i+300)
        producer.json().set(key, Path.root_path(), d)
    
    latency = []
    #for i in range(iter_count):
    #    start_set_time = time.perf_counter_ns()
    #    res = producer.execute_command("FT.INFO " + cct_prepare.TEST_INDEX_NAME)
    #    end_set_time = time.perf_counter_ns()
    #    latency.append(((end_set_time - start_set_time) // 1000))

    res = producer.execute_command("FT.INFO " + cct_prepare.TEST_INDEX_NAME)
    print(str(res))

    # ADD INITIAL DATA
    for i in range(total_count):
        d = cct_prepare.generate_single_object(10000000-i , 20000000+i, "aaa")
        key = cct_prepare.TEST_INDEX_PREFIX + str(i+100)
        producer.json().set(key, Path.root_path(), d)

    res = producer.execute_command("FT.INFO " + cct_prepare.TEST_INDEX_NAME)
    print(str(res))

    # ADD INITIAL DATA
    for i in range(total_count):
        d = cct_prepare.generate_single_object(10000000-i , 20000000+i, "aaa")
        key = cct_prepare.TEST_INDEX_PREFIX + str(i+200)
        producer.json().set(key, Path.root_path(), d)

    res = producer.execute_command("FT.INFO " + cct_prepare.TEST_INDEX_NAME)
    print(str(res))

    #print("Client latency is : " + str(mean(latency)) + " microsecond")

    res = producer.execute_command("INFO latencystats")
    print("INFO latencystats output : \n" + str(res) )

    kill_redis()
    time.sleep(2) 


def sortby_text_innerkey_text_test():
    producer = connect_redis_with_start()
    time.sleep(5)
    flush_db(producer)
    time.sleep(5)

    schema = (TextField("$.field1", as_name="field1", sortable=True), TextField("$.field2", as_name="field2", sortable=True),  \
              TextField("$.field3", as_name="field3", sortable=True) , TextField("$.field4", as_name="field4", sortable=True)  )
    producer.ft(TEST_INDEX_NAME).create_index(schema, definition=IndexDefinition(prefix=[cct_prepare.TEST_INDEX_PREFIX], index_type=IndexType.JSON)) 

    total_count = 1000000
    iter_count = 1
    for i in range(total_count):
        key = cct_prepare.TEST_INDEX_PREFIX + "text_key:" + str(i)
        d = {}
        d["field1"] = str(i) + "-text"
        d["field2"] = get_random_string(10)
        d["field3"] = get_random_string(10)
        d["field4"] = key
        producer.json().set(key, Path.root_path(), d)

    # FIRST CLIENT
    client1 = connect_redis()
    client1.execute_command("VIEW.REGISTER app1")
    latency = []
    for i in range(iter_count):
        start_set_time = time.perf_counter_ns()
        response = client1.execute_command("VIEW.SEARCH test_index * SORTBY 2 field1 DESC LIMIT 0 10 INNERKEY field4")
        #print(str(response))
        end_set_time = time.perf_counter_ns()
        latency.append(((end_set_time - start_set_time) // 1000))

    print("Client latency is : " + str(mean(latency)) + " microsecond")

    res = producer.execute_command("INFO latencystats")
    print("INFO latencystats output : \n" + str(res) )

    kill_redis()
    time.sleep(3)

def sortby_text_innerkey_text_with_offset_test():
    producer = connect_redis_with_start()
    time.sleep(5)
    flush_db(producer)
    time.sleep(5)

    schema = (TextField("$.field1", as_name="field1", sortable=True), TextField("$.field2", as_name="field2", sortable=True),  \
              TextField("$.field3", as_name="field3", sortable=True) , TextField("$.field4", as_name="field4", sortable=True)  )
    producer.ft(TEST_INDEX_NAME).create_index(schema, definition=IndexDefinition(prefix=[cct_prepare.TEST_INDEX_PREFIX], index_type=IndexType.JSON)) 

    total_count = 1000000
    iter_count = 1
    for i in range(total_count):
        key = cct_prepare.TEST_INDEX_PREFIX + "text_key:" + str(i)
        d = {}
        d["field1"] = str(i) + "-text"
        d["field2"] = get_random_string(10)
        d["field3"] = get_random_string(10)
        d["field4"] = key
        producer.json().set(key, Path.root_path(), d)

    # FIRST CLIENT
    client1 = connect_redis()
    client1.execute_command("FT.CONFIG SET MAXSEARCHRESULTS 1000000")
    client1.execute_command("VIEW.REGISTER app1")
    latency = []
    for i in range(iter_count):
        start_set_time = time.perf_counter_ns()
        response = client1.execute_command("VIEW.SEARCH test_index * SORTBY 2 field1 DESC LIMIT 900000 10 INNERKEY field4")
        #print(str(response))
        end_set_time = time.perf_counter_ns()
        latency.append(((end_set_time - start_set_time) // 1000))

    print("Client latency is : " + str(mean(latency)) + " microsecond")

    res = producer.execute_command("INFO latencystats")
    print("INFO latencystats output : \n" + str(res) )

    kill_redis()
    time.sleep(3)

def sortby_numeric_innerkey_numeric_test():
    producer = connect_redis_with_start()
    time.sleep(10)
    flush_db(producer)
    time.sleep(10)

    schema = (NumericField("$.field1", as_name="field1", sortable=True), TextField("$.field2", as_name="field2", sortable=True),  \
              TextField("$.field3", as_name="field3", sortable=True) , NumericField("$.field4", as_name="field4", sortable=True)  )
    producer.ft(TEST_INDEX_NAME).create_index(schema, definition=IndexDefinition(index_type=IndexType.JSON))  # no prefix!!!

    total_count = 2000000
    iter_count = 1
    for i in range(total_count):
        key = i
        d = {}
        d["field1"] = 1000 + i
        d["field2"] = get_random_string(10)
        d["field3"] = get_random_string(10)
        d["field4"] = key
        producer.json().set(key, Path.root_path(), d)

    # FIRST CLIENT
    client1 = connect_redis()
    client1.execute_command("VIEW.REGISTER app1")
    latency = []
    for i in range(iter_count):
        start_set_time = time.perf_counter_ns()
        response = client1.execute_command("VIEW.SEARCH test_index * SORTBY 2 field1 DESC LIMIT 0 10 INNERKEY field4")
        #print(str(response))
        end_set_time = time.perf_counter_ns()
        latency.append(((end_set_time - start_set_time) // 1000))

    print("Client latency is : " + str(mean(latency)) + " microsecond")

    res = producer.execute_command("INFO latencystats")
    print("INFO latencystats output : \n" + str(res) )

    kill_redis()
    time.sleep(5)     

def sortby_numeric_innerkey_numeric_with_offset_test():
    producer = connect_redis_with_start()
    time.sleep(10)
    flush_db(producer)
    time.sleep(10)

    schema = (NumericField("$.field1", as_name="field1", sortable=True), TextField("$.field2", as_name="field2", sortable=True),  \
              TextField("$.field3", as_name="field3", sortable=True) , NumericField("$.field4", as_name="field4", sortable=True)  )
    producer.ft(TEST_INDEX_NAME).create_index(schema, definition=IndexDefinition(index_type=IndexType.JSON))  # no prefix!!!

    total_count = 1000000
    iter_count = 1
    for i in range(total_count):
        key = i
        d = {}
        d["field1"] = 1000 + i
        d["field2"] = get_random_string(10)
        d["field3"] = get_random_string(10)
        d["field4"] = key
        producer.json().set(key, Path.root_path(), d)

    # FIRST CLIENT
    client1 = connect_redis()
    client1.execute_command("FT.CONFIG SET MAXSEARCHRESULTS 1000000")
    client1.execute_command("VIEW.REGISTER app1")
    latency = []
    for i in range(iter_count):
        start_set_time = time.perf_counter_ns()
        response = client1.execute_command("VIEW.SEARCH test_index * SORTBY 2 field1 DESC LIMIT 900000 10 INNERKEY field4")
        #print(str(response))
        end_set_time = time.perf_counter_ns()
        latency.append(((end_set_time - start_set_time) // 1000))

    print("Client latency is : " + str(mean(latency)) + " microsecond")

    res = producer.execute_command("INFO latencystats")
    print("INFO latencystats output : \n" + str(res) )

    kill_redis()
    time.sleep(5)     



def sortby_numeric_innerkey_text_test():
    producer = connect_redis_with_start()
    time.sleep(5)
    flush_db(producer)
    time.sleep(5)

    schema = (NumericField("$.field1", as_name="field1", sortable=True), TextField("$.field2", as_name="field2", sortable=True),  \
              TextField("$.field3", as_name="field3", sortable=True) , TextField("$.field4", as_name="field4", sortable=True)  )
    producer.ft(TEST_INDEX_NAME).create_index(schema, definition=IndexDefinition(prefix=[cct_prepare.TEST_INDEX_PREFIX], index_type=IndexType.JSON)) 

    total_count = 1000000
    iter_count = 1
    for i in range(total_count):
        key = cct_prepare.TEST_INDEX_PREFIX + str(i)
        d = {}
        d["field1"] = 1000 + i
        d["field2"] = get_random_string(10)
        d["field3"] = get_random_string(10)
        d["field4"] = key
        producer.json().set(key, Path.root_path(), d)

    # FIRST CLIENT
    client1 = connect_redis()
    client1.execute_command("VIEW.REGISTER app1")
    latency = []
    for i in range(iter_count):
        start_set_time = time.perf_counter_ns()
        response = client1.execute_command("VIEW.SEARCH test_index * SORTBY 2 field1 DESC LIMIT 0 10 INNERKEY field4")
        #print(str(response))
        end_set_time = time.perf_counter_ns()
        latency.append(((end_set_time - start_set_time) // 1000))

    print("Client latency is : " + str(mean(latency)) + " microsecond")

    res = producer.execute_command("INFO latencystats")
    print("INFO latencystats output : \n" + str(res) )

    kill_redis()
    time.sleep(5)

def sortby_numeric_innerkey_text_with_offset_test():
    producer = connect_redis_with_start()
    time.sleep(5)
    flush_db(producer)
    time.sleep(5)

    schema = (NumericField("$.field1", as_name="field1", sortable=True), TextField("$.field2", as_name="field2", sortable=True),  \
              TextField("$.field3", as_name="field3", sortable=True) , TextField("$.field4", as_name="field4", sortable=True)  )
    producer.ft(TEST_INDEX_NAME).create_index(schema, definition=IndexDefinition(prefix=[cct_prepare.TEST_INDEX_PREFIX], index_type=IndexType.JSON)) 

    total_count = 1000000
    iter_count = 1
    for i in range(total_count):
        key = cct_prepare.TEST_INDEX_PREFIX + str(i)
        d = {}
        d["field1"] = 1000 + i
        d["field2"] = get_random_string(10)
        d["field3"] = get_random_string(10)
        d["field4"] = key
        producer.json().set(key, Path.root_path(), d)

    # FIRST CLIENT
    client1 = connect_redis()
    client1.execute_command("FT.CONFIG SET MAXSEARCHRESULTS 1000000")
    client1.execute_command("VIEW.REGISTER app1")
    latency = []
    for i in range(iter_count):
        start_set_time = time.perf_counter_ns()
        response = client1.execute_command("VIEW.SEARCH test_index * SORTBY 2 field1 DESC LIMIT 900000 10 INNERKEY field4")
        #print(str(response))
        end_set_time = time.perf_counter_ns()
        latency.append(((end_set_time - start_set_time) // 1000))

    print("Client latency is : " + str(mean(latency)) + " microsecond")

    res = producer.execute_command("INFO latencystats")
    print("INFO latencystats output : \n" + str(res) )

    kill_redis()
    time.sleep(5)

def sortby_text_innerkey_numeric_test():
    producer = connect_redis_with_start()
    time.sleep(5)
    flush_db(producer)
    time.sleep(5)

    schema = (TextField("$.field1", as_name="field1", sortable=True), TextField("$.field2", as_name="field2", sortable=True),  \
              TextField("$.field3", as_name="field3", sortable=True) , NumericField("$.field4", as_name="field4", sortable=True)  )
    producer.ft(TEST_INDEX_NAME).create_index(schema, definition=IndexDefinition(index_type=IndexType.JSON)) 

    total_count = 1000000
    iter_count = 1
    for i in range(total_count):
        key = i
        d = {}
        d["field1"] = str(1000+i)
        d["field2"] = get_random_string(10)
        d["field3"] = get_random_string(10)
        d["field4"] = key
        producer.json().set(key, Path.root_path(), d)

    # FIRST CLIENT
    client1 = connect_redis()
    client1.execute_command("VIEW.REGISTER app1")
    latency = []
    for i in range(iter_count):
        start_set_time = time.perf_counter_ns()
        response = client1.execute_command("VIEW.SEARCH test_index * SORTBY 2 field1 DESC LIMIT 0 10 INNERKEY field4")
        #print(str(response))
        end_set_time = time.perf_counter_ns()
        latency.append(((end_set_time - start_set_time) // 1000))

    print("Client latency is : " + str(mean(latency)) + " microsecond")

    res = producer.execute_command("INFO latencystats")
    print("INFO latencystats output : \n" + str(res) )

    kill_redis()
    time.sleep(5)   

def sortby_text_innerkey_numeric_with_offset_test():
    producer = connect_redis_with_start()
    time.sleep(5)
    flush_db(producer)
    time.sleep(5)

    schema = (TextField("$.field1", as_name="field1", sortable=True), TextField("$.field2", as_name="field2", sortable=True),  \
              TextField("$.field3", as_name="field3", sortable=True) , NumericField("$.field4", as_name="field4", sortable=True)  )
    producer.ft(TEST_INDEX_NAME).create_index(schema, definition=IndexDefinition(index_type=IndexType.JSON)) 

    total_count = 1000000
    iter_count = 1
    for i in range(total_count):
        key = i
        d = {}
        d["field1"] = str(1000+i)
        d["field2"] = get_random_string(10)
        d["field3"] = get_random_string(10)
        d["field4"] = key
        producer.json().set(key, Path.root_path(), d)

    # FIRST CLIENT
    client1 = connect_redis()
    client1.execute_command("FT.CONFIG SET MAXSEARCHRESULTS 1000000")
    client1.execute_command("VIEW.REGISTER app1")
    latency = []
    for i in range(iter_count):
        start_set_time = time.perf_counter_ns()
        response = client1.execute_command("VIEW.SEARCH test_index * SORTBY 2 field1 DESC LIMIT 900000 10 INNERKEY field4")
        #print(str(response))
        end_set_time = time.perf_counter_ns()
        latency.append(((end_set_time - start_set_time) // 1000))

    print("Client latency is : " + str(mean(latency)) + " microsecond")

    res = producer.execute_command("INFO latencystats")
    print("INFO latencystats output : \n" + str(res) )

    kill_redis()
    time.sleep(5)   

def ft_aggreagate_with_sortby_and_innerkey_variation_test():
    producer = connect_redis_with_start()
    time.sleep(5)
    flush_db(producer)
    time.sleep(5)

    schema = (NumericField("$.field1", as_name="field1", sortable=True), TextField("$.field2", as_name="field2", sortable=True),  \
              TextField("$.field3", as_name="field3", sortable=True) , NumericField("$.field4", as_name="field4", sortable=True)  )
    producer.ft(TEST_INDEX_NAME).create_index(schema, definition=IndexDefinition(index_type=IndexType.JSON)) 

    total_count = 5
    iter_count = 10
    for i in range(total_count):
        key = i
        d = {}
        d["field1"] = 1000+i
        d["field2"] = get_random_string(10)
        d["field3"] = get_random_string(10)
        d["field4"] = key
        producer.json().set(key, Path.root_path(), d)

    # FIRST CLIENT
    client1 = connect_redis()
    client1.execute_command("VIEW.REGISTER app1")
    latency = []
    for i in range(iter_count):
        start_set_time = time.perf_counter_ns()
        response = client1.execute_command("FT.AGGREGATE test_index * SORTBY 4 @field1 DESC @field4 ASC LIMIT 0 3 DIALECT 4")
        print(str(response))
        end_set_time = time.perf_counter_ns()
        latency.append(((end_set_time - start_set_time) // 1000))

    print("Client latency is : " + str(mean(latency)) + " microsecond")

    res = producer.execute_command("INFO latencystats")
    print("INFO latencystats output : \n" + str(res) )

    kill_redis()
    time.sleep(5)    

if __name__=="__main__": 
    main() 
