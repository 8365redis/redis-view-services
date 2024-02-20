from manage_redis import connect_redis_with_start, connect_redis
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

pytest.skip("Skip during test , remove this during manual run", allow_module_level=True)

TEST_PERF_INDEX_NAME = "Messages"
TEST_PERF_INDEX_PREFIX = "Message:"
CLIENT_NAME_PREFIX = "Client-"


LOG_TIME_INTERVAL = 1 # seconds interval it will print the latency values
TOTAL_INSERT_COUNT = 10000 # total number of keys to insert 
THREAD_COUNT = 10 # total thread count to insert and update
TOTAL_UPDATE_RATE_START = 100 # total update count per second (it will divided to thread count )
MAX_UPDATE_RATE_PER_THREAD = 5000 # after update rate per thread increased and passed this number it will reset to start rate
RATE_INCREASE_DELAY = 4 # seconds interval delay rate for increase
RATE_INCREASE_MULTIPLY = 3 # times rate will increase (for example for 2 : 3 -> 6 -> 12 -> 24)
CLIENT_COUNT = 10
CLIENT_QUERY_INTERVAL = 1 # seconds interval client will send view.search
CLIENT_VIEW_SEARCH_VIEW_SIZE = 50 # size of the view , key count we are interested


COUNT_PER_THREAD = TOTAL_INSERT_COUNT // THREAD_COUNT 


def generate_single_perf_object():
    return {
        "name": random.randint(0, 20),
        "region": "region",
        "group": "group",
        "subgroup": "region",
        "value1": random.randint(0, 100000),
        "value2": random.randint(0, 100000),
        "value3": random.randint(0, 100000),
        "state": "NORMAL",
        "updatedBy": "someuserid",
        "updatedDate": "2023-09-27T13:34:59.794Z",
        "Type": "Type1",
        "id": str(uuid.uuid4())  
    }

def create_perf_index(client):
    schema = (
        TagField('id', as_name='id'),
        TagField('group', as_name='group'),
        TagField('name', as_name='name', sortable=True),
        NumericField('value1', as_name='value1', sortable=True)
    )
    index_definition = IndexDefinition(prefix=[TEST_PERF_INDEX_PREFIX], index_type=IndexType.JSON)
    client.ft(TEST_PERF_INDEX_NAME).create_index(schema, definition=index_definition)


def insert_perf_data(redis_client, count, thread_id):
    for i in range(count):
        obj = generate_single_perf_object()
        redis_client.json().set(TEST_PERF_INDEX_PREFIX + str(i) + "-" + str(thread_id), Path.root_path(), obj)

def update_perf_data(redis_client, count, thread_id, update_rate_per_thread, stop_event, set_latency):
    while not stop_event.is_set():
        obj = generate_single_perf_object()
        prefix = random.randint(0,count-1)
        start_set_time = time.time_ns()
        redis_client.json().set(TEST_PERF_INDEX_PREFIX + str(prefix) + "-" + str(thread_id), Path.root_path(), obj)
        end_set_time = time.time_ns()
        time.sleep(1 / update_rate_per_thread[0])
        set_latency.append((end_set_time - start_set_time) // 1000)

def extract_latency_values_from_string(input_string, command):
    command_text = r'latency_percentiles_usec_' + command
    pattern = command_text + r':p50=([\d.]+),p99=([\d.]+),p99\.9=([\d.]+)'
    match = re.search(pattern, input_string)
    if match:
        return match.group(1), match.group(2), match.group(3)
    else:
        return None


def log_set_latency(producer, update_rate_per_thread, set_latency):
    while True:
        print("---------------------------------------------------------")
        print("Update per thread rate : %d , Average client (naive) set latency : %d" %(update_rate_per_thread[0], mean(set_latency)) )
        set_latency.clear()
        set_latency.append(0) # workaround
        time.sleep(LOG_TIME_INTERVAL)
        res = producer.execute_command("INFO latencystats") # Also run ? 'redis-benchmark -t set -r 100000 -n 1000000'
        command = r'json\.set'
        pvalues = extract_latency_values_from_string(str(res), command)
        print(f"Redis json.set latencies p50 p99 p99.9 : {pvalues}" )
        command = r'VIEW\.MAIN\.SEARCH'
        pvalues = extract_latency_values_from_string(str(res), command)
        print(f"Redis VIEW.MAIN.SEARCH latencies p50 p99 p99.9 : {pvalues}" )
        command = r'VIEW\.SEARCH'
        pvalues = extract_latency_values_from_string(str(res), command)
        print(f"Redis VIEW.SEARCH latencies p50 p99 p99.9 : {pvalues}" )

def client_action(client_id, stop_event):
    client = connect_redis()
    client.execute_command("VIEW.REGISTER " + CLIENT_NAME_PREFIX + str(client_id))
    name = random.randint(0, 20)
    #client.execute_command(f"VIEW.MAIN.SEARCH {TEST_PERF_INDEX_NAME} @name:{{name}}")
    while not stop_event.is_set():
        client.execute_command(f"VIEW.SEARCH {TEST_PERF_INDEX_NAME} @name:{{name}} LIMIT 0 {CLIENT_VIEW_SEARCH_VIEW_SIZE} SORTBY value1")
        #print("View Search is send")
        time.sleep(CLIENT_QUERY_INTERVAL)

def test_main_perf():
    producer = connect_redis_with_start()
    #producer = connect_redis()
    producer.flushall()
    create_perf_index(producer)

    update_rate_per_thread = [0]
    update_rate_per_thread[0] = TOTAL_UPDATE_RATE_START // THREAD_COUNT
   
    threads = []

    # Insert
    for i in range(THREAD_COUNT):
        thread = threading.Thread(target=insert_perf_data, args=(producer, COUNT_PER_THREAD, i))
        thread.start()
        threads.append(thread)

    for thread in threads:
        thread.join()
    
    threads.clear()

    # Update
    stop_events = []
    set_latency = [0] # workaround
    for i in range(THREAD_COUNT):
        stop_event = threading.Event()
        thread = threading.Thread(target=update_perf_data, args=(producer, COUNT_PER_THREAD, i, update_rate_per_thread, stop_event, set_latency))
        thread.start()
        threads.append(thread)
        stop_events.append(stop_event)

    # Logging
    thread = threading.Thread(target=log_set_latency, args=(producer, update_rate_per_thread, set_latency))
    thread.start()
    threads.append(thread)


    # Clients
    for i in range(CLIENT_COUNT):
        stop_event = threading.Event()
        thread = threading.Thread(target=client_action, args=(i, stop_event))
        thread.start()
        threads.append(thread)

    try:
        # Keep the main thread running
        while True:
            time.sleep(RATE_INCREASE_DELAY)
            update_rate_per_thread[0] = int(update_rate_per_thread[0] * RATE_INCREASE_MULTIPLY)
            print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
            print("Increase rate to per thread : " + str(update_rate_per_thread[0]))
            if update_rate_per_thread[0] > MAX_UPDATE_RATE_PER_THREAD:
                update_rate_per_thread[0] = TOTAL_UPDATE_RATE_START // THREAD_COUNT

    except KeyboardInterrupt:
        # Stop all threads when a keyboard interrupt is received
        print("Stopping threads...")
        for event in stop_events:
            event.set()

    for thread in threads:
        thread.join()

def main():
    print("Perf Test starts")
    test_main_perf()
    print("Perf Test ends")


if __name__=="__main__": 
    main() 
