import time
from manage_redis import kill_redis, connect_redis_with_start, connect_redis
from manage_redis_data import flush_db, create_index, add_json_data, TEST_INDEX_NAME
import pytest
import cct_prepare
from redis.commands.json.path import Path
from redis.commands.search.query import GeoFilter, NumericFilter, Query
from redis.commands.search.result import Result
from random_json_generator import RandomJsonGenerator

pytest.skip("Skip during test , remove this during manual run", allow_module_level=True)

@pytest.fixture(autouse=True)
def before_and_after_test():
    print("Start")
    yield
    kill_redis()
    print("End")

TOTAL = 1000


def test_add_data():
    producer = connect_redis_with_start()
    time.sleep(1)
    flush_db(producer) # clean all db first
    generator = RandomJsonGenerator()
    generator.create_index(producer)

    iteration = 1
    #ADD INITIAL DATA
    for i in range(TOTAL):
        random_json_object = generator.generate_random_json()
        key = cct_prepare.TEST_INDEX_PREFIX + str(i)
        producer.json().set(key, Path.root_path(), random_json_object)



def test_aggragate_perf():
    producer = connect_redis_with_start()
    time.sleep(60)

    iteration = 1
    client1 = connect_redis()
    for i in range(iteration):
        client1.execute_command(f"FT.AGGREGATE test_index * SORTBY 4 @region ASC @uuid ASC LIMIT 0 10 DIALECT 4")
        client1.execute_command(f"FT.AGGREGATE test_index * SORTBY 4 @region ASC @uuid ASC LIMIT 500 600 DIALECT 4")

        #client1.execute_command(f"FT.AGGREGATE test_index * SORTBY 6 @tallyState ASC @uuid ASC @region ASC LIMIT 0 25")
        #client1.execute_command(f"FT.AGGREGATE test_index * SORTBY 6 @tallyState ASC @uuid ASC @region ASC LIMIT 150 200")

        #client1.execute_command(f"FT.AGGREGATE test_index * SORTBY 6 @uuid DESC @value ASC @region ASC LIMIT 0 10")
        #client1.execute_command(f"FT.AGGREGATE test_index * SORTBY 6 @uuid DESC @value ASC @region ASC LIMIT 900 950")

        #client1.execute_command(f"FT.AGGREGATE test_index * SORTBY 6 @tallyState ASC @uuid ASC @region ASC LIMIT 50 60")
        #client1.execute_command(f"FT.AGGREGATE test_index * SORTBY 6 @tallyState ASC @uuid ASC @region ASC LIMIT 750 950")
    
    res = client1.execute_command("INFO latencystats")
    print("AGGRAGATE")
    print(res)


def test_aggragate_load_perf():
    producer = connect_redis_with_start()
    time.sleep(60)
    iteration = 1
    client1 = connect_redis()
    for i in range(iteration):
        client1.execute_command(f"FT.AGGREGATE test_index * LOAD 1 @__key SORTBY 4 @region ASC @__key ASC LIMIT 0 10 DIALECT 4")
        client1.execute_command(f"FT.AGGREGATE test_index * LOAD 1 @__key SORTBY 4 @region ASC @__key ASC LIMIT 500 600 DIALECT 4")

        #client1.execute_command(f"FT.AGGREGATE test_index * LOAD 1 @__key SORTBY 4 @region ASC @__key ASC LIMIT 0 25")
        #client1.execute_command(f"FT.AGGREGATE test_index * LOAD 1 @__key SORTBY 4 @region ASC @__key ASC LIMIT 150 200")

        #client1.execute_command(f"FT.AGGREGATE test_index * LOAD 1 @__key SORTBY 4 @uuid DESC @__key ASC LIMIT 0 10")
        #client1.execute_command(f"FT.AGGREGATE test_index * LOAD 1 @__key SORTBY 4 @uuid DESC @__key ASC LIMIT 900 950")

        #client1.execute_command(f"FT.AGGREGATE test_index * LOAD 1 @__key SORTBY 4 @tallyState ASC @__key ASC LIMIT 50 60")
        #client1.execute_command(f"FT.AGGREGATE test_index * LOAD 1 @__key SORTBY 4 @tallyState ASC @__key ASC LIMIT 750 950")
    
    res = client1.execute_command("INFO latencystats")
    print("AGGRAGATE LOAD")
    print(res)

def test_search_perf():
    producer = connect_redis_with_start()
    time.sleep(60)
    iteration = 1
    client1 = connect_redis()
    for i in range(iteration):
        client1.execute_command(f"FT.SEARCH test_index * SORTBY region ASC LIMIT 0 10 DIALECT 4")
        client1.execute_command(f"FT.SEARCH test_index * SORTBY region ASC LIMIT 500 600 DIALECT 4")

        #client1.execute_command(f"FT.SEARCH test_index * SORTBY region ASC LIMIT 0 25")
        #client1.execute_command(f"FT.SEARCH test_index * SORTBY region ASC LIMIT 150 200")

        #client1.execute_command(f"FT.SEARCH test_index * SORTBY uuid DESC LIMIT 0 10")
        #client1.execute_command(f"FT.SEARCH test_index * SORTBY uuid DESC LIMIT 900 950")

        #client1.execute_command(f"FT.SEARCH test_index * SORTBY tallyState ASC LIMIT 50 60")
        #client1.execute_command(f"FT.SEARCH test_index * SORTBY tallyState ASC LIMIT 750 950")
    
    res = client1.execute_command("INFO latencystats")
    print("SEARCH")
    print(res)


