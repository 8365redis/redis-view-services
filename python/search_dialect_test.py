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
    print(str(resp))

