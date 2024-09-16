from manage_redis import kill_redis, connect_redis_with_start, connect_redis
from manage_redis_data import flush_db, create_index, add_json_data, TEST_INDEX_NAME
import pytest
import cct_prepare
from redis.commands.json.path import Path
from redis.commands.search.query import GeoFilter, NumericFilter, Query
from redis.commands.search.result import Result
from redis.commands.json.path import Path
from redis.commands.search.query import GeoFilter, NumericFilter, Query
from redis.commands.search.field import TextField, NumericField, TagField
from redis.commands.search.indexDefinition import IndexDefinition, IndexType
from redis.commands.search.result import Result

@pytest.fixture(autouse=True)
def before_and_after_test():
    print("Start")
    yield
    kill_redis()
    print("End")

def test_view_search_only_sortby_permutations():
    producer = connect_redis_with_start()
    flush_db(producer) # clean all db first
    schema = (TextField("$.aaa", as_name="aaa", sortable=True), TextField("$.bbb", as_name="bbb", sortable=True),  \
              TextField("$.ccc", as_name="ccc", sortable=True) , TextField("$.ddd", as_name="ddd", sortable=True)  )
    producer.ft(TEST_INDEX_NAME).create_index(schema, definition=IndexDefinition(prefix=[cct_prepare.TEST_INDEX_PREFIX], index_type=IndexType.JSON)) 

    # ADD INITIAL DATA
    for i in range(10):
        key = cct_prepare.TEST_INDEX_PREFIX + str(i)
        d = {}
        d["aaa"] = str(i)
        d["bbb"] = str(900+i)
        d["ccc"] = str(300+i)
        d["ddd"] = str(400+i)
        producer.json().set(key, Path.root_path(), d)

    # FIRST CLIENT
    client1 = connect_redis()
    client1.execute_command("VIEW.REGISTER app1")

    # SORTBY a
    response = client1.execute_command("VIEW.SEARCH test_index * SORTBY 2 aaa ASC LIMIT 0 3 ")
    assert response[5][3][3] == '''{"aaa":"0","bbb":"900","ccc":"300","ddd":"400"}'''
    assert response[5][7][3] == '''{"aaa":"1","bbb":"901","ccc":"301","ddd":"401"}'''
    assert response[5][11][3] == '''{"aaa":"2","bbb":"902","ccc":"302","ddd":"402"}'''

    # UPDATE DATA
    for i in range(10):
        key = cct_prepare.TEST_INDEX_PREFIX + str(i)
        d = {}
        d["aaa"] = "a"
        d["bbb"] = str(900+i)
        d["ccc"] = str(300+i)
        d["ddd"] = str(400+i)
        producer.json().set(key, Path.root_path(), d)

    # SORTBY a b ASC
    response = client1.execute_command("VIEW.SEARCH test_index * SORTBY 4 aaa ASC bbb ASC LIMIT 0 3 ")
    assert response[5][3][3] == '''{"aaa":"a","bbb":"900","ccc":"300","ddd":"400"}'''
    assert response[5][7][3] == '''{"aaa":"a","bbb":"901","ccc":"301","ddd":"401"}'''
    assert response[5][11][3] == '''{"aaa":"a","bbb":"902","ccc":"302","ddd":"402"}'''

    # SORTBY a b DESC
    response = client1.execute_command("VIEW.SEARCH test_index * SORTBY 4 aaa ASC bbb DESC LIMIT 0 3 ")
    assert response[5][3][3] == '''{"aaa":"a","bbb":"909","ccc":"309","ddd":"409"}'''
    assert response[5][7][3] == '''{"aaa":"a","bbb":"908","ccc":"308","ddd":"408"}'''
    assert response[5][11][3] == '''{"aaa":"a","bbb":"907","ccc":"307","ddd":"407"}'''

    # UPDATE DATA
    for i in range(10):
        key = cct_prepare.TEST_INDEX_PREFIX + str(i)
        d = {}
        d["aaa"] = "a"
        d["bbb"] = "b"
        d["ccc"] = str(300+i)
        d["ddd"] = str(400+i)
        producer.json().set(key, Path.root_path(), d)

    # SORTBY a b c ASC
    response = client1.execute_command("VIEW.SEARCH test_index * SORTBY 6 aaa ASC bbb DESC ccc ASC LIMIT 0 3 ")
    assert response[5][3][3] == '''{"aaa":"a","bbb":"b","ccc":"300","ddd":"400"}'''
    assert response[5][7][3] == '''{"aaa":"a","bbb":"b","ccc":"301","ddd":"401"}'''
    assert response[5][11][3] == '''{"aaa":"a","bbb":"b","ccc":"302","ddd":"402"}'''

    # SORTBY a b c DESC
    response = client1.execute_command("VIEW.SEARCH test_index * SORTBY 6 aaa ASC bbb DESC ccc DESC LIMIT 0 3 ")
    assert response[5][3][3] == '''{"aaa":"a","bbb":"b","ccc":"309","ddd":"409"}'''
    assert response[5][7][3] == '''{"aaa":"a","bbb":"b","ccc":"308","ddd":"408"}'''
    assert response[5][11][3] == '''{"aaa":"a","bbb":"b","ccc":"307","ddd":"407"}'''


def test_view_search_sortby_filter_innerkey_permutations():
    producer = connect_redis_with_start()
    flush_db(producer) # clean all db first
    schema = (TextField("$.aaa", as_name="aaa", sortable=True), TextField("$.bbb", as_name="bbb", sortable=True),  \
              TextField("$.ccc", as_name="ccc", sortable=True) , TextField("$.ddd", as_name="ddd", sortable=True)  )
    producer.ft(TEST_INDEX_NAME).create_index(schema, definition=IndexDefinition(prefix=[cct_prepare.TEST_INDEX_PREFIX], index_type=IndexType.JSON)) 

    # ADD INITIAL DATA
    for i in range(10):
        key = cct_prepare.TEST_INDEX_PREFIX + str(i)
        d = {}
        d["aaa"] = str(i)
        d["bbb"] = str(900+i)
        d["ccc"] = key
        d["ddd"] = str(400+i)
        producer.json().set(key, Path.root_path(), d)

    # FIRST CLIENT
    client1 = connect_redis()
    client1.execute_command("VIEW.REGISTER app1")

    #sortby a filter b innerkey c
    response = client1.execute_command("VIEW.SEARCH test_index * SORTBY 2 aaa ASC FILTER startswith(@bbb,'90') LIMIT 0 3 INNERKEY ccc")
    assert response[5][3][3] == '''{"aaa":"0","bbb":"900","ccc":"test_data:0","ddd":"400"}'''
    assert response[5][7][3] == '''{"aaa":"1","bbb":"901","ccc":"test_data:1","ddd":"401"}'''
    assert response[5][11][3] == '''{"aaa":"2","bbb":"902","ccc":"test_data:2","ddd":"402"}'''
    

    #sortby a filter a innerkey c
    response = client1.execute_command("VIEW.SEARCH test_index * SORTBY 2 aaa ASC FILTER startswith(@aaa,'0') LIMIT 0 3 INNERKEY ccc")
    assert response[5][3][3] == '''{"aaa":"0","bbb":"900","ccc":"test_data:0","ddd":"400"}'''
    

    #sortby c filter a innerkey c
    response = client1.execute_command("VIEW.SEARCH test_index * SORTBY 2 ccc ASC FILTER startswith(@aaa,'0') LIMIT 0 3 INNERKEY ccc")
    assert response[5][3][3] == '''{"aaa":"0","bbb":"900","ccc":"test_data:0","ddd":"400"}'''

    #sortby a filter d innerkey c
    response = client1.execute_command("VIEW.SEARCH test_index * SORTBY 2 aaa DESC FILTER startswith(@ddd,'40') LIMIT 0 3 INNERKEY ccc")
    assert response[5][3][3] == '''{"aaa":"9","bbb":"909","ccc":"test_data:9","ddd":"409"}'''
    assert response[5][7][3] == '''{"aaa":"8","bbb":"908","ccc":"test_data:8","ddd":"408"}'''
    assert response[5][11][3] == '''{"aaa":"7","bbb":"907","ccc":"test_data:7","ddd":"407"}'''

    #sortby c filter c innerkey c
    response = client1.execute_command("VIEW.SEARCH test_index * SORTBY 2 ccc ASC FILTER startswith(@ccc,'test_data') LIMIT 0 3 INNERKEY ccc")
    assert response[5][3][3] == '''{"aaa":"0","bbb":"900","ccc":"test_data:0","ddd":"400"}'''
    assert response[5][7][3] == '''{"aaa":"1","bbb":"901","ccc":"test_data:1","ddd":"401"}'''
    assert response[5][11][3] == '''{"aaa":"2","bbb":"902","ccc":"test_data:2","ddd":"402"}'''

    #sortby c filter c innerkey c (NO RESP)
    response = client1.execute_command("VIEW.SEARCH test_index * SORTBY 2 ccc ASC FILTER startswith(@ccc,'no_exists') LIMIT 0 3 INNERKEY ccc")
    assert response[5] == []

def test_view_search_sortby_filter_innerkey_prefix_permutations():
    producer = connect_redis_with_start()
    flush_db(producer) # clean all db first
    schema = (TextField("$.aaa", as_name="aaa", sortable=True), TextField("$.bbb", as_name="bbb", sortable=True),  \
              TextField("$.ccc", as_name="ccc", sortable=True) , TextField("$.ddd", as_name="ddd", sortable=True)  )
    producer.ft(TEST_INDEX_NAME).create_index(schema, definition=IndexDefinition(prefix=[cct_prepare.TEST_INDEX_PREFIX], index_type=IndexType.JSON)) 

    # ADD INITIAL DATA
    for i in range(10):
        key = str(i)
        d = {}
        d["aaa"] = str(200-i)
        d["bbb"] = str(900+i)
        d["ccc"] = str(key)
        d["ddd"] = str(400+i)
        producer.json().set(cct_prepare.TEST_INDEX_PREFIX + key, Path.root_path(), d)
    
    # FIRST CLIENT
    client1 = connect_redis()
    client1.execute_command("VIEW.REGISTER app1")

    #sortby a filter b innerkey c prefix
    response = client1.execute_command("VIEW.SEARCH test_index * SORTBY 2 aaa ASC LIMIT 0 3 INNERKEY ccc " + cct_prepare.TEST_INDEX_PREFIX)
    assert response[5][3][3] == '''{"aaa":"191","bbb":"909","ccc":"9","ddd":"409"}'''
    assert response[5][7][3] == '''{"aaa":"192","bbb":"908","ccc":"8","ddd":"408"}'''
    assert response[5][11][3] == '''{"aaa":"193","bbb":"907","ccc":"7","ddd":"407"}'''

    #sortby a filter b innerkey c prefix
    response = client1.execute_command("VIEW.SEARCH test_index * SORTBY 2 aaa ASC FILTER startswith(@bbb,'90') LIMIT 0 3 INNERKEY ccc " + cct_prepare.TEST_INDEX_PREFIX)
    assert response[5][3][3] == '''{"aaa":"191","bbb":"909","ccc":"9","ddd":"409"}'''
    assert response[5][7][3] == '''{"aaa":"192","bbb":"908","ccc":"8","ddd":"408"}'''
    assert response[5][11][3] == '''{"aaa":"193","bbb":"907","ccc":"7","ddd":"407"}'''

    #sortby a filter a innerkey c prefix
    response = client1.execute_command("VIEW.SEARCH test_index * SORTBY 2 aaa ASC FILTER startswith(@aaa,'19') LIMIT 0 3 INNERKEY ccc " + cct_prepare.TEST_INDEX_PREFIX)
    assert response[5][3][3] == '''{"aaa":"191","bbb":"909","ccc":"9","ddd":"409"}'''
    assert response[5][7][3] == '''{"aaa":"192","bbb":"908","ccc":"8","ddd":"408"}'''
    assert response[5][11][3] == '''{"aaa":"193","bbb":"907","ccc":"7","ddd":"407"}'''

    #sortby a filter a innerkey c prefix NO RESP
    response = client1.execute_command("VIEW.SEARCH test_index * SORTBY 2 aaa ASC FILTER startswith(@aaa,'not_exist') LIMIT 0 3 INNERKEY ccc " + cct_prepare.TEST_INDEX_PREFIX)
    assert response[5] == []

    #sortby b filter b innerkey c prefix
    response = client1.execute_command("VIEW.SEARCH test_index * SORTBY 2 bbb ASC FILTER startswith(@bbb,'90') LIMIT 0 3 INNERKEY ccc " + cct_prepare.TEST_INDEX_PREFIX)
    assert response[5][3][3] == '''{"aaa":"200","bbb":"900","ccc":"0","ddd":"400"}'''
    assert response[5][7][3] == '''{"aaa":"199","bbb":"901","ccc":"1","ddd":"401"}'''
    assert response[5][11][3] == '''{"aaa":"198","bbb":"902","ccc":"2","ddd":"402"}'''

    #sortby b filter b innerkey c prefix NO RESP
    response = client1.execute_command("VIEW.SEARCH test_index * SORTBY 2 bbb ASC FILTER startswith(@bbb,'not_exist') LIMIT 0 3 INNERKEY ccc " + cct_prepare.TEST_INDEX_PREFIX)
    assert response[5] == []

    #sortby a b filter d innerkey c prefix
    response = client1.execute_command("VIEW.SEARCH test_index * SORTBY 4 aaa ASC bbb ASC FILTER startswith(@ddd,'40') LIMIT 0 3 INNERKEY ccc " + cct_prepare.TEST_INDEX_PREFIX)
    assert response[5][3][3] == '''{"aaa":"191","bbb":"909","ccc":"9","ddd":"409"}'''
    assert response[5][7][3] == '''{"aaa":"192","bbb":"908","ccc":"8","ddd":"408"}'''
    assert response[5][11][3] == '''{"aaa":"193","bbb":"907","ccc":"7","ddd":"407"}'''

    #sortby a DESC b filter d innerkey c prefix
    response = client1.execute_command("VIEW.SEARCH test_index * SORTBY 4 aaa DESC bbb ASC FILTER startswith(@ddd,'40') LIMIT 0 3 INNERKEY ccc " + cct_prepare.TEST_INDEX_PREFIX)
    assert response[5][3][3] == '''{"aaa":"200","bbb":"900","ccc":"0","ddd":"400"}'''
    assert response[5][7][3] == '''{"aaa":"199","bbb":"901","ccc":"1","ddd":"401"}'''
    assert response[5][11][3] == '''{"aaa":"198","bbb":"902","ccc":"2","ddd":"402"}'''

    #sortby a b c filter d innerkey c prefix
    response = client1.execute_command("VIEW.SEARCH test_index * SORTBY 6 aaa ASC bbb ASC ccc ASC FILTER startswith(@ddd,'40') LIMIT 0 3 INNERKEY ccc " + cct_prepare.TEST_INDEX_PREFIX)
    assert response[5][3][3] == '''{"aaa":"191","bbb":"909","ccc":"9","ddd":"409"}'''
    assert response[5][7][3] == '''{"aaa":"192","bbb":"908","ccc":"8","ddd":"408"}'''
    assert response[5][11][3] == '''{"aaa":"193","bbb":"907","ccc":"7","ddd":"407"}'''
