import subprocess
import time
import redis
import os

def kill_redis():
    bashCommand = "redis-cli shutdown"
    process = subprocess.Popen(bashCommand.split(), 
                                stdin=subprocess.DEVNULL,
                                stdout=subprocess.DEVNULL,
                                stderr=subprocess.DEVNULL,
                                start_new_session=True)    
    time.sleep(1)

def kill_replica_redis(port):
    bashCommand = "redis-cli -p " + str(port) + " shutdown"
    process = subprocess.Popen(bashCommand.split(), 
                                stdin=subprocess.DEVNULL,
                                stdout=subprocess.DEVNULL,
                                stderr=subprocess.DEVNULL,
                                start_new_session=True)    
    time.sleep(1)

def start_redis():
    #current_working_directory = os.getcwd()
    #module = current_working_directory  + "/bin/cct-view.so"
    #bashCommand = "redis-stack-server --loadmodule " + module
    bashCommand = "redis-stack-server"
    subprocess.Popen(bashCommand.split(), 
                    stdin=subprocess.DEVNULL,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    start_new_session=True)
    time.sleep(2)

#Only works for already added config files , for more ports add more conf files
def connect_redis_replica_with_start(r_port): 
    current_working_directory = os.getcwd()
    full_conf_file = current_working_directory  + "/python/test_cluster_redis_configs/" + str(r_port) + "/redis-stack.conf"
    print(full_conf_file)
    #bashCommand = "redis-stack-server --loadmodule " + module
    bashCommand = "redis-stack-server " + full_conf_file
    subprocess.Popen(bashCommand.split(), 
                    stdin=subprocess.DEVNULL,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    start_new_session=True)
    time.sleep(2)
    r = redis.Redis(host='localhost', port=r_port, decode_responses=True)
    return r


def start_redis_without_module():
    bashCommand = "redis-stack-server"
    subprocess.Popen(bashCommand.split(), 
                    stdin=subprocess.DEVNULL,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    start_new_session=True)
    time.sleep(2)    

def connect_redis_with_start():
    start_redis()
    r = redis.Redis(host='localhost', port=6379, decode_responses=True)
    return r

def connect_redis_with_start_without_module():
    start_redis_without_module()
    r = redis.Redis(host='localhost', port=6379, decode_responses=True)
    return r

def connect_redis():
    r = redis.Redis(host='localhost', port=6379, decode_responses=True)
    return r