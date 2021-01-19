#!/usr/bin/python
import os
activate_this = os.path.abspath(os.getcwd()) + '/venv/bin/activate_this.py'
execfile(activate_this, dict(__file__=activate_this))
from daemon import Daemon
import argparse
import hashlib
from flask import Flask, request, jsonify
from threading import Thread, Lock
import json
import requests
import logging
from logging.handlers import RotatingFileHandler
import threading
import sys, traceback
import time
from hashlib import md5
COR_NODE_IP = "0.0.0.0"
COR_NODE_PORT = 5000

logger = logging.getLogger(__name__)

app = Flask(__name__)
hashTable = None
# size in bytes
SIZE = 10485760 
MAX_HT_ROWS = 100
REGISTER_FREQ = 5
HEARTBEATS_FREQ = 10

KEY_TOTAL_PAGES = "total_pages"
KEY_PAGE = "page"
KEY_VALUE = "value"
KEY_KEY = "key"
RESULT_SUCCESS = 0
RESULT_FAILED = 1
KEY_RESULT = "result"
KEY_MSG = "msg"

URL_PUT = "/put"
URL_GET = "/get"
URL_GET_PAGE = "/get/page/<int:page>"
URL_DEL = "/del"


class KeyNotFound(Exception):
    def __init__(self, arg):
            self.msg = arg

class CorRegisterationFailed(Exception):
    def __init__(self, arg):
            self.msg = arg

class SizeExceeded(Exception):
    def __init__(self, arg):
            self.msg = arg

class HashTable():
    # used to update to memory consumed by entire hashtable
    class memory():
        size = 0
        def __init__(self):
            self.size = 0
            self._key_lock = threading.Lock()
        def incr(self, delta):
            with self._key_lock:
                self.size += delta
        def dec(self, delta):
            with self._key_lock:
                self.size -= delta
        def getSize(self):
            return self.size
    # buckets is pointed by index in the hashtable; chain is protected by mutex
    class bucket():
        index = None
        chain = None
        KEY_INSERTED_MSG = "Successfully inserted"
        KEY_UPDATED_MSG = "Successfully updated"
        KEY_DELETED_MSG = "Successfully deleted"

        def __init__(self, index):
            self.index = index
            self.chain = []
            self._key_lock = threading.Lock()
            return
        
        def getIndex(self):
            return self.index

        def insert(self, key, value):
            with self._key_lock:
                for item in self.chain:
                    if key == item[0]:
                        item[1] = value
                        return self.KEY_UPDATED_MSG
                self.chain.append([key,value])
            return self.KEY_INSERTED_MSG
        
        def remove(self, key):
            _chain = []
            size = 0
            with self._key_lock:
                for x in self.chain:
                    if x[0] == key:
                        size = len(x[1])
                    else:
                        _chain.append(x)
                self.chain = _chain
            return (self.KEY_DELETED_MSG, size)

        def find(self, key):
            with self._key_lock:
                for item in self.chain:
                    if key == item[0]:
                        return item[1]
                raise KeyNotFound("Cound not find key %s" %(key))
            
    hashtable = None 
    size = None
    memory_consumed = None
    
    def __init__(self, size):
        self.size = size
        self.memory_consumed = self.memory()
        self.hashtable = [self.bucket(i) for i in range(0, MAX_HT_ROWS)]
        return

    #Todo: write your own hash function; it may not be uniformarly distributed but good for exercise
    def getmyhash(self, key):
        return None

    def insert(self, key, value):
        # todo: this addition should in lock
        if (self.memory_consumed.getSize() + len(value)) >= self.size:
            raise SizeExceeded("Hashtable size is full")
        index = int(md5(key).hexdigest(), 16) % MAX_HT_ROWS
        ret = self.hashtable[index].insert(key, value)
        self.memory_consumed.incr(len(value))
        return ret

    def remove(self, key):
        index = int(md5(key).hexdigest(), 16) % MAX_HT_ROWS
        (ret, size) = self.hashtable[index].remove(key)
        self.memory_consumed.dec(size)
        return ret

    def get(self, key):
        index = int(md5(key).hexdigest(), 16) % MAX_HT_ROWS
        try:
            return self.hashtable[index].find(key)
        except KeyNotFound:
            return None

class HashNodeService(Daemon):
    ip = None
    port = None
    def start(self, port):
        self.port = port
        Daemon.start(self)
    
    def register_and_hb_with_cor(self, identity):
        s = requests.Session()
        while (True):
            try:
                # todo: declare this urls as constants
                r = s.post('http://%s:%d/register?identity=%s' %(COR_NODE_IP, COR_NODE_PORT, identity))
                if r.status_code != 200:
                    logger.error("Registeration with Coordinator failed with error %s and code %d", r.text, r.status_code)
                    continue
                logger.debug("Registeration successful with cooridinator service")
                while (True):
                    r = s.post('http://%s:%d/hb?identity=%s' %(COR_NODE_IP, COR_NODE_PORT, identity))
                    if r.status_code != 200:
                        logger.error("HB with Coordinator failed with error %s and code %d", r.text, r.status_code)
                    if r.status_code == 403:
                        logger.error("HB with Coordinator failed with reason %s and code %d", r.text, r.status_code)
                        break
                    logger.debug("HB with Coordinator successful")
                    time.sleep(HEARTBEATS_FREQ)
            except requests.ConnectionError as e:
                logger.error("%s", str(e))
            except:
                e = sys.exc_info()[0]
                logger.error("%s", traceback.format_exc())
            time.sleep(REGISTER_FREQ)
    
    def run(self):
        try:
            self.ip = "0.0.0.0"
            node_identity = self.ip + ':' + self.port
            # spwan up a thread to perform coorinator registration and heartbeating
            logger.info("Starting a register_and_hb thread")
            hb_thread = threading.Thread(target=self.register_and_hb_with_cor, args=(node_identity,))
            hb_thread.start()
            logger.info("Starting flask app server")
            app.run(self.ip, self.port, threaded=True)
        except:
            e = sys.exc_info()[0]
            logger.error("%s", traceback.format_exc())

@app.route('/')
def hi():
    return 'Hi'

@app.route(URL_PUT, methods=['POST'])
def put():
    try:
        key = request.args.get(KEY_KEY)
        data = request.stream.read()
        msg = ""
        try:
            msg = hashTable.insert(key, data)
        except SizeExceeded as e:
            msg = e.msg
            return (jsonify({ KEY_RESULT : RESULT_FAILED, KEY_KEY: key, KEY_MSG : msg}), 503)
        return (jsonify({ KEY_RESULT : RESULT_SUCCESS, KEY_KEY: key, KEY_MSG : msg}), 200)
    except:
        logger.error("%s", traceback.format_exc())
        

@app.route(URL_GET,  methods=['GET'])
@app.route(URL_GET_PAGE,  methods=['POST'])
def get(page=None):
    # default page of 4bytes (can be changed to 4k; kept 4 bytes for testing purpose)
    PAGE_SIZE = 4
    try:
        key = request.args.get(KEY_KEY)
        value = hashTable.get(key)
        if not value:
            return (jsonify({KEY_RESULT : RESULT_FAILED,  KEY_KEY: key, KEY_VALUE: value}), 404)
        if page != None:
            total_pages = len(value)/4
            if (len(value)%4):
                total_pages+=1;
            value = value[PAGE_SIZE*page: (PAGE_SIZE*page)+PAGE_SIZE]
            return (jsonify({ KEY_RESULT : RESULT_SUCCESS, KEY_KEY: key, KEY_VALUE: value, KEY_PAGE : page, KEY_TOTAL_PAGES:total_pages}), 200)
        return (jsonify({ KEY_RESULT : RESULT_SUCCESS, KEY_KEY: key, KEY_VALUE: value}), 200)
    except:
        logger.error("%s", traceback.format_exc())

@app.route(URL_DEL,  methods=['DELETE'])
def remove():
    try:
        key = request.args.get(KEY_KEY)
        msg = hashTable.remove(key)
        return (jsonify({ KEY_RESULT: RESULT_SUCCESS, KEY_KEY: key, KEY_MSG: msg}), 200)
    except:
        logger.error("%s", traceback.format_exc())

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='HashNode Service', 
                                    formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    group = parser.add_mutually_exclusive_group()
    group.add_argument('--start', action='store_true', help='Start Service')
    group.add_argument('--stop', action='store_true', help='Stop Service')
    parser.add_argument('--port', help='Specify port for http server', required=True)

    args = parser.parse_args()
    HashServiceObj = HashNodeService('/tmp/ht_node_%s.pid' %(args.port))
    
    # todo: put logger in different file
    logger.setLevel(logging.DEBUG)
    handler = RotatingFileHandler("ht_node_%s.log" %(args.port), maxBytes=10485760, backupCount=10)
    formatter = logging.Formatter('[%(asctime)-15s] [%(name)s] %(levelname)s]: %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    logger.info("Creating hashTable instance")
    hashTable = HashTable(SIZE) 
    
    if args.start:
        HashServiceObj.start(args.port)
    elif args.stop:
        HashServiceObj.stop()
    '''
    logger.info("Starting a HB thread with COR")
    hb_thread = threading.Thread(target=HashServiceObj.register_and_hb_with_cor, args=("0.0.0.0:1000",))
    hb_thread.start()
    #HashServiceObj.register_and_hb_with_cor("0.0.0.0:1000")
    app.run('0.0.0.0', args.port)
    '''
