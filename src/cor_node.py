#!/usr/bin/python
import os
activate_this = os.path.abspath(os.getcwd()) + '/venv/bin/activate_this.py'
execfile(activate_this, dict(__file__=activate_this))

# Logging Message ID range 90101-91000
# Last message ID used 90101
from daemon import Daemon
import argparse
import bisect
import threading
from flask import Flask, request, jsonify
import time
from datetime import datetime
import logging
from logging.handlers import RotatingFileHandler
import traceback
from hashlib import md5
app = Flask(__name__)
logger = logging.getLogger(__name__)

RESULT_SUCCESS = 0
RESULT_FAILED = 1
KEY_RESULT = "result"
KEY_MSG = "msg"
KEY_VALUE = "value"
KEY_KEY = "key"
KEY_IDENTITY = "identity"
KEY_RING = "ring"
KEY_NODE = "node"

URL_GET_ALL_NODES = "/getallnodes"
URL_REGISTER = "/register"
URL_HEARTBEAT = "/hb"
URL_GETNODE = "/getnode"
URL_GET_NEXT_NODE = "/getnextnode"

hashRing = None

def get_time_diff_seconds(e_time, b_time):
    time_diff = e_time - b_time
    seconds_diff = float(time_diff.seconds + time_diff.days * 24 * 3600)
    return seconds_diff

class HashRing():
    ringsize = None
    ring = None
    book = None
    def __init__(self, ringsize):
        self.ringsize = ringsize
        # ring contains consistent hashses of nodes (place within ring).
        self.ring = []
        # book contains hash : (node:port, lastHBTimeStamp)
        # ex: {275 : ("0.0.0.0:6001", "1-1-2021-22:13:45")
        self.book = {}
        self._key_lock = threading.Lock()
    
    def getConsistentHash(self, key):
        return int(md5(key).hexdigest(), 16) % self.ringsize

    # place node on ring; ring is sorted by consistent hashes of nodes; place appropriatly
    # ex. node_identity : 0.0.0.0:60001
    def addNodeToRing(self, node_identity):
        with self._key_lock:
            bisect.insort(self.ring, self.getConsistentHash(node_identity))
            self.book[self.getConsistentHash(node_identity)] = (node_identity, datetime.now())
        logger.info("Added node %s to hashRing successfully", node_identity)
        logger.info("hashRing look like %s", str(self.ring))

    def removeNodeFromRing(self, node_identity):
        try:
            with self._key_lock:
                del self.ring[bisect.bisect_left(self.ring, self.getConsistentHash(node_identity))]
                del self.book[node]
        except IndexError:
            pass
        logger.info("Removed node %s to hashRing", node_identity)

    # find next node in clockwise direction to given key
    def findNextNodeFromRing(self, key):
        # todo: revisit wheather we need lock here
        if not len(self.ring):
            return None
        index = bisect.bisect_right(self.ring, self.getConsistentHash(key)) % len(self.ring)
        if index == len(self.ring):
            return None
        # return next node in clockwise direction
        return self.book[self.ring[index]][0]

    def isNodePresentInRing(self, node_identity):
        i = bisect.bisect_left(self.ring,  self.getConsistentHash(node_identity))
        if i != len(self.ring) and self.ring[i] == self.getConsistentHash(node_identity):
            return True
        return False

    # called from a thread; for registed nodes; check whether they are active by using HB sent by individual hash nodes servers.
    def checkRemoveNANodesFromRing(self):
        while (True):
            tobedel = []
            for node in self.book:
                diff = get_time_diff_seconds(datetime.now(), self.book[node][1])
                if diff > 120:
                    # remove node from book and ring
                    tobedel.append(node)
            for node in tobedel:
                logger.info("Removing node %s; no HB from node from last 2 mins", node)
                self.removeNodeFromRing(node)
            time.sleep(60)
        return

    def addHBToBook(self, identity, ts):
        self.book[self.getConsistentHash(identity)] = (identity, ts)
        return

class CoordinatorNodeService(Daemon):
    port = None
    def start(self, port):
        self.port = port
        Daemon.start(self)

    def run(self):
        try:
            logger.info("Starting clean up thread to remove inactive nodes from hashRing")
            cleanup_thread = threading.Thread(target=hashRing.checkRemoveNANodesFromRing)
            cleanup_thread.start()
            # Start flask app
            logger.info("Starting Flask server")
            app.run('0.0.0.0', self.port, threaded=True)
        except:
            logger.error("%s", traceback.format_exc())

# Called by hash node servers to registred themselves once they are up and running
@app.route(URL_REGISTER, methods=['POST'])
def register_node():
    try:
        identity = request.args.get(KEY_IDENTITY)
        if not hashRing.isNodePresentInRing(identity):
            hashRing.addNodeToRing(identity)
            return (jsonify({KEY_RESULT: RESULT_SUCCESS, KEY_MSG : "Registered Successfully" }), 200)
        else:
            return (jsonify({KEY_RESULT: RESULT_SUCCESS, KEY_MSG : "Already registered" }), 200)
    except:
        logger.error("%s", traceback.format_exc())

# Periodically called by hash node servers; here cor node can make sure that nodes are active within ring
@app.route(URL_HEARTBEAT, methods=['POST'])
def heartbeat():
    try:
        identity = request.args.get(KEY_IDENTITY)
        if not hashRing.isNodePresentInRing(identity):
            return (jsonify({KEY_RESULT: RESULT_FAILED, KEY_MSG : "HT Node %s isn't registred" %(identity) }), 403)
        hashRing.addHBToBook(identity, datetime.now())
        return (jsonify({KEY_RESULT: RESULT_SUCCESS, KEY_MSG : "HB Successfully" }), 200)
    except:
        logger.error("%s", traceback.format_exc())

# Client first checks which server it should connect to; hence it asks for next hash node sever on the ring.
@app.route(URL_GETNODE, methods=['GET'])
def get_ht_node_for_key():
    try:
        key = request.args.get(KEY_KEY)
        node = hashRing.findNextNodeFromRing(key)
        if not node:
            return (jsonify({KEY_RESULT: RESULT_FAILED, KEY_MSG : "No HT nodes are available"}), 403)
        return (jsonify({KEY_RESULT: RESULT_SUCCESS, KEY_NODE : node }), 200)
    except:
        logger.error("%s", traceback.format_exc())

# return ring as a with active hash nodes server list
@app.route(URL_GET_ALL_NODES, methods=['GET'])
def get_all_nodes():
    try:
        return (jsonify({KEY_RESULT: RESULT_SUCCESS, KEY_RING : str(hashRing.ring) }), 200)
    except:
        logger.error("%s", traceback.format_exc())

# find next node to given node on ring
@app.route(URL_GET_NEXT_NODE, methods=['GET'])
def get_next_node():
    try:
        node = request.args.get(KEY_NODE)
        next_node = hashRing.findNextNodeFromRing(node)
        return (jsonify({KEY_RESULT: RESULT_SUCCESS, KEY_NODE : next_node }), 200)
    except:
        logger.error("%s", traceback.format_exc())

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='CoordinatorNodeService Service',
                                    formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    group = parser.add_mutually_exclusive_group()
    group.add_argument('--start', action='store_true', help='Start Service')
    group.add_argument('--stop', action='store_true', help='Stop Service')
    parser.add_argument('--port', help='Specify port for http server')
    
    args = parser.parse_args()

    logger.setLevel(logging.DEBUG)
    handler = RotatingFileHandler("cor_node_%s.log" %(args.port), maxBytes=10485760, backupCount=10)
    formatter = logging.Formatter('[%(asctime)-15s] [%(name)s] %(levelname)s]: %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    if not (args.start or args.stop):
        parser.error('No args provided from start/stop/restart')

    if (args.start and not args.port):
        parser.error("Port is missing to start/restart server")

    logger.info("Creating hashRing of size 360")
    hashRing = HashRing(360)
    CorServiceObj = CoordinatorNodeService('/tmp/cor_node_%s.pid' %(args.port))
    if args.start:
        CorServiceObj.start(args.port)
    elif args.stop:
        CorServiceObj.stop()
    '''
    hashRing = HashRing(360)
    logger.info("starting flask app")
    cleanup_thread = threading.Thread(target=checkRemoveNANodesFromRing)
    cleanup_thread.start()
    app.run('0.0.0.0', args.port)
    '''
