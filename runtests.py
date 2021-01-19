#!/usr/bin/python
activate_this = 'venv/bin/activate_this.py'
execfile(activate_this, dict(__file__=activate_this))

import subprocess
import requests
import json
import time

CONST_NO_OF_HT_NODES = 5
CONST_MAX_SIZE_OF_HT_IN_BYTES = 1024

CONST_HT_SERVICE_PATH = "src/ht_node.py"
CONST_COR_SERVICE_PATH = "src/cor_node.py"

COR_NODE_IP = "0.0.0.0"
COR_NODE_PORT = 5000

random_str_dict = {
    "4966883053": "9113542871",
    "4849519610": "6299536643",
    "0231616805": "8666445535",
    "4683967315": "3757159710",
    "2422196602": "2011379614",
    "9595145804": "5033185368",
    "7900484710": "4439979268",
    "6741830721": "1405283270",
    "7913825919": "5970370181",
    "6116017206": "1212364887"
}
s = requests.Session()

def RunCommand(cmd):
    print cmd
    ps = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE)
    ps.wait()
    return ps.returncode

def stop_ht_services():
    _iport = 6000
    for i in range(0, CONST_NO_OF_HT_NODES):
        port = _iport+i
        cmd = "./%s --stop --port %d" %(CONST_HT_SERVICE_PATH, port)
        RunCommand(cmd)

def start_ht_services():
    _iport = 6000
    for i in range(0, CONST_NO_OF_HT_NODES):
        port = _iport+i
        cmd = "./%s --start --port %d" %(CONST_HT_SERVICE_PATH, port)
        RunCommand(cmd)

def stop_cor_service():
    port = 5000
    cmd = "./%s --stop --port %d" %(CONST_COR_SERVICE_PATH, COR_NODE_PORT)
    RunCommand(cmd)

def start_cor_service():
    port = 5000
    cmd = "./%s --start --port %d" %(CONST_COR_SERVICE_PATH, COR_NODE_PORT)
    RunCommand(cmd)

def getHTNode(key):
    r = s.get('http://%s:%d/getnode?key=%s' %(COR_NODE_IP, COR_NODE_PORT, key))
    if (r.status_code != 200):
        raise ValueError("Request for finding node failed for key %s with error %s" %( key, r.text))
    print r.text
    return json.loads(r.text)["node"]

def putKey(ip, port, key, value):
    r = s.post('http://%s:%d/put?key=%s' %(ip, port, key), data = value)
    if r.status_code != 200:
        raise ValueError("Put failed for key %s with error %s" %( key, r.text))
    print r.text

def getKey(ip, port, key):
    r = s.get('http://%s:%d/get?key=%s' %(ip, port, key))
    print r.text
    if r.status_code != 200:
        raise ValueError("Get failed for key %s with error %s" %( key, r.text))
    return json.loads(r.text)["value"]

def delKey(ip, port, key):
    r = s.delete('http://%s:%d/del?key=%s' %(ip, port, key))
    if r.status_code != 200:
        raise ValueError("Get failed for key %s with error %s" %(key, r.text))
    print r.text

def getAllNodes():
    r = s.get('http://%s:%d/getallnodes' %(COR_NODE_IP, COR_NODE_PORT))
    if r.status_code != 200:
        raise ValueError("request for get all nodes failed with error", r.text)
    print r.text
    return json.loads(r.text)["ring"]

def test1():
    cnt = 0
    while (cnt < 6):
        ring = getAllNodes()
        if len(ring) >=  CONST_NO_OF_HT_NODES:
            break
        cnt+=1
        time.sleep(5)
    if len(ring) == CONST_NO_OF_HT_NODES:
        raise ValueError("Nodes aren't registerd even after 30 seconds")

    # put all keys
    for key, value in random_str_dict.items():
        node = getHTNode(key)
        print "for key %s : communicate with %s"%(key,  node)
        ip, port = node.split(':')[0], int(node.split(':')[1])
        putKey(ip, port, key, value)

    # get all keys and verify values
    for key, value in random_str_dict.items():
        node = getHTNode(key)
        ip, port = node.split(':')[0], int(node.split(':')[1])
        result = getKey(ip, port, key)
        assert(result == value)

    # del all keys
    for key, value in random_str_dict.items():
        node = getHTNode(key)
        ip, port = node.split(':')[0], int(node.split(':')[1])
        delKey(ip, port, key)

    # check all keys again post deletion
    for key in random_str_dict.items():
        node = getHTNode(key)
        ip, port = node.split(':')[0], int(node.split(':')[1])
        try:
            result = getKey(ip, port, key)
        except ValueError:
            continue
        else:
            raise ValueError("Key %s still present post deletion on node %s", key, node)

def test2():
    pass

if __name__ == "__main__":
    stop_ht_services()
    stop_cor_service()
    start_ht_services()
    start_cor_service()
    test1()
