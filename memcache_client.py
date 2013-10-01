#! /usr/bin/env python

import telnetlib
import re
import inspect
import new
import threading
import time
import hash_ring

class InvalidConfigurationError(Exception):
    """
    receive configuration from endpoint is invalide
    """
    def __init__(self, msg):
        self.msg = msg
    def __str__(self):
        return self.msg

class GetLockError(Exception):
    """
    get lock failed, should not happend
    """
    def __init__(self, msg):
        self.msg = msg
    def __str__(self):
        return self.msg

class Node():
    def __init__(self, dns, ip, port):
        self.dns = dns
        self.ip = ip
        self.port = port

class Cluster():
    """
    get the cluster config, store version and node list
    """
    def __init__(self, server, timeout):
        host, port = server.split(':')
        tn = telnetlib.Telnet(host, port)
        tn.write('config get cluster\n')
        ret = tn.read_until('END\r\n',  3)
        tn.close()
        p = re.compile(r'\r?\n')
        conf = p.split(ret)
        if len(conf) != 6:
            raise InvalidConfigurationError(ret)
        if not (conf[0][0:14] == 'CONFIG cluster' and conf[4][0:3] == 'END'):
            raise InvalidConfigurationError(ret)
        self.version = conf[1]
        self.nodes = []
        nodes_str = conf[2].split(' ')
        for node_str in nodes_str:
            node_list = node_str.split('|')
            if len(node_list) != 3:
                raise InvalidConfigurationError(ret)
            node = Node(node_list[0], node_list[1], node_list[2])
            self.nodes.append(node)

class Timer(threading.Thread):
    def __init__(self, threadname, interval, func):
        self.__interval = interval
        self.__func = func
        self.__running = True
        threading.Thread.__init__(self,name=threadname)
    def run(self):
        while self.__running:
            time.sleep(self.__interval)
            self.__func()
    def end(self):
        self.__running = False

class MemcacheClient():
    def __init__(self, server, auotdiscovery_timeout=10, autodiscovery_interval=60, *k, **kw):
        self.k = k
        self.kw = kw
        self.auotdiscovery_timeout = auotdiscovery_timeout
        self.cluster = Cluster(server, auotdiscovery_timeout)
        servers = []
        for node in self.cluster.nodes:
            servers.append(node.ip+':'+node.port)
        self.ring = hash_ring.MemcacheRing(servers, *k, **kw)
        self.lock = threading.Lock()
        attrs = dir(hash_ring.MemcacheRing)
        for attr in attrs:
            if inspect.ismethod(getattr(hash_ring.MemcacheRing, attr)) and attr[0] != '_':
                method_str = 'def ' + attr + '(self, *k, **kw):\n' + \
                    '    ret = self.lock.acquire(True)\n' + \
                    '    if not ret:\n' + \
                    '        raise GetLockError(' + attr + ')\n' + \
                    '    ret = self.ring.' + attr + '(*k, **kw)\n' + \
                    '    self.lock.release()\n' + \
                    '    return ret'
                self._extends(attr, method_str)

        self.timer = Timer('testing', 1, self._update)
        self.timer.start()
    def _extends(self, method_name, method_str):
        #_method = None
        exec method_str + '''\n_method = %s''' % method_name
        self.__dict__[method_name] = new.instancemethod(_method, self, None)

    def _update(self):
        print self.auotdiscovery_timeout

    def stop_timer(self):
        self.timer.end()

if __name__ == '__main__':
    server = 'mytest.lwgyhw.cfg.usw2.cache.amazonaws.com:11211'
    m = MemcacheClient(server)
    # m.set('xyz', 14)
    print m.get('xyz')
    time.sleep(5)
    m.stop_timer()
