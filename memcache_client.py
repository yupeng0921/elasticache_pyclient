#! /usr/bin/env python

import telnetlib
import re
import inspect
import new
import threading
import time
import logging
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
        self.servers = []
        nodes_str = conf[2].split(' ')
        for node_str in nodes_str:
            node_list = node_str.split('|')
            if len(node_list) != 3:
                raise InvalidConfigurationError(ret)
            self.servers.append(node_list[1] + ':' + node_list[2])

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
    def __init__(self, server, auotdiscovery_timeout=10, autodiscovery_interval=60, client_debug=None, *k, **kw):
        self.server = server
        self.auotdiscovery_timeout = auotdiscovery_timeout
        self.cluster = Cluster(server, auotdiscovery_timeout)
        self.ring = hash_ring.MemcacheRing(self.cluster.servers, *k, **kw)
        self.need_update = False
        self.client_debug = client_debug

        self.lock = threading.Lock()

        if client_debug:
            self.logger = logging.getLogger('memcache_client')
            file_handler = logging.FileHandler(client_debug)
            formatter = logging.Formatter('%(name)-12s %(asctime)s %(funcName)s %(message)s', '%a, %d %b %Y %H:%M:%S',)
            file_handler.setFormatter(formatter)
            self.logger.addHandler(file_handler)
            self.logger.setLevel(logging.DEBUG)
            debug_string = 'self.logger.debug("setservers: " + str(self.cluster.servers))\n'
        else:
            self.logger = None
            debug_string = '\n'

        attrs = dir(hash_ring.MemcacheRing)
        for attr in attrs:
            if inspect.ismethod(getattr(hash_ring.MemcacheRing, attr)) and attr[0] != '_':
                method_str = 'def ' + attr + '(self, *k, **kw):\n' + \
                    '    ret = self.lock.acquire(True)\n' + \
                    '    if not ret:\n' + \
                    '        raise GetLockError(' + attr + ')\n' + \
                    '    if self.need_update:\n' + \
                    '        ' + debug_string + \
                    '        self.ring.set_servers(self.cluster.servers)\n' + \
                    '        self.need_update = False\n' + \
                    '    self.lock.release()\n' + \
                    '    ret = self.ring.' + attr + '(*k, **kw)\n' + \
                    '    return ret'
                self._extends(attr, method_str)

        self.timer = Timer('autodiscovery', autodiscovery_interval, self._update)
        self.timer.setDaemon(True)
        self.timer.start()

    def _extends(self, method_name, method_str):
        #_method = None
        exec method_str + '''\n_method = %s''' % method_name
        self.__dict__[method_name] = new.instancemethod(_method, self, None)

    def _update(self):
        cluster = Cluster(self.server, self.auotdiscovery_timeout)
        if cluster.version != self.cluster.version:
            ret = self.lock.acqurie(True)
            if not ret:
                raise GetLockError('_update')
            if self.logger:
                self.logger.debug("old: " + self.cluster.version + str(self.cluster.servers))
                self.logger.debug("new: " + cluster.version + str(cluster.servers))
            self.cluster = cluster
            self.need_update = True
            self.lock.release()

    def stop_timer(self):
        self.timer.end()

if __name__ == '__main__':
    server = 'mytest.lwgyhw.cfg.usw2.cache.amazonaws.com:11211'
    m = MemcacheClient(server, client_debug='test.log')
    # m.set('xyz', 14)
    print m.get('xyz')
    time.sleep(5)
