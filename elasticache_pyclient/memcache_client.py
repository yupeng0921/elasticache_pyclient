#! /usr/bin/env python

import re
from threading import Lock
from types import MethodType
from telnetlib import Telnet
from distutils.version import StrictVersion
from hash_ring import MemcacheRing
from elasticache_pyclient import elasticache_logger
from elasticache_pyclient.repeat_timer import RepeatTimer

class ElasticacheInvalidTelentReplyError(Exception):
    """
    receive configuration from endpoint is invalide
    """
    def __init__(self, msg):
        self.msg = msg
    def __str__(self):
        return self.msg

class Cluster():
    """
    get the cluster configuration, store version and node list
    """

    def __init__(self, endpoint, timeout):
        host, port = endpoint.split(':')
        elasticache_logger.debug('cluster: %s %s %s %s %s %s',
                                 str(host), str(type(host)),
                                 str(port), str(type(port)),
                                 str(timeout), str(type(timeout)))
        tn = Telnet(host, port)
        tn.write('version\n')
        ret = tn.read_until('\r\n', timeout)
        elasticache_logger.debug('version: %s', ret)
        version_list = ret.split(' ')
        if len(version_list) != 2 or version_list[0] != 'VERSION':
            raise ElasticacheInvalidTelentReplyError(ret)
        version = version_list[1][0:-2]
        if StrictVersion(version) >= StrictVersion('1.4.14'):
            get_cluster = 'config get cluster\n'
        else:
            get_cluster = 'get AmazonElastiCache:cluster\n'
        tn.write(get_cluster)
        ret = tn.read_until('END\r\n',  timeout)
        elasticache_logger.debug('config: %s', ret)
        tn.close()
        p = re.compile(r'\r?\n')
        conf = p.split(ret)
        if len(conf) != 6 or conf[4][0:3] != 'END':
            raise ElasticacheInvalidTelentReplyError(ret)
        self.version = conf[1]
        self.servers = []
        nodes_str = conf[2].split(' ')
        for node_str in nodes_str:
            node_list = node_str.split('|')
            if len(node_list) != 3:
                raise ElasticacheInvalidTelentReplyError(ret)
            self.servers.append(node_list[1] + ':' + node_list[2])

class MemcacheClient():
    """
    Do autodiscovery for elasticache memcache cluster.
    """

    def __init__(self, endpoint, autodiscovery_timeout=10, autodiscovery_interval=60, *args, **kwargs):
        """
        Create a new Client object, and launch a timer for the object.

        @param endpoint: String
        something like: test.lwgyhw.cfg.usw2.cache.amazonaws.com:11211

        @autodiscovery_timeout: Number
        Secondes for socket connection timeout when do autodiscovery

        @autodiscovery_interval: Number
        Seconds interval for check cluster status

        @client_debug: String
        A file name, if set, will write debug message to that file

        All Other parameters will be passed to python-memcached
        """
        self.endpoint = endpoint
        self.autodiscovery_timeout = autodiscovery_timeout
        elasticache_logger.debug('endpoint: %s' % endpoint)
        self.cluster = Cluster(endpoint, autodiscovery_timeout)
        self.ring = MemcacheRing(self.cluster.servers, *args, **kwargs)
        self.need_update = False
        self.lock = Lock()

        self.timer = RepeatTimer('autodiscovery', autodiscovery_interval, self._update)
        self.timer.start()

    def __getattr__(self, key):
        if not hasattr(self.ring, key):
            msg = "'%s' object has no attribute '%s'" % (type(self).__name__, key)
            raise AttributeError(msg)
        ori_func = getattr(self.ring, key)
        def tmp_func(self, *args, **kwargs):
            self.lock.acquire(True)
            if self.need_update:
                self.ring.set_servers(self.cluster.servers)
                self.need_update = False
            self.lock.release()
            return ori_func(*args, **kwargs)
        tmp_func.__name__ = key
        return MethodType(tmp_func, self)

    def _update(self):
        try:
            cluster = Cluster(self.endpoint, self.autodiscovery_timeout)
        except Exception, e:
            elasticache_logger.debug(e)
            return
        if cluster.version != self.cluster.version:
            self.lock.acquire(True)
            self.cluster = cluster
            self.need_update = True
            self.lock.release()

    def stop_timer(self):
        """
        If do not use the Client object anymore, you can call this function to stop the timer associate with
        the object, or the timer will alwasy run.
        """
        self.timer.stop_timer()
        self.timer.join()
