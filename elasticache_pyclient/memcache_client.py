#! /usr/bin/env python

import telnetlib
import re
import inspect
import new
import threading
import time
import logging
import hash_ring
from distutils.version import StrictVersion

class InvalidTelentReplyError(Exception):
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
    def __init__(self, server, timeout, logger=None):
        host, port = server.split(':')
        if logger:
            msg = '%s %s %s %s' % (str(host), str(type(host)), str(port), str(type(port)))
            logger.debug(msg)
            logger.handlers[0].flush()
        tn = telnetlib.Telnet(host, port)
        tn.write('version\n')
        ret = tn.read_until('\r\n', timeout)
        if logger:
            logger.debug('version:\n')
            logger.debug(ret)
            logger.handlers[0].flush()
        version_list = ret.split(' ')
        if len(version_list) != 2 or version_list[0] != 'VERSION':
            raise InvalidTelentReplyError(ret)
        version = version_list[1][0:-2]
        if StrictVersion(version) >= StrictVersion('1.4.14'):
            get_cluster = 'config get cluster\n'
        else:
            get_cluster = 'get AmazonElastiCache:cluster\n'
        tn.write(get_cluster)
        ret = tn.read_until('END\r\n',  timeout)
        if logger:
            logger.debug('config:\n')
            logger.debug(ret)
            logger.handlers[0].flush()
        tn.close()
        p = re.compile(r'\r?\n')
        conf = p.split(ret)
        if len(conf) != 6:
            raise InvalidTelentReplyError(ret)
        if conf[4][0:3] != 'END':
            raise InvalidTelentReplyError(ret)
        self.version = conf[1]
        self.servers = []
        nodes_str = conf[2].split(' ')
        for node_str in nodes_str:
            node_list = node_str.split('|')
            if len(node_list) != 3:
                raise InvalidTelentReplyError(ret)
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
    """
    Do autodiscovery for elasticache memcache cluster.
    """
    def __init__(self, server, autodiscovery_timeout=10, autodiscovery_interval=60, client_debug=None, *k, **kw):
        """
        Create a new Client object, and launch a timer for the object.

        @param server: String
        something like: test.lwgyhw.cfg.usw2.cache.amazonaws.com:11211

        @autodiscovery_timeout: Number
        Secondes for socket connection timeout when do autodiscovery

        @autodiscovery_interval: Number
        Seconds interval for check cluster status

        @client_debug: String
        A file name, if set, will write debug message to that file

        All Other parameters will be passed to python-memcached
        """
        self.server = server
        self.autodiscovery_timeout = autodiscovery_timeout

        if client_debug:
            self.logger = logging.getLogger('memcache_client')
            self.file_handler = logging.FileHandler(client_debug)
            formatter = logging.Formatter('%(name)-12s %(asctime)s %(funcName)s %(message)s', '%a, %d %b %Y %H:%M:%S',)
            self.file_handler.setFormatter(formatter)
            self.logger.addHandler(self.file_handler)
            self.logger.setLevel(logging.DEBUG)
            debug_string = 'self.logger.debug("setservers: " + str(self.cluster.servers))\n'
            flush_string = 'self.logger.handlers[0].flush()\n'
        else:
            self.logger = None
            debug_string = '\n'
            flush_string = '\n'

        self.cluster = Cluster(server, autodiscovery_timeout, self.logger)
        self.ring = hash_ring.MemcacheRing(self.cluster.servers, *k, **kw)
        self.need_update = False
        self.client_debug = client_debug

        self.lock = threading.Lock()
        attrs = dir(hash_ring.MemcacheRing)
        for attr in attrs:
            if inspect.ismethod(getattr(hash_ring.MemcacheRing, attr)) and attr[0] != '_':
                method_str = 'def ' + attr + '(self, *k, **kw):\n' + \
                    '    ret = self.lock.acquire(True)\n' + \
                    '    if not ret:\n' + \
                    '        raise GetLockError(' + attr + ')\n' + \
                    '    if self.need_update:\n' + \
                    '        ' + debug_string + \
                    '        ' + flush_string + \
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
        exec method_str + '''\n_method = %s''' % method_name
        self.__dict__[method_name] = new.instancemethod(_method, self, None)

    def _update(self):
        try:
            cluster = Cluster(self.server, self.autodiscovery_timeout)
        except Exception, e:
            if self.logger:
                self.logger.debug(e)
                self.logger.handlers[0].flush()
            return
        if cluster.version != self.cluster.version:
            ret = self.lock.acquire(True)
            if not ret:
                raise GetLockError('_update')
            if self.logger:
                self.logger.debug("old: " + self.cluster.version + str(self.cluster.servers))
                self.logger.debug("new: " + cluster.version + str(cluster.servers))
                self.logger.handlers[0].flush()
            self.cluster = cluster
            self.need_update = True
            self.lock.release()

    def stop_timer(self):
        """
        If do not use the Client object anymore, you can call this function to stop the timer associate with
        the object, or the timer will alwasy run.
        """
        self.timer.end()

if __name__ == '__main__':
    import unittest
    import time
    import sys
    import socket
    import boto.ec2
    import boto.elasticache
    class MemcacheClientTestCase(unittest.TestCase):
        resources = []
        region = 'us-west-2'
        default_type = 'cache.m1.small'
        default_number = 4
        use_exist_cluster = None
        def __init__(self, new_server, new_name, old_server, old_name, *k, **kw):
            if new_server and old_server and new_name and old_name:
                self.new_name = new_name
                self.new_server = new_server
                self.old_name = old_name
                self.old_server = old_server
                self.conn = boto.elasticache.connect_to_region(self.region)
                self.use_exist_cluster = True
            unittest.TestCase.__init__(self, *k, **kw)
        def setUp(self):
            if self.use_exist_cluster:
                return
            print "lauching resources"
            suffix = time.strftime('%y%m%d%H%M%S', time.localtime())
            self.new_name = 'new' + suffix
            self.old_name = 'old' + suffix
            new_version = '1.4.14'
            old_version = '1.4.5'
            port = 11211

            c1 = boto.ec2.connect_to_region(self.region)
            self.sg = c1.create_security_group('sg_'+suffix, 'for elasticache test')
            self.resources.append('sg')
            self.sg.authorize('tcp', str(port), str(port), '0.0.0.0/0')
            print 'sg created'

            self.conn = boto.elasticache.connect_to_region(self.region)
            self.conn.create_cache_cluster(self.new_name, self.default_number, self.default_type, 'memcached', \
                                               engine_version=new_version, security_group_ids=[self.sg.id], port=port)

            self.conn.create_cache_cluster(self.old_name, self.default_number, self.default_type, 'memcached', \
                                               engine_version=old_version, security_group_ids=[self.sg.id], port=port)

            print 'waiting for cluster available'

            while True:
                ret = self.conn.describe_cache_clusters(self.new_name)
                status = ret['DescribeCacheClustersResponse']['DescribeCacheClustersResult']['CacheClusters'][0]['CacheClusterStatus']
                if status == 'available':
                    break
                time.sleep(1)
            self.resources.append('new_memcached')
            endpoint = ret['DescribeCacheClustersResponse']['DescribeCacheClustersResult']['CacheClusters'][0]['ConfigurationEndpoint']
            self.new_server = str(endpoint['Address'] + ':' + str(endpoint['Port']))
            print self.new_server

            while True:
                ret = self.conn.describe_cache_clusters(self.old_name)
                status = ret['DescribeCacheClustersResponse']['DescribeCacheClustersResult']['CacheClusters'][0]['CacheClusterStatus']
                if status == 'available':
                    break
                time.sleep(1)
            self.resources.append('old_memcached')
            endpoint = ret['DescribeCacheClustersResponse']['DescribeCacheClustersResult']['CacheClusters'][0]['ConfigurationEndpoint']
            self.old_server = str(endpoint['Address'] + ':' + str(endpoint['Port']))
            print self.old_server

        def tearDown(self):
            if self.use_exist_cluster:
                return
            print 'clean resoures'
            if 'old_memcached' in self.resources:
                self.conn.delete_cache_cluster(self.old_name)
                print 'deleted old memcached'
            if 'new_memcached' in self.resources:
                self.conn.delete_cache_cluster(self.new_name)
                print 'deleted new memcached'
            if 'sg' in self.resources:
                while True:
                    try:
                        self.sg.delete()
                    except boto.exception.EC2ResponseError, e:
                        time.sleep(10)
                        continue
                    print 'deleted sg'
                    break
        def run_test(self, server, name, client_debug):
            print 'start testing'
            print server
            print name
            print client_debug
            retry = 300
            while retry >= 0:
                try:
                    m = MemcacheClient(server, client_debug=client_debug)
                except socket.gaierror, e:
                    time.sleep(10)
                    retry -= 1
                    continue
                break
            self.assertTrue(retry >= 0)
            print 'got MemcacheClient'

            total_count = 1000
            for i in range(0, total_count):
                m.set(str(i), i)
            for i in range(0, total_count):
                ret = m.get(str(i))
                self.assertEquals(ret, i)

            print 'increasing nodes'
            self.conn.modify_cache_cluster(name, num_cache_nodes=self.default_number+1, apply_immediately=True)
            time.sleep(10)
            while True:
                ret = self.conn.describe_cache_clusters(name)
                status = ret['DescribeCacheClustersResponse']['DescribeCacheClustersResult']['CacheClusters'][0]['CacheClusterStatus']
                if status == 'available':
                    break
                time.sleep(1)
            print 'increased nodes'

            correct_count = 0
            for i in range(0, total_count):
                ret = m.get(str(i))
                if ret:
                    self.assertEquals(ret, i)
                    correct_count += 1

            print 'total: %d' % total_count
            print 'correct: %d' % correct_count
            self.assertTrue(correct_count > total_count / 2)

            time.sleep(90)

            correct_count = 0
            for i in range(0, total_count):
                ret = m.get(str(i))
                if ret:
                    self.assertEquals(ret, i)
                    correct_count += 1

            print 'total: %d' % total_count
            print 'correct: %d' % correct_count
            self.assertTrue(correct_count > total_count / 2)

            for i in range(0, total_count):
                m.set(str(i+total_count), i+total_count)
            for i in range(0, total_count):
                ret = m.get(str(i+total_count))
                self.assertEquals(ret, i+total_count)

            print 'decreasing nodes'
            ret = self.conn.describe_cache_clusters(cache_cluster_id=name, show_cache_node_info=True)
            node_id = ret['DescribeCacheClustersResponse']['DescribeCacheClustersResult']['CacheClusters'][0]['CacheNodes'][0]['CacheNodeId']
            self.conn.modify_cache_cluster(name, num_cache_nodes=self.default_number, cache_node_ids_to_remove=[node_id], apply_immediately=True)
            time.sleep(10)

            correct_count = 0
            for i in range(0, total_count):
                ret = m.get(str(i+total_count))
                if ret:
                    self.assertEquals(ret, i+total_count)
                    correct_count += 1
            print 'total: %d' % total_count
            print 'correct: %d' % correct_count

            while True:
                ret = self.conn.describe_cache_clusters(name)
                status = ret['DescribeCacheClustersResponse']['DescribeCacheClustersResult']['CacheClusters'][0]['CacheClusterStatus']
                if status == 'available':
                    break
                time.sleep(1)
            print 'decreased nodes'

            time.sleep(90)

            correct_count = 0
            for i in range(0, total_count):
                ret = m.get(str(i+total_count))
                if ret:
                    self.assertEquals(ret, i+total_count)
                    correct_count += 1

            print 'total: %d' % total_count
            print 'correct: %d' % correct_count
            self.assertTrue(correct_count > total_count / 2)

            m.flush_all()

            print 'completed'


        def run_all_test(self):
            self.run_test(self.new_server, self.new_name, None)
            self.run_test(self.old_server, self.old_name, None)
            self.run_test(self.new_server, self.new_name, '/tmp/new.log')
            self.run_test(self.old_server, self.old_name, '/tmp/old.log')

    if (len(sys.argv) == 5):
        new_server = sys.argv[1]
        new_name = sys.argv[2]
        old_server = sys.argv[3]
        old_name = sys.argv[4]
    else:
        new_server = None
        new_name = None
        old_server = None
        old_name = None
    suite = unittest.TestSuite()
    suite.addTest(MemcacheClientTestCase(new_server, new_name, old_server, old_name, 'run_all_test'))
    runner = unittest.TextTestRunner()
    runner.run(suite)
