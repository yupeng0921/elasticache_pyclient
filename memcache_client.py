#! /usr/bin/env python

import telnetlib
import re
import inspect
import new
import hash_ring

class InvalidConfigurationError(Exception):
    """
    receive configuration from endpoint is invalide
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

class MemcacheClient():
    def __init__(self, server, auotdiscovery_timeout=10, autodiscovery_interval=60, *k, **kw):
        self.k = k
        self.kw = kw
        self.cluster = Cluster(server, auotdiscovery_timeout)
        servers = []
        for node in self.cluster.nodes:
            servers.append(node.ip+':'+node.port)
        self.ring = hash_ring.MemcacheRing(servers, *k, **kw)
        attrs = dir(hash_ring.MemcacheRing)
        methods = []
        for attr in attrs:
            if inspect.ismethod(getattr(hash_ring.MemcacheRing, attr)) and attr[0] != '_':
                methods.append(attr)
        for method in methods:
            method_str = 'def ' + method + '(self, *k, **kw):\n' + \
                '    return self.ring.' + method + '(*k, **kw)\n'
            self.extends(method, method_str)

    def extends(self, method_name, method_str):
        #_method = None
        exec method_str + '''\n_method = %s''' % method_name
        self.__dict__[method_name] = new.instancemethod(_method, self, None)

if __name__ == '__main__':
    server = 'mytest.lwgyhw.cfg.usw2.cache.amazonaws.com:11211'
    m = MemcacheClient(server)
    m.set('xyz', 13)
    print m.get('xyz')
