#! /usr/bin/env python

import unittest
from mock import Mock, patch
from elasticache_pyclient import MemcacheClient

class TestMemcacheClient(unittest.TestCase):
    def setUp(self):
        pass
    def tearDown(self):
        pass
    @patch('elasticache_pyclient.memcache_client.Telnet')
    def test_1_4_14(self, telnet_mock):
        tn_mock = Mock()
        telnet_mock.return_value = tn_mock
        read_until_mock = Mock()
        read_until_ret_list = ['VERSION 1.4.14\r\n', 'CONFIG cluster 0 191\r\n1\ntest1.rtstcw.0001.apne1.cache.amazonaws.com|172.31.12.60|11211 test1.rtstcw.0002.apne1.cache.amazonaws.com|172.31.3.240|11211 test1.rtstcw.0003.apne1.cache.amazonaws.com|172.31.10.26|11211\n\r\nEND\r\n']
        def read_until_ret_func(*args, **kwargs):
            return read_until_ret_list.pop(0)
        read_until_mock.side_effect = read_until_ret_func
        tn_mock.read_until = read_until_mock
        endpoint = 'test.lwgyhw.cfg.usw2.cache.amazonaws.com:11211'
        mc = MemcacheClient(endpoint)
        mc.set('foo', 'bar')
        mc.stop_timer()
    def test_1_4_5(self):
        pass
