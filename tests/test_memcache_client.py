# -*- coding: utf-8 -*-

import unittest
import os

from elasticache_pyclient import MemcacheClient


class MemcacheClientTest(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_client(self):
        ecache_url = os.environ['ECACHE_URL']
        size = int(os.environ.get('ECACHE_SIZE', '3'))
        mc = MemcacheClient(ecache_url)
        mc.set('foo', 'bar')
        self.assertEqual(mc.get('foo'), 'bar')
        self.assertEqual(mc.cluster_size(), size)
        mc.stop_timer()
