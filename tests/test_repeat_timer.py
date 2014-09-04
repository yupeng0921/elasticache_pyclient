#! /usr/bin/env python

import time
import unittest
from mock import Mock
from elasticache_pyclient.repeat_timer import RepeatTimer

class TestRepeatTimer(unittest.TestCase):
    def setUp(self):
        self.mock_func = Mock()
        self.timer = RepeatTimer('test_timer', 1, self.mock_func)
    def test_run_timer(self):
        self.timer.start()
        time.sleep(3)
        self.assertTrue(self.mock_func.call_count >= 2)
    def tearDown(self):
        self.timer.stop_timer()
        self.timer.join()
