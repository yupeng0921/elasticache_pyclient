#! /usr/bin/env python

import time
import unittest
from elasticache_pyclient.repeat_timer import RepeatTimer

class TestRepeatTimer(unittest.TestCase):
    def setUp(self):
        self.count = 0
        def test_func():
            self.count += 1
        self.timer = RepeatTimer('test_timer', 1, test_func)
    def test_run_timer(self):
        self.timer.start()
        time.sleep(3)
        assert self.count >= 2
        self.timer.stop_timer()
        self.timer.join()
