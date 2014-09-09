#! /usr/bin/env python

import logging

__all__ = ['MemcacheClient', 'elasticache_logger']

elasticache_logger = logging.getLogger('elasticache_logger')
class NullHandler(logging.Handler):
    def emit(self, record):
        pass
elasticache_logger.addHandler(NullHandler())

try:
    from memcache_client import MemcacheClient
except ImportError, e:
    pass
