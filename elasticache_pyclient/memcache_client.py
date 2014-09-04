#! /usr/bin/env python

from elasticache_pyclient import elasticache_logger
from elasticache_pyclient.repeat_timer import RepeatTimer

class MemcacheClient():
    def __init__(self, server, autodiscovery_timeout=10, autodiscovery_interval=60, *args, **kwargs):
        elasticache_logger.info('server: %s' % server)
