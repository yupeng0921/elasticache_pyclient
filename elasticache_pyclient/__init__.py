#! /usr/bin/env python

__version__ = "1.0.1"
try:
    from memcache_client import MemcacheClient
except ImportError, e:
    pass
