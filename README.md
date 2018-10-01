# do auto discovery for aws elasticache

## introduce
Implement aws elasticache auto discovery, for detail about auto discovery, please reference to:

http://docs.aws.amazon.com/AmazonElastiCache/latest/UserGuide/AutoDiscovery.html

It uses python-memcached implements memcache command, and use hash_ring implements consistent hash, below links have more detail about python-memcached and hash_ring:

https://pypi.python.org/pypi/python-memcached

https://pypi.python.org/pypi/hash_ring/

## install

    pip install elasticache_pyclient

## usage

    >>> from elasticache_pyclient import MemcacheClient
    >>> mc = MemcacheClient('test.lwgyhw.cfg.usw2.cache.amazonaws.com:11211')
    >>> mc.set('foo', 'bar')
    True
    >>> mc.get('foo')
    'bar'

Besides set and get, it supports all the python-memcached methods, to examine all the python-memcached methods, you can run these commands in python interpreter:

    import memcache
    help(memcache.Client)

## stop atuo discovery timer
Every MemcacheClient will start a timer for auto discovery, if do not use MemcacheClient object anymore, please call this funciton to stop the timer, or the timer will run forever

    >>> mc.stop_timer()

## version support
The elasticache_pyclient package is tested on python 2.7, 3.5 and 3.6.
