# do auto discovery for aws elasticache

## introduce
Implement aws elasticache auto discovery, for detail about auto discovery, please reference to:

http://docs.aws.amazon.com/AmazonElastiCache/latest/UserGuide/AutoDiscovery.html

It uses python-memcached implements memcache command, and use hash_ring implements consistent hash, below links have more detail about python-memcached and hash_ring:

https://pypi.python.org/pypi/python-memcached

https://pypi.python.org/pypi/hash_ring/

## install
First, install python_memcached and hash_ring, elasticache_pyclient depend on them:

    pip install python_memcached hash_ring

Then, install elasticache_pyclient:

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

## version support
The elasticache_pyclient package is tested on python 2.6 and 2.7. If anyone need python 3.0 support, please create an issue for me on the github project.
