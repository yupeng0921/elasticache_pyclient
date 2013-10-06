# do auto discovery for aws elasticache

## introduce
Implement aws elasticache auto discovery, for detail about auto discovery, please reference to:

http://docs.aws.amazon.com/AmazonElastiCache/latest/UserGuide/AutoDiscovery.html

It uses python-memcached implements memcache command, and use hash_ring implements consistent hash, below links have more detail about python-memcached and hash_ring:

https://pypi.python.org/pypi/python-memcached

https://pypi.python.org/pypi/hash_ring/

## install

    pip install elasticache_pyclient

or

    python-pip install elasticache_pyclient

The python-memcached and hash_ring packages will also be installed automatically

## usage

    >>> from elasticache_pyclient import MemcacheClient
    >>> mc = MemcacheClient('test.lwgyhw.cfg.usw2.cache.amazonaws.com:11211')
    >>> mc.set('foo', 'bar')
    True
    >>> mc.get('foo')
    'bar'

It support all the python-memcached methods, to examine all the python-memcached methods, you can run these commands in python interpreter:

    import memcache
    help(memcache.Client)
