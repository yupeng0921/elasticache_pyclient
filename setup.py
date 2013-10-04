from setuptools import setup, find_packages

PACKAGE = "elasticache_pyclient"
NAME = "elasticache_pyclient"
KEYWORDS = ("aws", "ealsticache")
VERSION = __import__(PACKAGE).__version__
DESCRIPTION = "pythone client for elasticache auto discovery"
LICENSE = 'LGPL'
URL = "https://github.com/yupeng820921/elasticache_pyclient"
AUTHOR = "yupeng"
AUTHOR_EMAIL = "yupeng0921@gmail.com"

setup(
    name = NAME,
    version = VERSION,
    keywords = KEYWORDS,
    description = DESCRIPTION,
    license = LICENSE,

    url = URL,
    author = AUTHOR,
    author_email = AUTHOR_EMAIL,

    packages = find_packages(),
    include_package_data = True,
    platforms = 'any',
    install_requires = ['python-memcached', 'hash_ring'],
    )
