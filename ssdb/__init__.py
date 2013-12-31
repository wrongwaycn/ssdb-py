#coding=utf-8

#from ssdb.client import Redis, StrictRedis
from ssdb.connection import (BlockingConnectionPool, ConnectionPool, Connection)
#from ssdb.utils import from_url
from ssdb.exceptions import (AuthenticationError, ConnectionError,
                             BusyLoadingError, DataError, InvalidResponse,
                             PubSubError, SSDBError, ResponseError, WatchError)


__version__ = '0.0.1'
VERSION = tuple(map(int, __version__.split('.')))

__all__ = ['SSDB', 'ConnectionPool', 'BlockingConnectionPool', 'Connection',
           'SSDBError', 'ConnectionError', 'ResponseError', 'AuthenticationError',
           'InvalidResponse', 'DataError', 'PubSubError', 'WatchError',
           'BusyLoadingError']

