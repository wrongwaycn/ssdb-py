#coding=utf-8
from __future__ import with_statement
from itertools import chain, starmap, izip_longest
import datetime
import sys
import warnings
import time as mod_time
from ssdb._compat import (b, basestring, bytes, imap, iteritems, iterkeys,
                          itervalues, izip, long, nativestr, urlparse, unicode,
                          OrderedDict)
from ssdb.connection import ConnectionPool
from ssdb.batch import BaseBatch
from ssdb.utils import (
    get_integer,
    get_integer_or_emptystring,
    get_nonnegative_integer,
    get_positive_integer,
    get_negative_integer,
    get_boolean
    )
from ssdb.exceptions import (
    RES_STATUS_MSG,
    RES_STATUS,
    ConnectionError,
    DataError,
    SSDBError,
    ResponseError,
    WatchError,
    NoScriptError,
    ExecAbortError,
    )

SYM_EMPTY = b('')

def list_or_arg(keys, args):
    #returns a single list combining keys and args
    try:
        iter(keys)
        # a string or bytes instance can be iterated, but indicates keys wasn't
        # passed as a list
        if isinstance(keys, (basestring, bytes)):
            key = [keys]
    except TypeError:
        keys = [keys]
    if args:
        keys.extend(args)
    return keys

def timestamp_to_datetime(response):
    """
    Converts a unix timestamp to a Python datetime object.
    """
    if not response:
        return None
    try:
        response = int(response)
    except ValueError:
        return None
    return datetime.datetime.fromtimestamp(response)

def string_keys_to_dict(key_string, callback):
    return dict.fromkeys(key_string.split(), callback)

def list_to_dict(lst):
    return dict(izip_longest(*[iter(lst)] * 2, fillvalue=None))

def list_to_ordereddict(lst):
    dst = OrderedDict()
    for k,v in izip_longest(*[iter(lst)] * 2, fillvalue=None):
        dst[k] = v
    return dst

def list_to_int_dict(lst):
    return {k:int(v) for k,v in list_to_dict(lst).items()}

def list_to_int_ordereddict(lst):
    dst = OrderedDict()
    for k,v in list_to_ordereddict(lst).items():
        dst[k] = int(v)
    return dst

def dict_to_list(dct):
    lst = []
    for key, value in dct.iteritems():
        lst.append(key)
        lst.append(value)
    return lst

def dict_merge(*dicts):
    merged = {}
    [merged.update(d) for d in dicts]
    return merged

def parse_debug_object(response):
    """
    Parse the results of ssdb's DEBUG OBJECT command into a Python dict
    """
    # The 'type' of the object is the first item in the response, but isn't
    # prefixed with
    response = nativestr(response)
    response = 'type:' + response
    response = dict([kv.split(':') for kv in response.split()])

    # parse some expected int values from the string response note: this cmd
    # isn't spec'd so these may not appear in all ssdb versions
    int_fields = ('refcount', 'serializedlength', 'lru', 'lru_seconds_idle')
    for field in int_fields:
        if field in response:
            response[field] = int(response[field])
    return response

def parse_object(response, infotype):
    """
    Parse the results of an OBJECT command
    """
    if infotype in ('idletime', 'refcount'):
        return int(response)
    return response


class StrictSSDB(object):
    """
    Implementation of the SSDB protocol.

    This abstract class provides a Python interface to all SSDB commands and an
    implementation of the SSDB protocol.
    
    Connection derives from this, implementing how the commands are sent and
    received to the SSDB server.
    """

    RESPONSE_CALLBACKS = dict_merge(
        string_keys_to_dict(
            'set setnx del exists expire '
            'setbit getbit '
            'hset hdel hexists '
            'zset zdel zexists',
            lambda r: bool(int(r[0]))
        ),
        string_keys_to_dict(
            'get hget getset substr '
            'qfront qback qget',
            lambda r: r[0]
        ),
        string_keys_to_dict(
            'incr decr multi_set multi_del ttl countbit strlen '
            'hincr hdecr hsize hclear multi_hset multi_hdel '
            'zincr zdecr zsize zclear multi_zset multi_zdel zget zrank zrrank '
            'zcount zsum zavg zremrangebyrank zremrangebyscore '
            'qsize qclear qpush_back qpush_front qtrim_back qtrim_front',
            lambda r: int(r[0])
        ),
        string_keys_to_dict(
            'zavg',
            lambda r: float(r[0])
        ),        
        string_keys_to_dict(
            'multi_get multi_hget hgetall '
            'multi_zget zscan zrscan',            
            list_to_dict
        ),
        string_keys_to_dict(
            'scan rscan hscan hrscan',
            list_to_ordereddict
        ),
        string_keys_to_dict(
            'zscan zrscan zrange zrrange',
            list_to_int_ordereddict
        ),        
        string_keys_to_dict(
            'multi_zget',
            list_to_int_dict
        ),        
        string_keys_to_dict(
            'keys hkeys hlist hrlist zkeys zlist zrlist '
            'qlist qrlist qrange qslice qpop_back qpop_front',
            lambda r: r
        ),        
        {
            'qset': lambda r: True,
        }
    )

    def __init__(self, host='localhost', port=8888, socket_timeout=None,
                 connection_pool=None, charset='utf-8', errors='strict',
                 decode_responses=False):
        if not connection_pool:
            kwargs = {
                'host': host,
                'port': port,
                'socket_timeout': socket_timeout,
                'encoding': charset,
                'encoding_errors': errors,
                'decode_responses': decode_responses,
            }
            connection_pool = ConnectionPool(**kwargs)
        self.connection_pool = connection_pool
        self.response_callbacks = self.__class__.RESPONSE_CALLBACKS.copy()

    def __repr__(self):
        return "%s<%s>" % (type(self).__name__, repr(self.connection_pool))

    def set_response_callback(self, command, callback):
        """
        Set a custom Response Callback
        """
        self.response_callbacks[command] = callback    

    #### COMMAND EXECUTION AND PROTOCOL PARSING ####
    def execute_command(self, *args, **options):
        """
        Execute a command and return a parsed response.
        """
        pool = self.connection_pool
        command_name = args[0]
        connection = pool.get_connection(command_name, **options)
        try:
            connection.send_command(*args)
            return self.parse_response(connection, command_name, **options)
        except ConnectionError:
            connection.disconnect()
            connection.send_command(*args)
            return self.parse_response(connection, command_name, **options)
        finally:
            pool.release(connection)

    def parse_response(self, connection, command_name, **options):
        """
        Parses a response from the ssdb server
        """
        response = connection.read_response()
        if command_name in self.response_callbacks and len(response):
            status = nativestr(response[0])
            if status == RES_STATUS.OK:
                return self.response_callbacks[command_name](response[1:],
                                                             **options)
            elif status == RES_STATUS.NOT_FOUND:
                return None
            else:
                raise DataError(RES_STATUS_MSG[status]+':'.join(response))
                #raise DataError('Not Found')
        return response

    #### KEY/VALUES OPERATION ####
    def get(self, name):
        """
        Return the value at key ``name``, or ``None`` if the key doesn't exist
        
        Like **Redis.GET**

        
        :param string name: the key name
        :return: the value at key ``name``, or ``None`` if the key doesn't exist        
        :rtype: string

        >>> ssdb.get("set_abc")
        'abc'
        >>> ssdb.get("set_a")
        'a'
        >>> ssdb.get("set_b")
        'b'
        >>> ssdb.get("not_exists_abc")
        >>> 
        """        
        return self.execute_command('get', name)
    
    def set(self, name, value):
        """
        Set the value at key ``name`` to ``value`` .
        
        Like **Redis.SET**

        :param string name: the key name
        :param string value: a string or an object can be converted to string
        :return: ``True`` on success, ``False`` if not
        :rtype: bool

        >>> ssdb.set("set_cde", 'cde')
        True
        >>> ssdb.set("set_cde", 'test')
        True        
        >>> ssdb.set("hundred", 100)
        True
        """
        return self.execute_command('set', name, value)    
    add = set

    def setnx(self, name, value):
        """
        Set the value at key ``name`` to ``value`` if and only if the key
        doesn't exist.
        
        Like **Redis.SETNX**

        :param string name: the key name
        :param string value: a string or an object can be converted to string
        :return: ``True`` on success, ``False`` if not
        :rtype: bool

        >>> ssdb.setnx("setnx_test", 'abc')
        True
        >>> ssdb.setnx("setnx_test", 'cde')
        False        
        """
        return self.execute_command('setnx', name, value)

    def getset(self, name, value):
        """
        Set the value at key ``name`` to ``value`` if key doesn't exist
        Return the value at key ``name`` atomically.
        
        Like **Redis.GETSET**

        :param string name: the key name
        :param string value: a string or an object can be converted to string
        :return: ``True`` on success, ``False`` if not
        :rtype: bool

        >>> ssdb.getset("getset_a", 'abc')
        None
        >>> ssdb.getset("getset_a", 'def')
        'abc'
        >>> ssdb.getset("getset_a", 'ABC')
        'def'
        >>> ssdb.getset("getset_a", 123)
        'ABC'
        """
        return self.execute_command('getset', name, value)    

    def delete(self, name):
        """
        Delete the key specified by ``name`` .

        Like **Redis.DELETE**

        .. note:: ``Delete`` **can't delete** the `Hash`_ or `Zsets`_, use
           `hclear`_ for `Hash`_ and `zclear`_ for `Zsets`_ 
           
        :param string name: the key name
        :return: ``True`` on deleting successfully, or ``False`` if the key doesn't
         exist or failure
        :rtype: bool

        >>> ssdb.delete('set_abc')
        True
        >>> ssdb.delete('set_a')
        True        
        >>> ssdb.delete('set_abc')
        False
        >>> ssdb.delete('not_exist')
        False
        """
        return self.execute_command('del', name)
    remove = delete

    def expire(self, name, ttl):
        """
        Set an expire flag on key ``name`` for ``ttl`` seconds. ``ttl``
        can be represented by an integer or a Python timedelta object.        

        Like **Redis.EXPIRE**

        .. note:: ``Expire`` **only expire** the `Key/Value`_ .
           
        :param string name: the key name
        :param int ttl: number of seconds to live
        :return: ``True`` on success, or ``False`` if the key doesn't
         exist or failure
        :rtype: bool

        >>> ssdb.expire('set_abc', 6)
        True
        >>> ssdb.expire('not_exist')
        False
        """
        if isinstance(time, datetime.timedelta):
            time = time.seconds + time.days * 24 * 3600        
        return self.execute_command('expire', name, ttl)

    def ttl(self, name):
        """
        Returns the number of seconds until the key ``name`` will expire.

        Like **Redis.TTL**

        .. note:: ``ttl`` **can only** be used to the `Key/Value`_ .
           
        :param string name: the key name
        :return: the number of seconds, or ``-1`` if the key doesn't
         exist or have no ttl
        :rtype: int

        >>> ssdb.ttl('set_abc')
        6
        >>> ssdb.ttl('not_exist')
        -1
        """
        if isinstance(time, datetime.timedelta):
            time = time.seconds + time.days * 24 * 3600        
        return self.execute_command('expire', name, ttl)    

    def exists(self, name):
        """
        Return a boolean indicating whether key ``name`` exists.

        Like **Redis.EXISTS**

        .. note:: ``exists`` **can't indicate** whether any `Hash`_, `Zsets`_
         or `Queue`_ exists, use `hash_exists`_ for `Hash`_ , `zset_exists`_
         for `Zsets`_ and `queue_exists`_ for `Queue`_ .
        

        :param string name: the key name
        :return: ``True`` if the key exists, ``False`` if not
        :rtype: bool

        >>> ssdb.exists('set_abc')
        True
        >>> ssdb.exists('set_a')
        True
        >>> ssdb.exists('not_exist')
        False        
        """        
        return self.execute_command('exists', name)

    def incr(self, name, amount=1):
        """
        Increase the value at key ``name`` by ``amount``. If no key exists, the value
        will be initialized as ``amount`` .

        Like **Redis.INCR**

        :param string name: the key name
        :param int amount: increments
        :return: the integer value at key ``name``
        :rtype: int

        >>> ssdb.incr('set_count', 3)
        13
        >>> ssdb.incr('set_count', 1)
        14
        >>> ssdb.incr('set_count', -2)
        12
        >>> ssdb.incr('temp_count', 42)
        42        
        """                
        amount = get_integer('amount', amount)
        return self.execute_command('incr', name, amount)

    def decr(self, name, amount=1):
        """
        Decrease the value at key ``name`` by ``amount``. If no key exists, the value
        will be initialized as 0 - ``amount`` .

        Like **Redis.DECR**

        :param string name: the key name
        :param int amount: decrements
        :return: the integer value at key ``name``
        :rtype: int

        >>> ssdb.decr('set_count', 3)
        7
        >>> ssdb.decr('set_count', 1)
        6
        >>> ssdb.decr('temp_count', 42)
        -42                
        """                        
        amount = get_positive_integer('amount', amount)
        return self.execute_command('decr', name, amount)

    def getbit(self, name, offset):
        """
        Returns a boolean indicating the value of ``offset`` in ``name``

        Like **Redis.GETBIT**

        :param string name: the key name
        :param int offset: the bit position
        :param bool val: the bit value
        :return: the bit at the ``offset`` , ``False`` if key doesn't exist or
         offset exceeds the string length.
        :rtype: bool
        
        >>> ssdb.set('bit_test', 1)
        True
        >>> ssdb.getbit('bit_test', 0)
        True
        >>> ssdb.getbit('bit_test', 1)
        False
        """                        
        offset = get_positive_integer('offset', offset)
        return self.execute_command('getbit', name, offset)

    def setbit(self, name, offset, val):
        """
        Flag the ``offset`` in ``name`` as ``value``. Returns a boolean
        indicating the previous value of ``offset``.        

        Like **Redis.SETBIT**        

        :param string name: the key name
        :param int offset: the bit position
        :param bool val: the bit value
        :return: the previous bit (False or True) at the ``offset``
        :rtype: bool
        
        >>> ssdb.set('bit_test', 1)
        True
        >>> ssdb.setbit('bit_test', 1, 1)
        False
        >>> ssdb.get('bit_test')
        3
        >>> ssdb.setbit('bit_test', 2, 1)
        False
        >>> ssdb.get('bit_test')
        7        
        """                        
        val = int(get_boolean('val', val))
        offset = get_positive_integer('offset', offset)
        return self.execute_command('setbit', name, offset, val)

    def countbit(self, name, start=None, size=None):
        """
        Returns the count of set bits in the value of ``key``.  Optional
        ``start`` and ``size`` paramaters indicate which bytes to consider.

        Similiar with **Redis.BITCOUNT**

        :param string name: the key name
        :param int start: Optional, if start is negative, count from start'th
         character from the end of string.
        :param int size: Optional, if size is negative, then that many
         characters will be omitted from the end of string.
        :return: the count of the bit 1
        :rtype: int
        
        >>> ssdb.set('bit_test', 1)
        True
        >>> ssdb.countbit('bit_test')
        3
        >>> ssdb.set('bit_test','1234567890')
        True
        >>> ssdb.countbit('bit_test', 0, 1)
        3
        >>> ssdb.countbit('bit_test', 3, -3)
        16
        """
        if start is not None and size is not None:
            start = get_integer('start', start)
            size = get_integer('size', size)
            return self.execute_command('countbit', name, start, size)
        elif start is not None:
            start = get_integer('start', start)            
            return self.execute_command('countbit', name, start)
        return self.execute_command('countbit', name)
    bitcount = countbit

    def substr(self, name, start=None, size=None):
        """
        Return a substring of the string at key ``name``. ``start`` and ``size``
        are 0-based integers specifying the portion of the string to return.

        Like **Redis.SUBSTR**

        :param string name: the key name
        :param int start: Optional, the offset of first byte returned. If start
         is negative, the returned string will start at the start'th character
         from the end of string.
        :param int size: Optional, number of bytes returned. If size is
         negative, then that many characters will be omitted from the end of string.
        :return: The extracted part of the string.
        :rtype: string
        
        >>> ssdb.set('str_test', 'abc12345678')
        True
        >>> ssdb.substr('str_test', 2, 4)
        'c123'
        >>> ssdb.substr('str_test', -2, 2)
        '78'
        >>> ssdb.substr('str_test', 1, -1)
        'bc1234567'
        """
        if start is not None and size is not None:
            start = get_integer('start', start)
            size = get_integer('size', size)
            return self.execute_command('substr', name, start, size)
        elif start is not None:
            start = get_integer('start', start)            
            return self.execute_command('substr', name, start)
        return self.execute_command('substr', name)

    def strlen(self, name):
        """
        Return the number of bytes stored in the value of ``name``

        Like **Redis.STRLEN**        

        :param string name: the key name
        :return: The number of bytes of the string, if key not exists, returns 0.
        :rtype: int
        
        >>> ssdb.set('str_test', 'abc12345678')
        True
        >>> ssdb.strlen('str_test')
        11
        """
        return self.execute_command('strlen', name)

    def multi_set(self, **kvs):
        """
        Set key/value based on a mapping dictionary as kwargs.

        Like **Redis.MSET**

        :param dict kvs: a key/value mapping dict
        :return: the number of successful operation
        :rtype: int

        >>> ssdb.multi_set(set_a='a', set_b='b', set_c='c', set_d='d')
        4
        >>> ssdb.multi_set(set_abc='abc',set_count=10)
        2
        """        
        return self.execute_command('multi_set', *dict_to_list(kvs))
    mset = multi_set

    def multi_get(self, *names):
        """
        Return a dictionary mapping key/value by ``names``

        Like **Redis.MGET**        

        :param list names: a list of keys
        :return: a dict mapping key/value
        :rtype: dict

        >>> ssdb.multi_get('a', 'b', 'c', 'd')
        {'a': 'a', 'c': 'c', 'b': 'b', 'd': 'd'}
        >>> ssdb.multi_get('set_abc','set_count')
        {'set_abc': 'set_abc', 'set_count': '10'}
        """                
        return self.execute_command('multi_get', *names)
    mget = multi_get

    def multi_del(self, *names):
        """
        Delete one or more keys specified by ``names``

        Like **Redis.DELETE**        

        :param list names: a list of keys
        :return: the number of successful deletion
        :rtype: int

        >>> ssdb.multi_del('a', 'b', 'c', 'd')
        4
        >>> ssdb.multi_del('set_abc','set_count')
        2
        """
        return self.execute_command('multi_del', *names)
    mdel = multi_del

    def keys(self, name_start, name_end, limit=10):
        """
        Return a list of the top ``limit`` keys between ``name_start`` and
        ``name_end``

        Similiar with **Redis.KEYS**        

        .. note:: The range is (``name_start``, ``name_end``]. ``name_start``
           isn't in the range, but ``name_end`` is.

        :param string name_start: The lower bound(not included) of keys to be
         returned, empty string ``''`` means -inf
        :param string name_end: The upper bound(included) of keys to be
         returned, empty string ``''`` means +inf
        :param int limit: number of elements will be returned.
        :return: a list of keys
        :rtype: list
        
        >>> ssdb.keys('set_x1', 'set_x3', 10)
        ['set_x2', 'set_x3']
        >>> ssdb.keys('set_x ', 'set_xx', 3)
        ['set_x1', 'set_x2', 'set_x3']
        >>> ssdb.keys('set_x ', '', 3)
        ['set_x1', 'set_x2', 'set_x3', 'set_x4']
        >>> ssdb.keys('set_zzzzz ', '', )
        []
        """
        limit = get_positive_integer('limit', limit)
        return self.execute_command('keys', name_start, name_end, limit)

    def scan(self, name_start, name_end, limit=10):
        """
        Scan and return a dict mapping key/value in the top ``limit`` keys between
        ``name_start`` and ``name_end`` in ascending order

        Similiar with **Redis.SCAN**        

        .. note:: The range is (``name_start``, ``name_end``]. ``name_start``
           isn't in the range, but ``name_end`` is.

        :param string name_start: The lower bound(not included) of keys to be
         returned, empty string ``''`` means -inf
        :param string name_end: The upper bound(included) of keys to be
         returned, empty string ``''`` means +inf
        :param int limit: number of elements will be returned.
        :return: a dict mapping key/value in ascending order
        :rtype: OrderedDict
        
        >>> ssdb.scan('set_x1', 'set_x3', 10)
        {'set_x2': 'x2', 'set_x3': 'x3'}
        >>> ssdb.scan('set_x ', 'set_xx', 3)
        {'set_x1': 'x1', 'set_x2': 'x2', 'set_x3': 'x3'}
        >>> ssdb.scan('set_x ', '', 10)
        {'set_x1': 'x1', 'set_x2': 'x2', 'set_x3': 'x3', 'set_x4': 'x4'}
        >>> ssdb.scan('set_zzzzz ', '', 10)
        {}
        """        
        limit = get_positive_integer('limit', limit)        
        return self.execute_command('scan', name_start, name_end, limit)

    def rscan(self, name_start, name_end, limit=10):
        """
        Scan and return a dict mapping key/value in the top ``limit`` keys between
        ``name_start`` and ``name_end`` in descending order

        .. note:: The range is (``name_start``, ``name_end``]. ``name_start``
           isn't in the range, but ``name_end`` is.

        :param string name_start: The upper bound(not included) of keys to be
         returned, empty string ``''`` means +inf
        :param string name_end: The lower bound(included) of keys to be
         returned, empty string ``''`` means -inf
        :param int limit: number of elements will be returned.
        :return: a dict mapping key/value in descending order
        :rtype: OrderedDict
        
        >>> ssdb.scan('set_x3', 'set_x1', 10)
        {'set_x2': 'x2', 'set_x1': 'x1'}
        >>> ssdb.scan('set_xx', 'set_x ', 3)
        {'set_x4': 'x4', 'set_x3': 'x3', 'set_x2': 'x2'}
        >>> ssdb.scan('', 'set_x ', 10)
        {'set_x4': 'x4', 'set_x3': 'x3', 'set_x2': 'x2', 'set_x1': 'x1'}
        >>> ssdb.scan('', 'set_zzzzz', 10)
        {}
        """                
        limit = get_positive_integer('limit', limit)        
        return self.execute_command('rscan', name_start, name_end, limit)

    #### HASH OPERATION ####
    def hget(self, name, key):
        """
        Get the value of ``key`` within the hash ``name``

        Like **Redis.HGET**        

        :param string name: the hash name
        :param string key: the key name
        :return: the value at ``key`` within hash ``name`` , or ``None`` if the
         ``name`` or ``key`` doesn't exist
        :rtype: string

        >>> ssdb.hget("hash_1", 'a')
        'A'
        >>> ssdb.hget("hash_1", 'b')
        'B'
        >>> ssdb.hget("hash_1", 'z')
        >>>
        >>> ssdb.hget("hash_2", 'key1')
        '42'
        """                
        return self.execute_command('hget', name, key)

    def hset(self, name, key, value):
        """
        Set the value of ``key`` within the hash ``name`` to ``value``

        Like **Redis.HSET**        

        :param string name: the hash name
        :param string key: the key name
        :param string value: a string or an object can be converted to string
        :return: ``True`` if ``hset`` created a new field, otherwise ``False``
        :rtype: bool
        
        >>> ssdb.hset("hash_3", 'yellow', '#FFFF00')
        True
        >>> ssdb.hset("hash_3", 'red', '#FF0000')
        True
        >>> ssdb.hset("hash_3", 'blue', '#0000FF')
        True
        >>> ssdb.hset("hash_3", 'yellow', '#FFFF00')
        False
        """        
        return self.execute_command('hset', name, key, value)
    hadd = hset

    def hdel(self, name, key):
        """
        Remove the ``key`` from hash ``name``

        Like **Redis.HDEL**        

        :param string name: the hash name
        :param string key: the key name
        :return: ``True`` if deleted successfully, otherwise ``False``
        :rtype: bool

        >>> ssdb.hdel("hash_2", 'key1')
        True
        >>> ssdb.hdel("hash_2", 'key2')
        True
        >>> ssdb.hdel("hash_2", 'key3')
        True
        >>> ssdb.hdel("hash_2", 'key_not_exist')
        False
        >>> ssdb.hdel("hash_not_exist", 'key1')
        False
        """
        return self.execute_command('hdel', name, key)
    hremove = hdel
    hdelete = hdel

    def hclear(self, name):
        """
        **Clear&Delete** the hash specified by ``name``

        :param string name: the hash name
        :return: the length of removed elements
        :rtype: int

        >>> ssdb.hclear('hash_1')
        7
        >>> ssdb.hclear('hash_1')
        0
        >>> ssdb.hclear('hash_2')
        6
        >>> ssdb.hclear('not_exist')
        0
        """        
        return self.execute_command('hclear', name)

    def hexists(self, name, key):
        """
        Return a boolean indicating whether ``key`` exists within hash ``name``

        Like **Redis.HEXISTS**        

        :param string name: the hash name
        :param string key: the key name        
        :return: ``True`` if the key exists, ``False`` if not
        :rtype: bool

        >>> ssdb.hexists('hash_1', 'a')
        True
        >>> ssdb.hexists('hash_2', 'key2')
        True
        >>> ssdb.hexists('hash_not_exist', 'a')
        False
        >>> ssdb.hexists('hash_1', 'z_not_exists')
        False
        >>> ssdb.hexists('hash_not_exist', 'key_not_exists')
        False                        
        """                
        return self.execute_command('hexists', name, key)

    def hincr(self, name, key, amount=1):
        """
        Increase the value of ``key`` in hash ``name`` by ``amount``. If no key
        exists, the value will be initialized as ``amount``

        Like **Redis.HINCR**        

        :param string name: the hash name
        :param string key: the key name        
        :param int amount: increments
        :return: the integer value of ``key`` in hash ``name``
        :rtype: int

        >>> ssdb.hincr('hash_2', 'key1', 7)
        49
        >>> ssdb.hincr('hash_2', 'key2', 3)
        6
        >>> ssdb.hincr('hash_2', 'key_not_exists', 101)
        101
        >>> ssdb.hincr('hash_not_exists', 'key_not_exists', 8848)
        8848
        """                        
        amount = get_integer('amount', amount)        
        return self.execute_command('hincr', name, key, amount)

    def hdecr(self, name, key, amount=1):
        """
        Decrease the value of ``key`` in hash ``name`` by ``amount``. If no key
        exists, the value will be initialized as 0 - ``amount`` 

        :param string name: the hash name
        :param string key: the key name        
        :param int amount: increments
        :return: the integer value of ``key`` in hash ``name``
        :rtype: int

        >>> ssdb.hdecr('hash_2', 'key1', 7)
        35
        >>> ssdb.hdecr('hash_2', 'key2', 3)
        0
        >>> ssdb.hdecr('hash_2', 'key_not_exists', 101)
        -101
        >>> ssdb.hdecr('hash_not_exists', 'key_not_exists', 8848)
        -8848
        """                                
        amount = get_positive_integer('amount', amount)        
        return self.execute_command('hdecr', name, key, amount)

    def hsize(self, name):
        """
        Return the number of elements in hash ``name``

        Like **Redis.HLEN**        
        
        :param string name: the hash name
        :return: the size of hash `name``
        :rtype: int

        >>> ssdb.hsize('hash_1')
        7
        >>> ssdb.hsize('hash_2')
        6
        >>> ssdb.hsize('hash_not_exists')
        0
        """                                        
        return self.execute_command('hsize', name)
    hlen=hsize

    def multi_hset(self, name, **kvs):
        """
        Set key to value within hash ``name`` for each corresponding key and
        value from the ``kvs`` dict.

        Like **Redis.HMSET**

        :param string name: the hash name        
        :param list keys: a list of keys
        :return: the number of successful creation
        :rtype: int

        >>> ssdb.multi_hset('hash_4', a='AA', b='BB', c='CC', d='DD')
        4
        >>> ssdb.multi_hset('hash_4', a='AA', b='BB', c='CC', d='DD')
        0
        >>> ssdb.multi_hset('hash_4', a='AA', b='BB', c='CC', d='DD', e='EE')
        1        
        """                                
        return self.execute_command('multi_hset', name, *dict_to_list(kvs))
    hmset = multi_hset

    def multi_hget(self, name, *keys):
        """
        Return a dictionary mapping key/value by ``keys`` from hash ``names``

        Like **Redis.HMGET**        

        :param string name: the hash name        
        :param list keys: a list of keys
        :return: a dict mapping key/value
        :rtype: dict

        >>> ssdb.multi_hget('hash_1', 'a', 'b', 'c', 'd')
        {'a': 'A', 'c': 'C', 'b': 'B', 'd': 'D'}
        >>> ssdb.multi_hget('hash_2', 'key2', 'key5')
        {'key2': '3.1415926', 'key5': 'e'}
        """
        return self.execute_command('multi_hget', name, *keys)
    hmget = multi_hget

    def multi_hdel(self, name, *keys):
        """
        Remove ``keys`` from hash ``name``

        Like **Redis.HMDEL**        

        :param string name: the hash name        
        :param list keys: a list of keys
        :return: the number of successful deletion
        :rtype: int

        >>> ssdb.multi_hdel('hash_1', 'a', 'b', 'c', 'd')
        4
        >>> ssdb.multi_hdel('hash_1', 'a', 'b', 'c', 'd')
        0        
        >>> ssdb.multi_hdel('hash_2', 'key2_not_exist', 'key5_not_exist')
        0
        """
        return self.execute_command('multi_hdel', name, *keys)
    hmdel = multi_hdel

    def hkeys(self, name, key_start, key_end, limit=10):
        """
        Return a list of the top ``limit`` keys between ``key_start`` and
        ``key_end`` in hash ``name``

        Similiar with **Redis.HKEYS**        

        .. note:: The range is (``key_start``, ``key_end``]. The ``key_start``
           isn't in the range, but ``key_end`` is.

        :param string name: the hash name
        :param string key_start: The lower bound(not included) of keys to be
         returned, empty string ``''`` means -inf
        :param string key_end: The upper bound(included) of keys to be
         returned, empty string ``''`` means +inf
        :param int limit: number of elements will be returned.
        :return: a list of keys
        :rtype: list
        
        >>> ssdb.hkeys('hash_1', 'a', 'g', 10)
        ['b', 'c', 'd', 'e', 'f', 'g']
        >>> ssdb.hkeys('hash_2', 'key ', 'key4', 3)
        ['key1', 'key2', 'key3']
        >>> ssdb.hkeys('hash_1', 'f', '', 10)
        ['g']
        >>> ssdb.hkeys('hash_2', 'keys', '', 10)
        []
        """        
        limit = get_positive_integer('limit', limit)
        return self.execute_command('hkeys', name, key_start, key_end, limit)

    def hgetall(self, name):
        """
        Return a Python dict of the hash's name/value pairs

        Like **Redis.HGETALL** 

        :param string name: the hash name
        :return: a dict mapping key/value
        :rtype: dict
        
        >>> ssdb.hgetall('hash_1')
        {"a":'aa',"b":'bb',"c":'cc'}
        """        
        return self.execute_command('hgetall', name)    

    def hlist(self, name_start, name_end, limit=10):
        """
        Return a list of the top ``limit`` hash's name between ``name_start`` and
        ``name_end`` in ascending order

        .. note:: The range is (``name_start``, ``name_end``]. The ``name_start``
           isn't in the range, but ``name_end`` is.

        :param string name_start: The lower bound(not included) of hash names to
         be returned, empty string ``''`` means -inf
        :param string name_end: The upper bound(included) of hash names to be
         returned, empty string ``''`` means +inf
        :param int limit: number of elements will be returned.
        :return: a list of hash's name
        :rtype: list
        
        >>> ssdb.hlist('hash_ ', 'hash_z', 10)
        ['hash_1', 'hash_2']
        >>> ssdb.hlist('hash_ ', '', 3)
        ['hash_1', 'hash_2']
        >>> ssdb.hlist('', 'aaa_not_exist', 10)
        []
        """                
        limit = get_positive_integer('limit', limit)
        return self.execute_command('hlist', name_start, name_end, limit)

    def hrlist(self, name_start, name_end, limit=10):
        """
        Return a list of the top ``limit`` hash's name between ``name_start`` and
        ``name_end`` in descending order

        .. note:: The range is (``name_start``, ``name_end``]. The ``name_start``
           isn't in the range, but ``name_end`` is.

        :param string name_start: The lower bound(not included) of hash names to
         be returned, empty string ``''`` means +inf
        :param string name_end: The upper bound(included) of hash names to be
         returned, empty string ``''`` means -inf
        :param int limit: number of elements will be returned.
        :return: a list of hash's name
        :rtype: list
        
        >>> ssdb.hrlist('hash_ ', 'hash_z', 10)
        ['hash_2', 'hash_1']
        >>> ssdb.hrlist('hash_ ', '', 3)
        ['hash_2', 'hash_1']
        >>> ssdb.hrlist('', 'aaa_not_exist', 10)
        []
        """                
        limit = get_positive_integer('limit', limit)
        return self.execute_command('hrlist', name_start, name_end, limit)        

    def hscan(self, name, key_start, key_end, limit=10):
        """
        Return a dict mapping key/value in the top ``limit`` keys between
        ``key_start`` and ``key_end`` within hash ``name`` in ascending order

        Similiar with **Redis.HSCAN**

        .. note:: The range is (``key_start``, ``key_end``]. The ``key_start``
           isn't in the range, but ``key_end`` is.

        :param string name: the hash name
        :param string key_start: The lower bound(not included) of keys to be
         returned, empty string ``''`` means -inf
        :param string key_end: The upper bound(included) of keys to be
         returned, empty string ``''`` means +inf
        :param int limit: number of elements will be returned.
        :return: a dict mapping key/value in ascending order
        :rtype: OrderedDict
        
        >>> ssdb.hscan('hash_1', 'a', 'g', 10)
        {'b': 'B', 'c': 'C', 'd': 'D', 'e': 'E', 'f': 'F', 'g': 'G'}
        >>> ssdb.hscan('hash_2', 'key ', 'key4', 3)
        {'key1': '42', 'key2': '3.1415926', 'key3': '-1.41421'}
        >>> ssdb.hscan('hash_1', 'f', '', 10)
        {'g': 'G'}
        >>> ssdb.hscan('hash_2', 'keys', '', 10)
        {}
        """                
        limit = get_positive_integer('limit', limit)        
        return self.execute_command('hscan', name, key_start, key_end, limit)

    
    def hrscan(self, name, key_start, key_end, limit=10):
        """
        Return a dict mapping key/value in the top ``limit`` keys between
        ``key_start`` and ``key_end`` within hash ``name`` in descending order

        .. note:: The range is (``key_start``, ``key_end``]. The ``key_start``
           isn't in the range, but ``key_end`` is.

        :param string name: the hash name
        :param string key_start: The upper bound(not included) of keys to be
         returned, empty string ``''`` means +inf
        :param string key_end: The lower bound(included) of keys to be
         returned, empty string ``''`` means -inf
        :param int limit: number of elements will be returned.
        :return: a dict mapping key/value in descending order
        :rtype: OrderedDict
        
        >>> ssdb.hrscan('hash_1', 'g', 'a', 10)
        {'f': 'F', 'e': 'E', 'd': 'D', 'c': 'C', 'b': 'B', 'a': 'A'}
        >>> ssdb.hrscan('hash_2', 'key7', 'key1', 3)
        {'key6': 'log', 'key5': 'e', 'key4': '256'}
        >>> ssdb.hrscan('hash_1', 'c', '', 10)
        {'b': 'B', 'a': 'A'}
        >>> ssdb.hscan('hash_2', 'keys', '', 10)
        {}        
        """
        limit = get_positive_integer('limit', limit)        
        return self.execute_command('hrscan', name, key_start, key_end, limit)

    #### ZSET OPERATION ####
    def zset(self, name, key, score=1):
        """
        Set the score of ``key`` from the zset ``name`` to ``score``

        Like **Redis.ZADD**

        :param string name: the zset name
        :param string key: the key name
        :param int score: the score for ranking
        :return: ``True`` if ``zset`` created a new score, otherwise ``False``
        :rtype: bool

        >>> ssdb.zset("zset_1", 'z', 1024)
        True
        >>> ssdb.zset("zset_1", 'a', 1024)
        False
        >>> ssdb.zset("zset_2", 'key_10', -4)
        >>>
        >>> ssdb.zget("zset_2", 'key1')
        42        
        """
        score = get_integer('score', score)        
        return self.execute_command('zset', name, key, score)
    zadd = zset

    def zget(self, name, key):
        """
        Return the score of element ``key`` in sorted set ``name``

        Like **Redis.ZSCORE**        

        :param string name: the zset name
        :param string key: the key name
        :return: the score, ``None`` if the zset ``name`` or the ``key`` doesn't exist
        :rtype: int

        >>> ssdb.zget("zset_1", 'a')
        30
        >>> ssdb.zget("zset_1", 'b')
        20
        >>> ssdb.zget("zset_1", 'z')
        >>>
        >>> ssdb.zget("zset_2", 'key1')
        42
        """        
        return self.execute_command('zget', name, key)
    zscore = zget

    def zdel(self, name, key):
        """
        Remove the specified ``key`` from zset ``name``

        Like **Redis.ZREM**

        :param string name: the zset name
        :param string key: the key name
        :return: ``True`` if deleted success, otherwise ``False``
        :rtype: bool

        >>> ssdb.zdel("zset_2", 'key1')
        True
        >>> ssdb.zdel("zset_2", 'key2')
        True
        >>> ssdb.zdel("zset_2", 'key3')
        True
        >>> ssdb.zdel("zset_2", 'key_not_exist')
        False
        >>> ssdb.zdel("zset_not_exist", 'key1')
        False        
        """                

        return self.execute_command('zdel', name, key)
    zremove = zdel
    zdelete = zdel

    def zclear(self, name):
        """
        **Clear&Delete** the zset specified by ``name``

        :param string name: the zset name
        :return: the length of removed elements
        :rtype: int

        >>> ssdb.zclear('zset_1')
        7
        >>> ssdb.zclear('zset_1')
        0
        >>> ssdb.zclear('zset_2')
        6
        >>> ssdb.zclear('not_exist')
        0
        """                
        return self.execute_command('zclear', name)

    def zexists(self, name, key):
        """
        Return a boolean indicating whether ``key`` exists within zset ``name``

        :param string name: the zset name
        :param string key: the key name        
        :return: ``True`` if the key exists, ``False`` if not
        :rtype: bool

        >>> ssdb.zexists('zset_1', 'a')
        True
        >>> ssdb.zexists('zset_2', 'key2')
        True
        >>> ssdb.zexists('zset_not_exist', 'a')
        False
        >>> ssdb.zexists('zset_1', 'z_not_exists')
        False
        >>> ssdb.zexists('zset_not_exist', 'key_not_exists')
        False                        
        """                        
        return self.execute_command('zexists', name, key)

    def zincr(self, name, key, amount=1):
        """
        Increase the score of ``key`` in zset ``name`` by ``amount``. If no key
        exists, the value will be initialized as ``amount``

        Like **Redis.ZINCR**

        :param string name: the zset name
        :param string key: the key name        
        :param int amount: increments
        :return: the integer value of ``key`` in zset ``name``
        :rtype: int

        >>> ssdb.zincr('zset_2', 'key1', 7)
        49
        >>> ssdb.zincr('zset_2', 'key2', 3)
        317
        >>> ssdb.zincr('zset_2', 'key_not_exists', 101)
        101
        >>> ssdb.zincr('zset_not_exists', 'key_not_exists', 8848)
        8848
        """                                
        amount = get_integer('amount', amount)        
        return self.execute_command('zincr', name, key, amount)

    def zdecr(self, name, key, amount=1):
        """
        Decrease the value of ``key`` in zset ``name`` by ``amount``. If no key
        exists, the value will be initialized as 0 - ``amount`` 

        :param string name: the zset name
        :param string key: the key name        
        :param int amount: increments
        :return: the integer value of ``key`` in zset ``name``
        :rtype: int

        >>> ssdb.zdecr('zset_2', 'key1', 7)
        36
        >>> ssdb.zdecr('zset_2', 'key2', 3)
        311
        >>> ssdb.zdecr('zset_2', 'key_not_exists', 101)
        -101
        >>> ssdb.zdecr('zset_not_exists', 'key_not_exists', 8848)
        -8848
        """                                        
        amount = get_positive_integer('amount', amount)
        return self.execute_command('zdecr', name, key, amount)

    def zsize(self, name):
        """
        Return the number of elements in zset ``name`` 

        Like **Redis.ZCARD**
        
        :param string name: the zset name
        :return: the size of zset `name``
        :rtype: int

        >>> ssdb.zsize('zset_1')
        7
        >>> ssdb.zsize('zset_2')
        6
        >>> ssdb.zsize('zset_not_exists')
        0
        """        
        return self.execute_command('zsize', name)
    zlen = zsize
    zcard = zsize

    def multi_zset(self, name, **kvs):
        """
        Return a dictionary mapping key/value by ``keys`` from zset ``names``

        :param string name: the zset name        
        :param list keys: a list of keys
        :return: the number of successful creation
        :rtype: int

        >>> ssdb.multi_zset('zset_4', a=100, b=80, c=90, d=70)
        4
        >>> ssdb.multi_zset('zset_4', a=100, b=80, c=90, d=70)
        0
        >>> ssdb.multi_zset('zset_4', a=100, b=80, c=90, d=70, e=60)
        1        
        """                                        
        for k,v in kvs.items():
            kvs[k] = get_integer(k, int(v))
        return self.execute_command('multi_zset', name, *dict_to_list(kvs))
    zmset = multi_zset

    def multi_zget(self, name, *keys):
        """
        Return a dictionary mapping key/value by ``keys`` from zset ``names``

        :param string name: the zset name        
        :param list keys: a list of keys
        :return: a dict mapping key/value
        :rtype: dict

        >>> ssdb.multi_zget('zset_1', 'a', 'b', 'c', 'd')
        {'a': 30, 'c': 100, 'b': 20, 'd': 1}
        >>> ssdb.multi_zget('zset_2', 'key2', 'key5')
        {'key2': 314, 'key5': 0}
        """                                
        return self.execute_command('multi_zget', name, *keys)
    zmget = multi_zget

    def multi_zdel(self, name, *keys):
        """
        Remove ``keys`` from zset ``name``

        :param string name: the zset name        
        :param list keys: a list of keys
        :return: the number of successful deletion
        :rtype: int

        >>> ssdb.multi_zdel('zset_1', 'a', 'b', 'c', 'd')
        4
        >>> ssdb.multi_zdel('zset_1', 'a', 'b', 'c', 'd')
        0        
        >>> ssdb.multi_zdel('zset_2', 'key2_not_exist', 'key5_not_exist')
        0
        """        
        return self.execute_command('multi_zdel', name, *keys)
    zmdel = multi_zdel

    def zlist(self, name_start, name_end, limit=10):
        """
        Return a list of the top ``limit`` zset's name between ``name_start`` and
        ``name_end`` in ascending order

        .. note:: The range is (``name_start``, ``name_end``]. The ``name_start``
           isn't in the range, but ``name_end`` is.

        :param string name_start: The lower bound(not included) of zset names to
         be returned, empty string ``''`` means -inf
        :param string name_end: The upper bound(included) of zset names to be
         returned, empty string ``''`` means +inf
        :param int limit: number of elements will be returned.
        :return: a list of zset's name
        :rtype: list
        
        >>> ssdb.zlist('zset_ ', 'zset_z', 10)
        ['zset_1', 'zset_2']
        >>> ssdb.zlist('zset_ ', '', 3)
        ['zset_1', 'zset_2']
        >>> ssdb.zlist('', 'aaa_not_exist', 10)
        []
        """        
        limit = get_positive_integer('limit', limit)
        return self.execute_command('zlist', name_start, name_end, limit)

    def zrlist(self, name_start, name_end, limit=10):
        """
        Return a list of the top ``limit`` zset's name between ``name_start`` and
        ``name_end`` in descending order

        .. note:: The range is (``name_start``, ``name_end``]. The ``name_start``
           isn't in the range, but ``name_end`` is.

        :param string name_start: The lower bound(not included) of zset names to
         be returned, empty string ``''`` means +inf
        :param string name_end: The upper bound(included) of zset names to be
         returned, empty string ``''`` means -inf
        :param int limit: number of elements will be returned.
        :return: a list of zset's name
        :rtype: list
        
        >>> ssdb.zlist('zset_ ', 'zset_z', 10)
        ['zset_2', 'zset_1']
        >>> ssdb.zlist('zset_ ', '', 3)
        ['zset_2', 'zset_1']
        >>> ssdb.zlist('', 'aaa_not_exist', 10)
        []
        """        
        limit = get_positive_integer('limit', limit)
        return self.execute_command('zrlist', name_start, name_end, limit)

    def zkeys(self, name, key_start, score_start, score_end, limit=10):
        """
        Return a list of the top ``limit`` keys after ``key_start`` from zset
        ``name`` with scores between ``score_start`` and ``score_end`` 

        .. note:: The range is (``key_start``+``score_start``, ``key_end``]. That
           means (key.score == score_start && key > key_start || key.score >
           score_start)

        :param string name: the zset name
        :param string key_start: The lower bound(not included) of keys to be
         returned, empty string ``''`` means -inf
        :param string key_end: The upper bound(included) of keys to be
         returned, empty string ``''`` means +inf
        :param int limit: number of elements will be returned.
        :return: a list of keys
        :rtype: list

        >>> ssdb.zkeys('zset_1', '', 0, 200, 10)
        ['g', 'd', 'b', 'a', 'e', 'c']
        >>> ssdb.zkeys('zset_1', '', 0, 200, 3)
        ['g', 'd', 'b']
        >>> ssdb.zkeys('zset_1', 'b', 20, 200, 3)
        ['a', 'e', 'c']
        >>> ssdb.zkeys('zset_1', 'c', 100, 200, 3)
        []
        """                
        score_start = get_integer_or_emptystring('score_start', score_start)
        score_end = get_integer_or_emptystring('score_end', score_end)        
        limit = get_positive_integer('limit', limit)
        return self.execute_command('zkeys', name, key_start, score_start,
                                    score_end, limit)    

    def zscan(self, name, key_start, score_start, score_end, limit=10):
        """
        Return a dict mapping key/score of the top ``limit`` keys after
        ``key_start`` with scores between ``score_start`` and ``score_end`` in
        zset ``name`` in ascending order

        Similiar with **Redis.ZSCAN**

        .. note:: The range is (``key_start``+``score_start``, ``key_end``]. That
           means (key.score == score_start && key > key_start || key.score >
           score_start)           

        :param string name: the zset name
        :param string key_start: The key related to score_start, could be empty
         string ``''``
        :param int score_start: The minimum score related to keys(may not be
         included, depend on key_start), empty string ``''`` means -inf
        :param int score_end: The maximum score(included) related to keys,
         empty string ``''`` means +inf
        :param int limit: number of elements will be returned.
        :return: a dict mapping key/score in ascending order
        :rtype: OrderedDict
        
        >>> ssdb.zscan('zset_1', '', 0, 200, 10)
        {'g': 0, 'd': 1, 'b': 20, 'a': 30, 'e': 64, 'c': 100}
        >>> ssdb.zscan('zset_1', '', 0, 200, 3)
        {'g': 0, 'd': 1, 'b': 20}
        >>> ssdb.zscan('zset_1', 'b', 20, 200, 3)
        {'a': 30, 'e': 64, 'c': 100}
        >>> ssdb.zscan('zset_1', 'c', 100, 200, 3)
        {}
        """        
        score_start = get_integer_or_emptystring('score_start', score_start)
        score_end = get_integer_or_emptystring('score_end', score_end)
        limit = get_positive_integer('limit', limit)        
        return self.execute_command('zscan', name, key_start, score_start,
                                    score_end, limit)

    def zrscan(self, name, key_start, score_start, score_end, limit=10):
        """
        Return a dict mapping key/score of the top ``limit`` keys after
        ``key_start`` with scores between ``score_start`` and ``score_end`` in
        zset ``name`` in descending order

        .. note:: The range is (``key_start``+``score_start``, ``key_end``]. That
           means (key.score == score_start && key < key_start || key.score <
           score_start)

        :param string name: the zset name
        :param string key_start: The key related to score_start, could be empty
         string ``''``
        :param int score_start: The maximum score related to keys(may not be
         included, depend on key_start), empty string ``''`` means +inf
        :param int score_end: The minimum score(included) related to keys,
         empty string ``''`` means -inf        
        :param int limit: number of elements will be returned.
        :return: a dict mapping key/score in descending order
        :rtype: OrderedDict
        
        >>> ssdb.zrscan('zset_1', '', '', '', 10)
        {'c': 100, 'e': 64, 'a': 30, 'b': 20, 'd': 1, 'g': 0, 'f': -3}
        >>> ssdb.zrscan('zset_1', '', 1000, -1000, 3)
        {'c': 100, 'e': 64, 'a': 30}
        >>> ssdb.zrscan('zset_1', 'a', 30, -1000, 3)
        {'b': 20, 'd': 1, 'g': 0}
        >>> ssdb.zrscan('zset_1', 'g', 0, -1000, 3)
        {'g': 0}
        """       
        score_start = get_integer_or_emptystring('score_start', score_start)
        score_end = get_integer_or_emptystring('score_end', score_end)                
        limit = get_positive_integer('limit', limit)        
        return self.execute_command('zrscan', name, key_start, score_start,
                                    score_end, limit)

    def zrank(self, name, key):
        """
        Returns a 0-based value indicating the rank of ``key`` in zset ``name``

        Like **Redis.ZRANK**

        .. warning:: This method may be extremly SLOW! May not be used in an
           online service.
           
        :param string name: the zset name
        :param string key: the key name     
        :return: the rank of ``key`` in zset ``name``, **-1** if the ``key`` or
         the ``name`` doesn't exists
        :rtype: int

        >>> ssdb.zrank('zset_1','d')
        2
        >>> ssdb.zrank('zset_1','f')
        0
        >>> ssdb.zrank('zset_1','x')
        -1
        """        
        return self.execute_command('zrank', name, key)

    def zrrank(self, name, key):
        """
        Returns a 0-based value indicating the descending rank of ``key`` in
        zset
        
        .. warning:: This method may be extremly SLOW! May not be used in an
           online service.
           
        :param string name: the zset name
        :param string key: the key name     
        :return: the reverse rank of ``key`` in zset ``name``, **-1** if the
         ``key`` or the ``name`` doesn't exists
        :rtype: int

        >>> ssdb.zrrank('zset_1','d')
        4
        >>> ssdb.zrrank('zset_1','f')
        6
        >>> ssdb.zrrank('zset_1','x')
        -1
        """                
        return self.execute_command('zrrank', name, key)

    def zrange(self, name, offset, limit):
        """
        Return a dict mapping key/score in a range of score from zset ``name``
        between ``offset`` and ``offset+limit`` sorted in ascending order.

        Like **Redis.ZRANGE**                
        
        .. warning:: This method is SLOW for large offset!
           
        :param string name: the zset name
        :param int offset: zero or positive,the returned pairs will start at
         this offset
        :param int limit: number of elements will be returned
        :return: a dict mapping key/score in ascending order
        :rtype: OrderedDict

        >>> ssdb.zrange('zset_1', 2, 3)
        {'d': 1, 'b': 20, 'a': 30}
        >>> ssdb.zrange('zset_1', 0, 2)
        {'f': -3, 'g': 0}
        >>> ssdb.zrange('zset_1', 10, 10)
        {}
        """
        offset = get_nonnegative_integer('offset', offset)        
        limit = get_positive_integer('limit', limit)        
        return self.execute_command('zrange', name, offset, limit)

    def zrrange(self, name, offset, limit):
        """
        Return a dict mapping key/score in a range of score from zset ``name``
        between ``offset`` and ``offset+limit`` sorted in descending order.        
        
        .. warning:: This method is SLOW for large offset!
           
        :param string name: the zset name
        :param int offset: zero or positive,the returned pairs will start at
         this offset
        :param int limit: number of elements will be returned
        :return: a dict mapping key/score in ascending order
        :rtype: OrderedDict

        >>> ssdb.zrrange('zset_1', 0, 4)
        {'c': 100, 'e': 64, 'a': 30, 'b': 20}
        >>> ssdb.zrrange('zset_1', 4, 5)
        {'d': 1, 'g': 0, 'f': -3}
        >>> ssdb.zrrange('zset_1', 10, 10)
        {}        
        """        
        offset = get_nonnegative_integer('offset', offset)        
        limit = get_positive_integer('limit', limit)        
        return self.execute_command('zrrange', name, offset, limit)


    def zcount(self, name, score_start, score_end):
        """
        Returns the number of elements in the sorted set at key ``name`` with
        a score between ``score_start`` and ``score_end``.

        Like **Redis.ZCOUNT**

        .. note:: The range is [``score_start``, ``score_end``]

        :param string name: the zset name
        :param int score_start: The minimum score related to keys(included),
         empty string ``''`` means -inf
        :param int score_end: The maximum score(included) related to keys,
         empty string ``''`` means +inf
        :return: the number of keys in specified range
        :rtype: int
        
        >>> ssdb.zount('zset_1', 20, 70)
        3
        >>> ssdb.zcount('zset_1', 0, 100)
        6
        >>> ssdb.zcount('zset_1', 2, 3)
        0
        """        
        score_start = get_integer_or_emptystring('score_start', score_start)
        score_end = get_integer_or_emptystring('score_end', score_end)
        return self.execute_command('zcount', name, score_start, score_end)

    def zsum(self, name, score_start, score_end):
        """
        Returns the sum of elements of the sorted set stored at the specified
        key which have scores in the range [score_start,score_end].
        
        .. note:: The range is [``score_start``, ``score_end``]

        :param string name: the zset name
        :param int score_start: The minimum score related to keys(included),
         empty string ``''`` means -inf
        :param int score_end: The maximum score(included) related to keys,
         empty string ``''`` means +inf
        :return: the sum of keys in specified range
        :rtype: int
        
        >>> ssdb.zsum('zset_1', 20, 70)
        114
        >>> ssdb.zsum('zset_1', 0, 100)
        215
        >>> ssdb.zsum('zset_1', 2, 3)
        0
        """        
        score_start = get_integer_or_emptystring('score_start', score_start)
        score_end = get_integer_or_emptystring('score_end', score_end)
        return self.execute_command('zsum', name, score_start, score_end)

    def zavg(self, name, score_start, score_end):
        """
        Returns the average of elements of the sorted set stored at the
        specified key which have scores in the range [score_start,score_end].        
        
        .. note:: The range is [``score_start``, ``score_end``]

        :param string name: the zset name
        :param int score_start: The minimum score related to keys(included),
         empty string ``''`` means -inf
        :param int score_end: The maximum score(included) related to keys,
         empty string ``''`` means +inf
        :return: the average of keys in specified range
        :rtype: int
        
        >>> ssdb.zavg('zset_1', 20, 70)
        38
        >>> ssdb.zavg('zset_1', 0, 100)
        35
        >>> ssdb.zavg('zset_1', 2, 3)
        0
        """        
        score_start = get_integer_or_emptystring('score_start', score_start)
        score_end = get_integer_or_emptystring('score_end', score_end)
        return self.execute_command('zavg', name, score_start, score_end)

    def zremrangebyrank(self, name, rank_start, rank_end):
        """
        Remove the elements of the zset which have rank in the range
        [rank_start,rank_end].
        
        .. note:: The range is [``rank_start``, ``rank_end``]

        :param string name: the zset name
        :param int rank_start: zero or positive,the start position
        :param int rank_end: zero or positive,the end position
        :return: the number of deleted elements
        :rtype: int
        
        >>> ssdb.zremrangebyrank('zset_1', 0, 2)
        3
        >>> ssdb.zremrangebyrank('zset_1', 1, 4)
        5
        >>> ssdb.zremrangebyrank('zset_1', 0, 0)
        1
        """        
        rank_start = get_nonnegative_integer('rank_start', rank_start)
        rank_end = get_nonnegative_integer('rank_end', rank_end)
        return self.execute_command('zremrangebyrank', name, rank_start,
                                    rank_end)

    def zremrangebyscore(self, name, score_start, score_end):
        """
        Delete the elements of the zset which have rank in the range
        [score_start,score_end].
        
        .. note:: The range is [``score_start``, ``score_end``]

        :param string name: the zset name
        :param int score_start: The minimum score related to keys(included),
         empty string ``''`` means -inf
        :param int score_end: The maximum score(included) related to keys,
         empty string ``''`` means +inf        
        :return: the number of deleted elements
        :rtype: int
        
        >>> ssdb.zremrangebyscore('zset_1', 20, 70)
        3
        >>> ssdb.zremrangebyscore('zset_1', 0, 100)
        6
        >>> ssdb.zremrangebyscore('zset_1', 2, 3)
        0        
        """        
        score_start = get_integer_or_emptystring('score_start', score_start)
        score_end = get_integer_or_emptystring('score_end', score_end)
        return self.execute_command('zremrangebyscore', name, score_start,
                                    score_end)

    #### QUEUE OPERATION ####
    def qget(self, name, index):
        """
        Get the element of ``index`` within the queue ``name``

        :param string name: the queue name
        :param int index: the specified index, can < 0
        :return: the value at ``index`` within queue ``name`` , or ``None`` if the
         element doesn't exist
        :rtype: string

        """
        index = get_integer('index', index)
        return self.execute_command('qget', name, index)
    
    def qset(self, name, index, value):
        """
        Set the list element at ``index`` to ``value``. 

        :param string name: the queue name
        :param int index: the specified index, can < 0
        :param string value: the element value
        :return: Unknown
        :rtype: True
        
        """
        index = get_integer('index', index)        
        return self.execute_command('qset', name, index, value)

    def qpush_back(self, name, *items):
        """
        Push ``items`` onto the tail of the list ``name``

        Like **Redis.RPUSH**

        :param string name: the queue name
        :param list items: the list of items
        :return: length of queue
        :rtype: int
        
        """
        return self.execute_command('qpush_back', name, *items)
    qpush=qpush_back

    def qpush_front(self, name, *items):
        """
        Push ``items`` onto the head of the list ``name``

        Like **Redis.LPUSH**

        :param string name: the queue name
        :param int index: the specified index
        :param string value: the element value
        :return: length of queue
        :rtype: int
        
        """
        return self.execute_command('qpush_front', name, *items)

    def qpop_front(self, name, size=1):
        """
        Remove and return the first ``size`` item of the list ``name``

        Like **Redis.LPOP**        

        :param string name: the queue name
        :param int size: the length of result
        :return: the list of pop elements
        :rtype: list
        
        """
        size = get_positive_integer("size", size)
        return self.execute_command('qpop_front', name, size)
    qpop = qpop_front

    def qpop_back(self, name, size=1):
        """
        Remove and return the last ``size`` item of the list ``name``

        Like **Redis.RPOP**

        :param string name: the queue name
        :param int size: the length of result
        :return: the list of pop elements
        :rtype: list
        
        """
        size = get_positive_integer("size", size)
        return self.execute_command('qpop_back', name, size)

    def qsize(self, name):
        """
        Return the length of the list ``name`` . If name does not exist, it is
        interpreted as an empty list and 0 is returned.

        Like **Redis.LLEN**

        :param string name: the queue name
        :return: the queue length or 0 if the queue doesn't exist.
        :rtype: int
        
        >>> ssdb.qsize('queue_1')
        7
        >>> ssdb.qsize('queue_2')
        6
        >>> ssdb.qsize('queue_not_exists')
        0        
        """
        return self.execute_command('qsize', name)
    qlen = qsize

    def qclear(self, name):
        """
        **Clear&Delete** the queue specified by ``name``

        :param string name: the queue name
        :return: the length of removed elements
        :rtype: int

        """
        return self.execute_command('qclear', name)    

    def qlist(self, name_start, name_end, limit):
        """
        Return a list of the top ``limit`` keys between ``name_start`` and
        ``name_end``  in ascending order

        .. note:: The range is (``name_start``, ``name_end``]. ``name_start``
           isn't in the range, but ``name_end`` is.

        :param string name_start: The lower bound(not included) of keys to be
         returned, empty string ``''`` means -inf
        :param string name_end: The upper bound(included) of keys to be
         returned, empty string ``''`` means +inf
        :param int limit: number of elements will be returned.
        :return: a list of keys
        :rtype: list

        >>> ssdb.qlist('queue_1', 'queue_2', 10)
        ['queue_2']
        >>> ssdb.qlist('queue_', 'queue_2', 10)
        ['queue_1', 'queue_2']
        >>> ssdb.qlist('z', '', 10)
        []        
        """
        limit = get_positive_integer("limit", limit)
        return self.execute_command('qlist', name_start, name_end, limit)

    def qrlist(self, name_start, name_end, limit):
        """
        Return a list of the top ``limit`` keys between ``name_start`` and
        ``name_end``  in descending order

        .. note:: The range is (``name_start``, ``name_end``]. ``name_start``
           isn't in the range, but ``name_end`` is.

        :param string name_start: The lower bound(not included) of keys to be
         returned, empty string ``''`` means +inf
        :param string name_end: The upper bound(included) of keys to be
         returned, empty string ``''`` means -inf
        :param int limit: number of elements will be returned.
        :return: a list of keys
        :rtype: list

        >>> ssdb.qrlist('queue_2', 'queue_1', 10)
        ['queue_1']
        >>> ssdb.qrlist('queue_z', 'queue_', 10)
        ['queue_2', 'queue_1']
        >>> ssdb.qrlist('z', '', 10)
        ['queue_2', 'queue_1']
        """
        limit = get_positive_integer("limit", limit)
        return self.execute_command('qrlist', name_start, name_end, limit)

    def qfront(self, name):
        """
        Returns the first element of a queue.
        
        :param string name: the queue name
        :return: ``None`` if queue empty, otherwise the item returned
        :rtype: string
        
        """
        return self.execute_command('qfront', name)

    def qback(self, name):
        """
        Returns the last element of a queue.
        
        :param string name: the queue name
        :return: ``None`` if queue empty, otherwise the item returned
        :rtype: string
        
        """
        return self.execute_command('qback', name)

    def qrange(self, name, offset, limit):
        """
        Return a ``limit`` slice of the list ``name`` at position ``offset``

        ``offset`` can be negative numbers just like Python slicing notation

        Similiar with **Redis.LRANGE**

        :param string name: the queue name
        :param int offset: the returned list will start at this offset
        :param int limit: number of elements will be returned
        :return: a list of elements
        :rtype: list
        
        """
        offset = get_integer('offset', offset)        
        limit = get_positive_integer('limit', limit)                
        return self.execute_command('qrange', name, offset, limit)

    def qslice(self, name, start, end):
        """
        Return a slice of the list ``name`` between
        position ``start`` and ``end``

        ``start`` and ``end`` can be negative numbers just like
        Python slicing notation

        Like **Redis.LRANGE**

        :param string name: the queue name
        :param int start: the returned list will start at this offset
        :param int end: the returned list will end at this offset
        :return: a list of elements
        :rtype: list
        """
        start = get_integer('start', start)
        end = get_integer('end', end)
        return self.execute_command('qslice', name, start, end)

    def qtrim_front(self, name, size=1):
        """
        Sets the list element at ``index`` to ``value``. An error is returned for out of
        range indexes.

        :param string name: the queue name
        :param int size: the max length of removed elements
        :return: the length of removed elements
        :rtype: int
        
        """
        size = get_positive_integer("size", size)
        return self.execute_command('qtrim_front', name, size)
    qrem_front = qtrim_front
    
    def qtrim_back(self, name, size=1):
        """
        Sets the list element at ``index`` to ``value``. An error is returned for out of
        range indexes.

        :param string name: the queue name
        :param int size: the max length of removed elements        
        :return: the length of removed elements
        :rtype: int
        
        """
        size = get_positive_integer("size", size)
        return self.execute_command('qtrim_back', name, size)
    qrem_back=qtrim_back

    def batch(self):
        return StrictBatch(
            self.connection_pool,
            self.response_callbacks
        )

    pipeline = batch

    ## def contains(self, name):
    ##     p = self.pipeline()
    ##     p.exists(name)
    ##     p.hsize(name)
    ##     p.zsize(name)
    ##     s =p.execute()
    ##     return s[0] or s[1] or [2]

    def hash_exists(self, name):
        """
        Return a boolean indicating whether hash ``name`` exists

        :param string name: the hash name
        :return: ``True`` if the hash exists, ``False`` if not
        :rtype: string

        >>> ssdb.hash_exists('hash_1')
        True
        >>> ssdb.hash_exists('hash_2')
        True
        >>> ssdb.hash_exists('hash_not_exist')
        False        
        """                
        try:
            return hsize(name) > 0
        except:
            return False

    def zset_exists(self, name):
        """
        Return a boolean indicating whether zset ``name`` exists

        :param string name: the zset name
        :return: ``True`` if the zset exists, ``False`` if not
        :rtype: string

        >>> ssdb.zset_exists('zset_1')
        True
        >>> ssdb.zset_exists('zset_2')
        True
        >>> ssdb.zset_exists('zset_not_exist')
        False        
        """                
        try:
            return zsize(name) > 0
        except:
            return False    
    
    def queue_exists(self, name):
        """
        Return a boolean indicating whether queue ``name`` exists

        :param string name: the queue name
        :return: ``True`` if the queue exists, ``False`` if not
        :rtype: string

        >>> ssdb.queue_exists('queue_1')
        True
        >>> ssdb.queue_exists('queue_2')
        True
        >>> ssdb.queue_exists('queue_not_exist')
        False        
        """                
        try:
            return hsize(name) > 0
        except:
            return False    
    
        
class SSDB(StrictSSDB):
    """
    Provides backwards compatibility with older versions of ssdb-py(1.6.6) that changed
    arguments to some commands to be more Pythonic, sane, or by accident. 
    """
    RESPONSE_CALLBACKS = dict_merge(
        StrictSSDB.RESPONSE_CALLBACKS,
        {
            'setx': lambda r: True,
        }
    )

    def batch(self):
        return Batch(
            self.connection_pool,
            self.response_callbacks
        )
    pipeline = batch

    def setx(self, name, value, ttl):
        """
        Set the value of key ``name`` to ``value`` that expires in ``ttl``
        seconds. ``ttl`` can be represented by an integer or a Python
        timedelta object.

        Like **Redis.SETEX**

        :param string name: the key name
        :param string value: a string or an object can be converted to string
        :param int ttl: positive int seconds or timedelta object
        :return: ``True`` on success, ``False`` if not
        :rtype: bool

        >>> import time
        >>> ssdb.set("test_ttl", 'ttl', 4)
        True
        >>> ssdb.get("test_ttl")
        'ttl'
        >>> time.sleep(4)
        >>> ssdb.get("test_ttl")
        >>> 
        """
        if isinstance(ttl, datetime.timedelta):
            ttl = ttl.seconds + ttl.days * 24 * 3600
        ttl = get_positive_integer('ttl', ttl)
        return self.execute_command('setx', name, value, ttl)

    
class StrictBatch(BaseBatch, StrictSSDB):
    """
    Batch for the StrictSSDB class
    """
    parse_response = StrictSSDB.parse_response
    #exec = execute

    
class Batch(BaseBatch, SSDB):
    """
    Batch for the SSDB class
    """
    parse_response = SSDB.parse_response
    #exec = execute            
    
