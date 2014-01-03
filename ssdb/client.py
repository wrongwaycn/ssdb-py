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
from ssdb.exceptions import (
    ConnectionError,
    DataError,
    SSDBError,
    ResponseError,
    WatchError,
    NoScriptError,
    ExecAbortError,
    )

SYM_EMPTY = b('')

def get_integer(name, num):
    if not isinstance(num, int):
        raise ValueError('``%s`` must be a integer' % name)
    return num

def get_integer_or_emptystring(name, num):
    if not isinstance(num, int) and num != '':
        raise ValueError('``%s`` must be a integer or an empty string' % name)
    return num

def get_nonnegative_integer(name, num):
    is_valid = isinstance(num, int) and num >= 0
    if not is_valid:
        raise ValueError('``%s`` must be a nonnegative integer' % name)
    return num

def get_positive_integer(name, num):
    is_valid = isinstance(num, int) and num > 0
    if not is_valid:
        raise ValueError('``%s`` must be a positive integer' % name)
    return num

def get_negative_integer(name, num):
    is_valid = isinstance(num, int) and num < 0
    if not is_valid:
        raise ValueError('``%s`` must be a negative integer' % name)
    return num

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
            'set del exists '
            'hset hdel hclear hexists '
            'zset zdel zclear zexists',
            lambda r: bool(int(r[0]))
        ),
        string_keys_to_dict(
            'get hget',
            lambda r: r[0]
        ),
        string_keys_to_dict(
            'incr decr multi_set multi_del '
            'hincr hdecr hsize multi_hset multi_hdel '
            'zincr zdecr zsize multi_zset multi_zdel zget zrank zrrank',
            lambda r: int(r[0])
        ),
        string_keys_to_dict(
            'multi_get multi_hget '
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
            'keys hkeys hlist zkeys zlist',
            lambda r: r
        ),        
        {
            'zget1': lambda r: int(r[0]),
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
            if status == 'ok' :
                return self.response_callbacks[command_name](response[1:],
                                                             **options)
            if status == 'not_found':
                return None
                #raise DataError('Not Found')
        return response

    #### KEY/VALUES OPERATION ####
    def get(self, name):
        """
        Return the value at key ``name``, or ``None`` if the key doesn't exist

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
        Set the value at key ``name`` to ``value``

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

    def delete(self, name):
        """
        Delete the key specified by ``name``

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

    def exists(self, name):
        """
        Return a boolean indicating whether key ``name`` exists

        .. note:: ``exists`` **can't indicate** whether any `Hash`_ or `Zsets`_ exists, use
           `hash_exists`_ for `Hash`_ and `zset_exists`_ for `Zsets`_        

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
        will be initialized as ``amount`` 

        :param string name: the key name
        :param int amount: increments
        :return: the integer value at key ``name``
        :rtype: int

        >>> ssdb.incr('set_count', 3)
        13
        >>> ssdb.incr('set_count', 1)
        14
        >>> ssdb.incr('temp_count', 42)
        42        
        """                
        amount = get_positive_integer('amount', amount)
        return self.execute_command('incr', name, amount)

    def decr(self, name, amount=1):
        """
        Decrease the value at key ``name`` by ``amount``. If no key exists, the value
        will be initialized as 0 - ``amount`` 

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

    def multi_set(self, **kvs):
        """
        Set key/value based on a mapping dictionary as kwargs.

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
        Return a dict mapping key/value in the top ``limit`` keys between
        ``name_start`` and ``name_end`` in ascending order

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
        Return a dict mapping key/value in the top ``limit`` keys between
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

    def hclear(self, name):
        """
        **Clear&Delete** the hash specified by ``name``

        :param string name: the hash name
        :return: ``True`` on deleting successfully, or ``False`` if the hash `name`` doesn't
         exist or failure
        :rtype: bool

        >>> ssdb.hclear('hash_1')
        True
        >>> ssdb.hclear('hash_1')
        False
        >>> ssdb.hclear('hash_2')
        True
        >>> ssdb.hclear('not_exist')
        False
        """        
        return self.execute_command('hclear', name)

    def hexists(self, name, key):
        """
        Return a boolean indicating whether ``key`` exists within hash ``name``

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
        amount = get_positive_integer('amount', amount)        
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
        36
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
        Return a dictionary mapping key/value by ``keys`` from hash ``names``

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

    def hlist(self, name_start, name_end, limit=10):
        """
        Return a list of the top ``limit`` hash's name between ``name_start`` and
        ``name_end``

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

    def hscan(self, name, key_start, key_end, limit=10):
        """
        Return a dict mapping key/value in the top ``limit`` keys between
        ``key_start`` and ``key_end`` within hash ``name`` in ascending order

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
        Get the score of ``key`` from the zset at ``name``

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

    def zclear(self, name):
        """
        **Clear&Delete** the zset specified by ``name``

        :param string name: the zset name
        :return: ``True`` on deleting successfully, or ``False`` if the zset `name`` doesn't
         exist or failure
        :rtype: bool

        >>> ssdb.zclear('zset_1')
        True
        >>> ssdb.zclear('zset_1')
        False
        >>> ssdb.zclear('zset_2')
        True
        >>> ssdb.zclear('not_exist')
        False
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
        Increase the value of ``key`` in zset ``name`` by ``amount``. If no key
        exists, the value will be initialized as ``amount`` 

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
        amount = get_positive_integer('amount', amount)        
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

        >>> ssdb.multi_hset('zset_4', a=100, b=80, c=90, d=70)
        4
        >>> ssdb.multi_hset('zset_4', a=100, b=80, c=90, d=70)
        0
        >>> ssdb.multi_hset('zset_4', a=100, b=80, c=90, d=70, e=60)
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
        ``name_end``

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

    def zkeys(self, name, key_start, score_start, score_end, limit=10):
        """
        Return a list of the top ``limit`` keys after ``key_start`` from zset
        ``name`` with scores between ``score_start`` and ``score_end`` 

        .. note:: The range is (``key_start``+``key_start``, ``key_end``]. That
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

        .. note:: The range is (``key_start``+``key_start``, ``key_end``]. That
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

        .. note:: The range is (``key_start``+``key_start``, ``key_end``]. That
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
    
