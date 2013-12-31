#coding=utf-8
from __future__ import with_statement
from itertools import chain, starmap, izip_longest
import datetime
import sys
import warnings
import time as mod_time
from ssdb._compat import (b, basestring, bytes, imap, iteritems, iterkeys,
                          itervalues, izip, long, nativestr, urlparse, unicode)
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

def list_to_int_dict(lst):
    return {k:int(v) for k,v in list_to_dict(lst).items()}

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
    
    Connection and Pipeline derive from this, implementing how the commands are
    sent and received to the SSDB server
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
            'multi_get scan rscan '
            'multi_hget hscan hrscan '
            'multi_zget zscan zrscan',            
            list_to_dict
        ),
        string_keys_to_dict(
            'multi_zget zscan zrscan zrange zrrange',
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

    def delete(self, name):
        """
        Delete the key specified by ``name``

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

    def exists(self, name):
        """
        Return a boolean indicating whether key ``name`` exists

        :param string name: the key name
        :return: ``True`` if the key exists, ``False`` if not
        :rtype: string

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

    def multi_get(self, *keys):
        """
        Return a dictionary of key/value from keys

        :param list keys: a list of keys
        :return: a dict mapping key/value
        :rtype: dict

        >>> ssdb.multi_get('a', 'b', 'c', 'd')
        {'a':'a', 'c':'c', 'b':'b', 'd':'d'}
        >>> ssdb.multi_set('set_abc','set_count')
        {'set_abc':'set_abc', 'set_count':'10'}
        """                
        return self.execute_command('multi_get', *keys)

    def multi_delete(self, *keys):
        return self.execute_command('multi_del', *keys)

    def keys(self, key_start, key_end, limit=10):
        limit = get_positive_integer('limit', limit)
        return self.execute_command('keys', key_start, key_end, limit)

    def scan(self, key_start, key_end, limit=10):
        limit = get_positive_integer('limit', limit)        
        return self.execute_command('scan', key_start, key_end, limit)

    def rscan(self, key_start, key_end, limit=10):
        limit = get_positive_integer('limit', limit)        
        return self.execute_command('rscan', key_start, key_end, limit)

    #### HASH OPERATION ####
    def hget(self, name, key):
        """
        Get the value of ``key`` within the hash ``name``

        :param string name: the hash name
        :param string key: the key name
        :return: the value at ``key`` within hash ``name`` , or ``None`` if the
         ``name`` or the ``key`` doesn't exist
        :rtype: string
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
        """        
        return self.execute_command('hset', name, key, value)
    hadd = hset

    def hdelete(self, name, key):
        """
        Remove the ``key`` from hash ``name``

        :param string name: the hash name
        :param string key: the key name
        :return: ``True`` if deleted successfully, otherwise ``False``
        :rtype: bool        
        """
        return self.execute_command('hdel', name, key)

    def hclear(self, name):
        return self.execute_command('hclear', name)

    def hexists(self, name, key):
        return self.execute_command('hexists', name, key)

    def hincr(self, name, key, num=1):
        num = get_positive_integer('num', num)        
        return self.execute_command('hincr', name, key, num)

    def hdecr(self, name, key, num=1):
        num = get_positive_integer('num', num)        
        return self.execute_command('hdecr', name, key, num)

    def hsize(self, name):
        return self.execute_command('hsize', name)

    def multi_hset(self, name, **kvs):
        return self.execute_command('multi_hset', name, *dict_to_list(kvs))

    def multi_hget(self, name, *keys):
        return self.execute_command('multi_hget', name, *keys)

    def multi_hdelete(self, name, *keys):
        return self.execute_command('multi_hdel', name, *keys)

    def hkeys(self, name, key_start, key_end, limit=10):
        limit = get_positive_integer('limit', limit)
        return self.execute_command('hkeys', name, key_start, key_end, limit)

    def hlist(self, key_start, key_end, limit=10):
        limit = get_positive_integer('limit', limit)
        return self.execute_command('hlist', key_start, key_end, limit)    

    def hscan(self, name, key_start, key_end, limit=10):
        limit = get_positive_integer('limit', limit)        
        return self.execute_command('hscan', name, key_start, key_end, limit)

    def hrscan(self, name, key_start, key_end, limit=10):
        limit = get_positive_integer('limit', limit)        
        return self.execute_command('hrscan', name, key_start, key_end, limit)

    #### ZSET OPERATION ####
    def zset(self, name, key, score=1):
        """
        Set the score of ``key`` from the zset ``name`` to ``score``

        :param string name: the hash name
        :param string key: the key name
        :param int score: the score for ranking
        :return: ``True`` if ``zset`` created a new score, otherwise ``False``
        :rtype: bool        
        """
        score = get_integer('score', score)        
        return self.execute_command('zset', name, key, score)
    zadd = zset

    def zget(self, name, key):
        """
        Get the score of ``key`` from the zset at ``name``

        :param string name: the hash name
        :param string key: the key name
        :return: the score, ``None`` if the hash ``name`` or the ``key`` doesn't exist
        :rtype: int
        """        
        return self.execute_command('zget', name, key)
    zscore = zget

    def zdelete(self, name, key):
        """
        Remove the specified ``key`` from the zset at ``name``

        :param string name: the hash name
        :param string key: the key name
        :return: ``True`` if deleted success, otherwise ``False``
        :rtype: bool
        """                

        return self.execute_command('zdel', name, key)

    def zclear(self, name):
        return self.execute_command('zclear', name)

    def zexists(self, name, key):
        return self.execute_command('zexists', name, key)

    def zincr(self, name, key, num=1):
        num = get_positive_integer('num', num)        
        return self.execute_command('zincr', name, key, num)

    def zdecr(self, name, key, num=1):
        num = get_positive_integer('num', num)        
        return self.execute_command('zdecr', name, key, num)

    def zsize(self, name):
        return self.execute_command('zsize', name)

    def multi_zset(self, name, **kvs):
        for k,v in kvs.items():
            kvs[k] = get_integer(k, int(v))
        return self.execute_command('multi_zset', name, *dict_to_list(kvs))

    def multi_zget(self, name, *keys):
        return self.execute_command('multi_zget', name, *keys)

    def multi_zdelete(self, name, *keys):
        return self.execute_command('multi_zdel', name, *keys)

    def zlist(self, name_start, name_end, limit=10):
        limit = get_positive_integer('limit', limit)
        return self.execute_command('zlist', name_start, name_end, limit)

    def zkeys(self, name, key_start, score_start, score_end, limit=10):
        score_start = get_integer('score_start', score_start)
        score_end = get_integer('score_end', score_end)        
        limit = get_positive_integer('limit', limit)
        return self.execute_command('zkeys', name, key_start, score_start,
                                    score_end, limit)    

    def zscan(self, name, key_start, score_start, score_end, limit=10):
        score_start = get_integer('score_start', score_start)
        score_end = get_integer('score_end', score_end)                
        limit = get_positive_integer('limit', limit)        
        return self.execute_command('zscan', name, key_start, score_start,
                                    score_end, limit)

    def zrscan(self, name, key_start, score_start, score_end, limit=10):
        score_start = get_integer('score_start', score_start)
        score_end = get_integer('score_end', score_end)        
        limit = get_positive_integer('limit', limit)        
        return self.execute_command('zrscan', name, key_start, score_start,
                                    score_end, limit)

    def zrank(self, name, key):
        return self.execute_command('zrank', name, key)

    def zrrank(self, name, key):
        return self.execute_command('zrrank', name, key)

    def zrange(self, name, offset, limit):
        offset = get_nonnegative_integer('offset', offset)        
        limit = get_positive_integer('limit', limit)        
        return self.execute_command('zrange', name, offset, limit)

    def zrrange(self, name, offset, limit):
        offset = get_nonnegative_integer('offset', offset)        
        limit = get_positive_integer('limit', limit)        
        return self.execute_command('zrrange', name, offset, limit)

    def batch(self):
        return StrictBatch(
            self.connection_pool,
            self.response_callbacks
        )

    pipeline = batch

    def contains(self, name):
        p = self.pipeline()
        p.exists(name)
        p.hsize(name)
        p.zsize(name)
        s =p.execute()
        return s[0] or s[1] or [2]

    def hash_exists(self, name):
        try:
            return hsize(name) > 0
        except:
            return False

    def zset_exists(self, name):
        try:
            return zsize(name) > 0
        except:
            return False    
    
    
    
        
class SSDB(StrictSSDB):
    """
    Provides backwards compatibility with older versions of ssdb-py that changed
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
    
