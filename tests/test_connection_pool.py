#coding=utf-8
import re
import time
from nose.tools import (assert_equals, assert_dict_equal, assert_not_equals,
                        assert_tuple_equal, raises)
from threading import Thread
import ssdb
from ssdb._compat import Queue
from ssdb.connection import Connection,ConnectionPool,BlockingConnectionPool


class TestConnectionPoolCase(object):

    def setUp(self):
        print('set UP')
        
    def tearDown(self):
        print('tear down')
    
    def get_pool(self, connection_info=None, max_connections=None):
        connection_info = connection_info or {
            "host":'127.0.0.1',
            "port":'8888',
        }
        pool = ConnectionPool(
                connection_class=Connection,
                max_connections=max_connections,
                **connection_info)
        return pool

    def test_connection_creation(self):
        connection_info = {
            "host":'localhost',
            "port":'8888',
        }        
        pool = self.get_pool(connection_info)
        connection = pool.get_connection('_')
        assert_dict_equal(connection.kwargs, connection_info)

    def test_multiple_connections(self):
        pool = self.get_pool()
        c1 = pool.get_connection('_')
        c2 = pool.get_connection('_')
        c3 = pool.get_connection('_')
        c4 = pool.get_connection('_')        
        assert_not_equals(c1,c2)
        assert_not_equals(c1,c3)
        assert_not_equals(c1,c4)
        assert_not_equals(c2,c3)
        assert_not_equals(c2,c4)
        assert_not_equals(c3,c4)

    @raises(ssdb.ConnectionError)
    def test_max_connections(self):
        pool = self.get_pool(max_connections=2)
        pool.get_connection('_')
        pool.get_connection('_')
        pool.get_connection('_')        

    def test_reuse_previously_released_connection(self):
        pool = self.get_pool()
        c1 = pool.get_connection('_')
        pool.release(c1)
        c2 = pool.get_connection('_')
        assert_equals(c1,c2)

    def test_repr_contains_db_info_tcp(self):
        pool = ConnectionPool(host='localhost', port=8888)
        assert_tuple_equal(
            re.match('(.*)<(.*)<(.*)>>', repr(pool)).groups(),
            (
                'ConnectionPool',
                'Connection',
                'host=localhost,port=8888'
            )
        )

        
class TestBlockingConnectionPoolCase(object):

    def setUp(self):
        print('set UP')
        
    def tearDown(self):
        print('tear down')
    
    def get_pool(self, connection_info=None, max_connections=10, timeout=20):
        connection_info = connection_info or {
            "host":'127.0.0.1',
            "port":'8888',
        }
        pool = BlockingConnectionPool(
                connection_class=Connection,
                max_connections=max_connections,
                timeout=timeout,
                **connection_info)
        return pool

    def test_connection_creation(self):
        connection_info = {
            "host":'localhost',
            "port":'8888',
        }        
        pool = self.get_pool(connection_info)
        connection = pool.get_connection('_')
        assert_dict_equal(connection.kwargs, connection_info)

    def test_multiple_connections(self):
        pool = self.get_pool()
        c1 = pool.get_connection('_')
        c2 = pool.get_connection('_')
        c3 = pool.get_connection('_')
        c4 = pool.get_connection('_')        
        assert_not_equals(c1,c2)
        assert_not_equals(c1,c3)
        assert_not_equals(c1,c4)
        assert_not_equals(c2,c3)
        assert_not_equals(c2,c4)
        assert_not_equals(c3,c4)

    def test_max_connections_blocks(self):
        """
        Getting a connection should block for until available.
        """
        q = Queue()
        q.put_nowait('Not yet got')
        pool = self.get_pool(max_connections=2, timeout=5)
        c1 = pool.get_connection('_')
        pool.get_connection('_')

        target = lambda: q.put_nowait(pool.get_connection('_'))
        Thread(target=target).start()

        # Blocks while non available.
        time.sleep(0.05)
        c3 = q.get_nowait()
        assert_equals(c3,'Not yet got')

        # Then got when available.
        pool.release(c1)
        time.sleep(0.05)
        c3 = q.get_nowait()
        assert_equals(c1,c3)

    @raises(ssdb.ConnectionError)
    def test_max_connections_timeout(self):
        """
        Getting a connection raises ``ConnectionError`` after timeout.
        """
        pool = self.get_pool(max_connections=2, timeout=0.1)
        pool.get_connection('_')
        pool.get_connection('_')
        pool.get_connection('_')
        
    def test_reuse_previously_released_connection(self):
        pool = self.get_pool()
        c1 = pool.get_connection('_')
        pool.release(c1)
        c2 = pool.get_connection('_')
        assert_equals(c1,c2)

    def test_repr_contains_db_info_tcp(self):
        pool = BlockingConnectionPool(host='localhost', port=8888)
        assert_tuple_equal(
            re.match('(.*)<(.*)<(.*)>>', repr(pool)).groups(),
            (
                'BlockingConnectionPool',
                'Connection',
                'host=localhost,port=8888'
            )
        )
    
