#coding=utf-8
import re
import time
from nose.tools import (assert_equals, assert_dict_equal, assert_false,
                        assert_tuple_equal, assert_true, assert_is_none, raises)
from threading import Thread
from ssdb._compat import Queue
from ssdb.connection import Connection,ConnectionPool,BlockingConnectionPool
from ssdb.client import SSDB


class TestBatchCase(object):

    def setUp(self):
        pool = BlockingConnectionPool(
            connection_class=Connection,
            max_connections=2,
            timeout=5,
            host = '127.0.0.1',
            port = 8888)        
        self.client = SSDB(connection_pool=pool)
        #self.client = SSDB(host='127.0.0.1', port=8888)        
        print('set UP')
        
    def tearDown(self):
        print('tear down')
    
    def test_ttl(self):
        a = self.client.setx('set_test', 'defcwd', 5)
        assert_true(a)
        c = self.client.exists('set_test')
        assert_true(c)
        c = self.client.exists('set_test')
        assert_true(c)                
        time.sleep(5)
        c = self.client.exists('set_test')
        assert_false(c)        
