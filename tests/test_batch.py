#coding=utf-8
import re
import time
from nose.tools import (assert_equals, assert_dict_equal, assert_not_equals,
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
    
    def test_pipeline(self):
        batch = self.client.batch()
        batch.set('test_set_a','a1')
        batch.set('test_set_b','b2')
        batch.set('test_set_c','c3')
        batch.set('test_set_d','d4')
        batch.hset('hset_a', 'a', 'a1')
        batch.hset('hset_a', 'b', 'b1')
        batch.hset('hset_a', 'c', 'c1')
        batch.hset('hset_a', 'd', 'd1')
        a1 = self.client.get('test_set_a')
        assert_is_none(a1)
        batch.execute()
        a1 = self.client.get('test_set_a')
        assert_equals(a1,'a1')
        b2 = self.client.get('test_set_b')
        assert_equals(b2,'b2')
        c3 = self.client.get('test_set_c')
        assert_equals(c3,'c3')
        d4 = self.client.get('test_set_d')
        assert_equals(d4,'d4')
        e5 = self.client.get('test_set_e')
        assert_is_none(e5)
        a1 = self.client.hget('hset_a', 'a')
        assert_equals(a1, 'a1')
        b1 = self.client.hget('hset_a', 'b')
        assert_equals(b1, 'b1')
        c1 = self.client.hget('hset_a', 'c')
        assert_equals(c1, 'c1')
        d1 = self.client.hget('hset_a', 'd')
        assert_equals(d1, 'd1')                
        d = self.client.multi_delete('test_set_a', 'test_set_b', 'test_set_c',
                                     'test_set_d')
        d = self.client.hclear('hset_a')
        assert_true(d)
        
