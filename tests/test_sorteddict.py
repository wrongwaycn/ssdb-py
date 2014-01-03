#coding=utf-8
import re
import time
from nose.tools import (assert_equals, assert_dict_equal, assert_false,
                        assert_tuple_equal, assert_true, assert_is_none, raises)
from ssdb.utils import SortedDict


class TestSortedDict(object):

    def setUp(self):
        print('set UP')
        
    def tearDown(self):
        print('tear down')
    
    def test_sorteddict(self):
        keys = ['a', 'b', 'c', 'd', 'e', 'f', 'g']
        sd = SortedDict()
        for i in range(len(keys)):
            sd[keys[i]] = i+1
        index = 0
        for k,v in sd.items():
            assert_equals(k, keys[index])
            assert_equals(v, index+1)
            index += 1
