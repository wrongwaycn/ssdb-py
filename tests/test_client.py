#coding=utf-8
from nose.tools import (assert_equals, assert_dict_equal, assert_not_equals,
                        assert_tuple_equal, assert_true, assert_false,
                        assert_list_equal, assert_is_none, assert_items_equal,
                        raises)
import ssdb
from ssdb.connection import Connection,ConnectionPool,BlockingConnectionPool
from ssdb.client import SSDB


class TestClient(object):

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

    def test_get(self):
        a = self.client.set('get_test','321')
        assert_true(a)
        b = self.client.get('get_test')
        assert_equals(b,'321')
        c = self.client.delete('get_test')
        assert_true(c)
        d = self.client.get('get_none')
        assert_is_none(d)
    
    def test_set(self):
        a = self.client.set('set_test','123')
        assert_true(a)
        c = self.client.exists('set_test')
        assert_true(c)
        b = self.client.delete('set_test')
        assert_true(b)
        c = self.client.exists('set_test')
        assert_false(c)        

    @raises(ValueError)
    def test_incr(self):
        a = self.client.delete('incr0')
        assert_true(a)        
        a = self.client.set('incr0',10)
        assert_true(a)
        a = self.client.get('incr0')
        assert_equals(a,'10')
        a = self.client.incr('incr0',2)
        assert_equals(a,12)
        b = self.client.get('incr0')
        assert_equals(int(b),a)
        a = self.client.delete('incr0')
        assert_true(a)                
        c = self.client.incr('incr0', 0)

    @raises(ValueError)
    def test_decr(self):
        a = self.client.delete('decr0')
        assert_true(a)        
        a = self.client.set('decr0',10)
        assert_true(a)
        a = self.client.get('decr0')
        assert_equals(a,'10')
        a = self.client.decr('decr0',3)
        assert_equals(a,7)
        b = self.client.get('decr0')
        assert_equals(int(b),a)
        a = self.client.delete('decr0')
        assert_true(a)                
        c = self.client.decr('decr0', -2)

    def test_multi_set(self):
        params = {
            'aa':1,
            'bb':2,
            'cc':3,
            'dd':4,
        }
        a = self.client.multi_set(**params)
        assert_equals(a,4)
        b = self.client.get('aa')
        assert_equals(b,'1')
        b = self.client.get('bb')
        assert_equals(b,'2')
        b = self.client.get('cc')
        assert_equals(b,'3')
        b = self.client.get('dd')
        assert_equals(b,'4')                        
        d = self.client.delete('aa')
        assert_true(d)
        d = self.client.delete('bb')
        assert_true(d)        
        d = self.client.delete('cc')
        assert_true(d)        
        d = self.client.delete('dd')
        assert_true(d)

    def test_multi_get(self):
        params = {
            'aa': 'a1',
            'bb': 'b2',
            'cc': 'c3',
            'dd': 'd4',
        }
        a = self.client.multi_set(**params)
        assert_equals(a,len(params))        
        r = self.client.multi_get(*params.keys())
        assert_dict_equal(r,params)
        d = self.client.multi_delete(*params.keys())
        assert_equals(d,len(params))

    def test_keys(self):
        params = {
            'uuu0': 'a1',
            'uuu1': 'b2',
            'uuu2': 'c3',
            'uuu3': 'd4',
            'uuu4': 'e5',
            'uuu5': 'f6',
            'uuu6': 'g7',
            'uuu7': 'h8',            
        }
        a = self.client.multi_set(**params)
        assert_equals(a,len(params))
        b = self.client.keys('uuu ','uuuu',10)
        assert_items_equal(b,params.keys())
        d = self.client.multi_delete(*params.keys())
        assert_equals(d,len(params))        

    def test_scan(self):
        params = {
            'zzz0': 'a1',
            'zzz1': 'b2',
            'zzz2': 'c3',
            'zzz3': 'd4',
            'zzz4': 'e5',
            'zzz5': 'f6',
            'zzz6': 'g7',
            'zzz7': 'h8',            
        }
        a = self.client.multi_set(**params)
        assert_equals(a,len(params))
        b = self.client.scan('zzz ','zzzz',10)
        assert_dict_equal(b,params)
        d = self.client.multi_delete(*params.keys())
        assert_equals(d,len(params))

    def test_rscan(self):
        params = {
            'zzzz0': 'aa1',
            'zzzz1': 'bb2',
            'zzzz2': 'cc3',
            'zzzz3': 'dd4',
            'zzzz4': 'ee5',
            'zzzz5': 'ff6',
            'zzzz6': 'gg7',
            'zzzz7': 'hh8',            
        }
        a = self.client.multi_set(**params)
        assert_equals(a,len(params))
        b = self.client.rscan('zzzzz','zzzz ',10)
        assert_dict_equal(b,params)
        d = self.client.multi_delete(*params.keys())
        assert_equals(d,len(params))

    def test_hset(self):
        a = self.client.hset('test_hset', 'keya', 'abc123')
        assert_true(a)
        a = self.client.hset('test_hset', 'keyb', 'def456')
        assert_true(a)
        b = self.client.hget('test_hset', 'keya')
        assert_equals(b, 'abc123')
        b = self.client.hget('test_hset', 'keyb')
        assert_equals(b, 'def456')
        d = self.client.hclear('test_hset')
        assert_true(d)

    def test_hdel(self):
        a = self.client.hset('test_hdel', 'keya', 'abc123')
        assert_true(a)
        a = self.client.hset('test_hdel', 'keyb', 'def456')
        assert_true(a)
        e = self.client.hexists('test_hdel', 'keya')
        assert_true(e)
        e = self.client.hexists('test_hdel', 'keyb')
        assert_true(e)
        b = self.client.hexists('test_hdel', 'keyc')
        assert_false(b)                
        b = self.client.hdelete('test_hdel', 'keya')
        assert_true(b)
        b = self.client.hdelete('test_hdel', 'keyb')
        assert_true(b)
        b = self.client.hexists('test_hdel', 'keyb')
        assert_false(b)        
        c = self.client.hget('test_hdel', 'keya')
        assert_is_none(c)
        b = self.client.hget('test_hdel', 'keyb')
        assert_is_none(c)
        #d = self.client.hclear('test_hdel')
        #assert_false(d)

    def test_hincr(self):
        a = self.client.hset('test_counter', 'hincr', 100)
        assert_true(a)
        b = self.client.hincr('test_counter', 'hincr', 10)
        assert_equals(b, 110)
        b = self.client.hincr('test_counter', 'hincr')
        assert_equals(b, 111)
        b = self.client.hdecr('test_counter', 'hincr', 10)
        assert_equals(b, 101)
        b = self.client.hdecr('test_counter', 'hincr')
        assert_equals(b, 100)                
        d = self.client.hclear('test_counter')
        assert_true(d)

    def test_hsize(self):
        b = self.client.hsize('test_hsize')
        assert_equals(b, 0)
        a = self.client.hset('test_hsize', 'a', 'a1')
        assert_true(a)
        a = self.client.hset('test_hsize', 'b', 'b1')
        assert_true(a)
        a = self.client.hset('test_hsize', 'c', 'c1')
        assert_true(a)
        b = self.client.hsize('test_hsize')
        assert_equals(b, 3)
        d = self.client.hclear('test_hsize')
        assert_true(d)

    def test_hmulti(self):
        params = {
            'uuu0': 'a1',
            'uuu1': 'b2',
            'uuu2': 'c3',
            'uuu3': 'd4',
            'uuu4': 'e5',
            'uuu5': 'f6',
            'uuu6': 'g7',
            'uuu7': 'h8',            
        }        
        a = self.client.multi_hset('multi', **params)
        assert_equals(a,8)
        a1 = self.client.hget('multi', 'uuu0')
        assert_equals(a1, 'a1')
        b2 = self.client.hget('multi', 'uuu1')
        assert_equals(b2, 'b2')
        c3 = self.client.hget('multi', 'uuu2')
        assert_equals(c3, 'c3')
        d4 = self.client.hget('multi', 'uuu3')
        assert_equals(d4, 'd4')
        e5 = self.client.hget('multi', 'uuu4')
        assert_equals(e5, 'e5')
        f6 = self.client.hget('multi', 'uuu5')
        assert_equals(f6, 'f6')
        g7 = self.client.hget('multi', 'uuu6')
        assert_equals(g7, 'g7')
        h8 = self.client.hget('multi', 'uuu7')
        assert_equals(h8, 'h8')
        keys = self.client.hkeys('multi', 'uuu ', 'uuuu', 10)
        assert_items_equal(keys,params.keys())
        kvs = self.client.multi_hget('multi', 'uuu0', 'uuu7')
        assert_dict_equal(kvs,{
            "uuu0": 'a1',
            "uuu7": 'h8',
        })
        kvs = self.client.hscan('multi', 'uuu ', 'uuuu', 10)
        assert_dict_equal(kvs, params)
        kvs = self.client.hrscan('multi', 'uuu4', 'uuu0', 10)
        assert_dict_equal(kvs,{
            "uuu3": 'd4',
            "uuu2": 'c3',
            "uuu1": 'b2',
            "uuu0": 'a1',
        })
        r = self.client.multi_hget('multi', *params.keys())
        assert_dict_equal(r,params)
        d = self.client.multi_hdelete('multi', *params.keys())
        assert_equals(d,len(params))
        #d = self.client.hclear('multi')
        #assert_true(d)

    def test_hlist(self):
        params = {
            'hash_a': {
                'a': 1,
                'b': 2,
                'c': 3,
                'd': 4,
            },
            'hash_b': {
                'h': 11,
                'i': 12,
                'j': 13,
                'k': 14,
            },
            'hash_c': {
                'o': 21,
                'p': 22,
                'q': 23,
            },
            'hash_d': {
                'r': 31,
                's': 32,
                't': 33,
            },            
        }
        for k,v in params.items():
            a = self.client.multi_hset(k, **v)
            assert_equals(a,len(v))
        c = self.client.hlist('hash_ ', 'hash_z', 10)
        assert_items_equal(c,params.keys())
        for k,v in params.items():
            a = self.client.hclear(k)
            assert_true(a)
            
    def test_zset(self):
        params = {
            'zset_a': {
                'a': 1,
                'b': 2,
                'c': 3,
                'd': 4,
            },
            'zset_b': {
                'h': 11,
                'i': 12,
                'j': 13,
                'k': 14,
            },
            'zset_c': {
                'o': 21,
                'p': 22,
                'q': 23,
            },
            'zset_d': {
                'r': 31,
                's': 32,
                't': 33,
            },            
        }
        for k,v in params.items():
            a = self.client.multi_zset(k, **v)
            assert_equals(a, len(v))
        a = self.client.zlist('zset_ ', 'zset_z', 10)
        assert_items_equal(a,params.keys())
        a = self.client.zkeys('zset_b', 'h', 11, 20, 10)
        zset_b = params['zset_b'].copy()
        zset_b.pop('h')
        assert_items_equal(a,zset_b.keys())
        a = self.client.zscan('zset_a', 'a', 1, 3, 10)
        zset_a = params['zset_a'].copy()
        zset_a.pop('a')
        zset_a.pop('d')
        assert_dict_equal(a, zset_a)
        a = self.client.zrscan('zset_a', 'd', 4, 1, 10)
        zset_a['a'] = params['zset_a']['a']
        assert_dict_equal(a, zset_a)

        a = self.client.zrank('zset_a', 'a')
        assert_equals(a, 0)
        a = self.client.zrank('zset_a', 'b')
        assert_equals(a, 1)
        a = self.client.zrank('zset_a', 'c')
        assert_equals(a, 2)
        a = self.client.zrank('zset_a', 'd')
        assert_equals(a, 3)
        a = self.client.zrrank('zset_a', 'd')
        assert_equals(a, 0)
        a = self.client.zrrank('zset_a', 'c')
        assert_equals(a, 1)
        a = self.client.zrrank('zset_a', 'b')
        assert_equals(a, 2)
        a = self.client.zrrank('zset_a', 'a')
        assert_equals(a, 3)

        a = self.client.zrange('zset_b', 0, 2)
        zset_b = params['zset_b'].copy()
        zset_b.pop('j')
        zset_b.pop('k')
        assert_dict_equal(a, zset_b)

        a = self.client.zrange('zset_b', 2, 2)
        zset_b = params['zset_b'].copy()
        zset_b.pop('h')
        zset_b.pop('i')        
        assert_dict_equal(a, zset_b)

        a = self.client.zrrange('zset_b', 0, 2)
        zset_b = params['zset_b'].copy()
        zset_b.pop('h')
        zset_b.pop('i')        
        assert_dict_equal(a, zset_b)

        a = self.client.zrrange('zset_b', 2, 2)
        zset_b = params['zset_b'].copy()
        zset_b.pop('j')
        zset_b.pop('k')        
        assert_dict_equal(a, zset_b)                
        
        for k,v in params.items():
            a = self.client.multi_zget(k, *v.keys())
            assert_dict_equal(a, v)
        for k,v in params.items():
            d = self.client.multi_zdelete(k, *v.keys())
            assert_equals(d, len(v))
        for k,v in params['zset_a'].items():
            a = self.client.zset('zset_a', k, v)
            assert_true(a)
        for k,v in params['zset_a'].items():
            a = self.client.zget('zset_a', k)
            assert_equals(a,v)
        for k,v in params['zset_a'].items():
            a = self.client.zdelete('zset_a', k)
            assert_true(a)
        for k,v in params['zset_b'].items():
            a = self.client.zset('zset_b', k, v)
            assert_true(a)
        for k,v in params['zset_b'].items():
            a = self.client.zexists('zset_b', k)
            assert_true(a)
            a = self.client.zexists('zset_b', k+"1")
            assert_false(a)
        c = self.client.zsize('zset_b')
        assert_equals(c, len(params['zset_b']))
        c = self.client.zincr('zset_b', 'h', 3)
        assert_equals(c, params['zset_b']['h']+3)
        c = self.client.zdecr('zset_b', 'h', 5)
        assert_equals(c, params['zset_b']['h']-5+3)        
        d = self.client.zclear('zset_b')
        assert_true(d)
            
