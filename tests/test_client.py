#coding=utf-8
import math
from nose.tools import (assert_equals, assert_dict_equal, assert_not_equals,
                        assert_tuple_equal, assert_true, assert_false,
                        assert_list_equal, assert_is_none, assert_items_equal,
                        assert_almost_equal, raises)
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

    def test_getset(self):
        self.client.delete('getset_test')
        a = self.client.getset('getset_test','abc')
        assert_is_none(a)
        b = self.client.get('getset_test')
        assert_equals(b,'abc')                
        c = self.client.set('getset_test','abc')
        assert_true(c)
        d = self.client.getset('getset_test','defg')
        assert_equals(d,'abc')
        e = self.client.getset('getset_test','hijk')
        assert_equals(e,'defg')        
        f = self.client.delete('getset_test')
        assert_true(f)
        g = self.client.exists('getset_test')
        assert_false(g)

    def test_setnx(self):
        self.client.delete('setnx_test')
        a = self.client.setnx('setnx_test','abc')
        assert_true(a)
        b = self.client.get('setnx_test')
        assert_equals(b,'abc')                
        c = self.client.setnx('setnx_test','def')
        assert_false(c)
        f = self.client.delete('setnx_test')
        assert_true(f)
        g = self.client.exists('setnx_test')
        assert_false(g)

    def test_bit(self):
        self.client.delete('bit_test')
        self.client.set('bit_test',1)
        a = self.client.countbit('bit_test')
        assert_equals(a,3)
        a = self.client.setbit('bit_test', 1, 1)
        assert_false(a)
        a = self.client.getbit('bit_test', 1)
        assert_true(a)        
        b = self.client.get('bit_test')
        assert_equals(b,'3')                
        c = self.client.setbit('bit_test', 2, 1)
        assert_false(c)        
        b = self.client.get('bit_test')
        assert_equals(b,'7')
        c = self.client.setbit('bit_test', 2, 0)
        assert_true(c)
        c = self.client.getbit('bit_test', 2)
        assert_false(c)
        c = self.client.set('bit_test', '1234567890')
        c = self.client.countbit('bit_test', 0, 1)
        assert_equals(c,3)
        c = self.client.countbit('bit_test', 3, -3)
        assert_equals(c,16)        
        f = self.client.delete('bit_test')
        assert_true(f)

    def test_str(self):
        self.client.delete('str_test')
        self.client.set('str_test',"abc12345678")
        a = self.client.substr('str_test', 2, 4)
        assert_equals(a, "c123")
        a = self.client.substr('str_test', -2, 2)
        assert_equals(a, "78")
        a = self.client.substr('str_test', 1, -1)
        assert_equals(a, "bc1234567")
        a = self.client.strlen('str_test')
        assert_equals(a, 11)                
        f = self.client.delete('str_test')
        assert_true(f)

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
        a = self.client.incr('incr0',-2)
        assert_equals(a,10)        
        b = self.client.get('incr0')
        assert_equals(int(b),a)
        a = self.client.delete('incr0')
        assert_true(a)                
        c = self.client.incr('incr0', 'abc')

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
        d = self.client.multi_del(*params.keys())
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
        d = self.client.multi_del(*params.keys())
        assert_equals(d,len(params))        

    def test_scan(self):
        keys = [
            'zzz0',
            'zzz1',
            'zzz2',
            'zzz3',
            'zzz4',
            'zzz5',
            'zzz6',
            'zzz7'
        ]
        values = [
            'a1',
            'b2',
            'c3',
            'd4',
            'e5',
            'f6',
            'g7',
            'h8'
        ]
        params = {}
        for i in range(len(keys)):
            params[keys[i]] = values[i]
        a = self.client.multi_set(**params)
        assert_equals(a,len(params))
        b = self.client.scan('zzz ','zzzz',10)
        assert_dict_equal(b,params)
        index = 0
        for k,v in b.items():
            assert_equals(k, keys[index])
            assert_equals(v, values[index])
            index += 1
        d = self.client.multi_del(*params.keys())
        assert_equals(d,len(params))

    def test_rscan(self):
        keys = [
            'zzzz0',
            'zzzz1',
            'zzzz2',
            'zzzz3',
            'zzzz4',
            'zzzz5',
            'zzzz6',
            'zzzz7'
        ]
        values = [
            'aa1',
            'bb2',
            'cc3',
            'dd4',
            'ee5',
            'ff6',
            'gg7',
            'hh8'
        ]
        params = {}
        for i in range(len(keys)):
            params[keys[i]] = values[i]        
        a = self.client.multi_set(**params)
        assert_equals(a,len(params))
        b = self.client.rscan('zzzzz','zzzz ',10)
        assert_dict_equal(b,params)
        index = 0
        c = len(keys)
        for k,v in b.items():
            assert_equals(k, keys[c-index-1])
            assert_equals(v, values[c-index-1])
            index += 1        
        d = self.client.multi_del(*params.keys())
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
        b = self.client.hdel('test_hdel', 'keya')
        assert_true(b)
        b = self.client.hdel('test_hdel', 'keyb')
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
        self.client.hclear('test_counter')        
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

    def test_hgetall(self):
        self.client.hclear('test_hgetall')        
        self.client.delete('test_hgetall')
        dct = {
            'a':"AA",
            'b':"BB",
            'c':"CC",
            'd':"DD"
        }
        a = self.client.multi_hset('test_hgetall', **dct)
        assert_equals(a,4)
        a = self.client.hgetall('test_hgetall')
        assert_dict_equal(a,dct)
        b = self.client.delete('test_hgetall')
        d = self.client.hclear('test_hgetall')
        assert_true(d)
        self.client.delete('test_hgetall')

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
        d = self.client.multi_hdel('multi', *params.keys())
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
            a = self.client.hclear(k)

        for k,v in params.items():
            a = self.client.multi_hset(k, **v)
            assert_equals(a,len(v))
        c = self.client.hlist('hash_ ', 'hash_z', 10)
        assert_items_equal(c,params.keys())

        lst = ['hash_a','hash_b','hash_c','hash_d']
        for index,item in enumerate(c):
            assert_equals(item,lst[index])
        c = self.client.hrlist('hash_z', 'hash_ ', 10)
        lst.reverse()
        for index,item in enumerate(c):
            assert_equals(item,lst[index])
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
            d = self.client.multi_zdel(k, *v.keys())
            assert_equals(d, len(v))
        for k,v in params['zset_a'].items():
            a = self.client.zset('zset_a', k, v)
            assert_true(a)
        for k,v in params['zset_a'].items():
            a = self.client.zget('zset_a', k)
            assert_equals(a,v)
        for k,v in params['zset_a'].items():
            a = self.client.zdel('zset_a', k)
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
            
    def test_zset(self):
        zset_1 = {
            'a': 30,
            'b': 20,
            'c': 100,
            'd': 1,
            'e': 64,
            'f': -3,
            'g': 0
        }
        self.client.zclear('zset_1')
        self.client.delete('zset_1')
        a = self.client.multi_zset('zset_1', **zset_1)
        assert_equals(a, len(zset_1))
        b = self.client.zcount('zset_1', 20, 70)
        assert_equals(b, 3)
        c = self.client.zcount('zset_1', 0, 100)
        assert_equals(c, 6)
        d = self.client.zcount('zset_1', 2, 3)
        assert_equals(d, 0)

        b = self.client.zsum('zset_1', 20, 70)
        assert_equals(b, 114)
        c = self.client.zsum('zset_1', 0, 100)
        assert_equals(c, 215)
        d = self.client.zsum('zset_1', 2, 3)
        assert_equals(d, 0)

        b = self.client.zavg('zset_1', 20, 70)
        assert_equals(b, 38.0)
        c = self.client.zavg('zset_1', 0, 100)
        assert_equals(round(abs(c-215.0/6),4),0)
        d = self.client.zavg('zset_1', 2, 3)
        assert_true(math.isnan(float('nan')))

        b = self.client.zremrangebyrank('zset_1', 0, 2)
        assert_equals(b, 3)
        b = self.client.zremrangebyrank('zset_1', 1, 2)
        assert_equals(b, 2)

        a = self.client.multi_zset('zset_1', **zset_1)
        b = self.client.zremrangebyscore('zset_1', 20, 70)
        assert_equals(b, 3)
        b = self.client.zremrangebyscore('zset_1', 0, 100)
        assert_equals(b, 3)
                        
        self.client.zclear('zset_1')
        self.client.delete('zset_1')

    def test_queue(self):
        self.client.qclear('queue_1')
        self.client.qclear('queue_2')
        queue_1 = ['a','b','c','d','e','f','g']
        queue_2 = ['test1','test2','test3','test4','test5','test6']
        #qpush
        a = self.client.qpush('queue_1',*queue_1)
        assert_equals(a,len(queue_1))
        a = self.client.qpush('queue_2',*queue_2)
        assert_equals(a,len(queue_2))
        
        #qsize
        a = self.client.qsize('queue_1')
        assert_equals(a,len(queue_1))

        #qlist
        a = self.client.qlist('queue_1', 'queue_2', 10)
        assert_equals(a,['queue_2'])
        a = self.client.qlist('queue_', 'queue_2', 10)
        assert_equals(a,['queue_1', 'queue_2'])
        a = self.client.qlist('z', '', 10)
        assert_equals(a,[])

        #qrlist
        a = self.client.qrlist('queue_2', 'queue_1', 10)
        assert_equals(a,['queue_1'])
        a = self.client.qrlist('queue_z', 'queue_', 10)
        assert_equals(a,['queue_2', 'queue_1'])
        a = self.client.qrlist('z', '', 10)
        assert_equals(a,['queue_2', 'queue_1'])

        #qfront
        a = self.client.qfront('queue_1')
        assert_equals(a,'a')

        #qback
        a = self.client.qback('queue_1')
        assert_equals(a,'g')

        #qget
        a = self.client.qget('queue_1',2)
        assert_equals(a,'c')
        a = self.client.qget('queue_1',0)
        assert_equals(a,'a')
        a = self.client.qget('queue_1',-1)
        assert_equals(a,'g')

        #qset
        a = self.client.qset('queue_1',0,'aaa')
        a = self.client.qget('queue_1',0)
        assert_equals(a,'aaa')
        a = self.client.qset('queue_1',0,'a')

        #qrange
        a = self.client.qrange('queue_1', 2, 2)
        assert_list_equal(a,['c','d'])
        a = self.client.qrange('queue_1', 2, 10)
        assert_list_equal(a,['c','d','e','f','g'])
        a = self.client.qrange('queue_1', -1, 1)
        assert_list_equal(a,['g'])

        #qslice
        a = self.client.qslice('queue_1', 2, 2)
        assert_list_equal(a,['c'])
        a = self.client.qslice('queue_1', 2, 3)
        assert_list_equal(a,['c','d'])
        a = self.client.qslice('queue_1', 2, 10)
        assert_list_equal(a,['c','d','e','f','g'])
        a = self.client.qslice('queue_1', -3, 5)
        assert_list_equal(a,['e','f'])

        #qpush
        a = self.client.qpush_back('queue_1','h')
        assert_equals(a,8)
        a = self.client.qpop_back('queue_1')
        assert_list_equal(a,['h'])
        a = self.client.qpush_back('queue_1','h','i','j','k')
        assert_equals(a,11)
        a = self.client.qpop_back('queue_1',4)
        assert_list_equal(a,['k','j','i','h'])

        a = self.client.qpush('queue_1','h')
        assert_equals(a,8)
        a = self.client.qpop_back('queue_1')
        assert_list_equal(a,['h'])
        a = self.client.qpush('queue_1','h','i','j','k')
        assert_equals(a,11)
        a = self.client.qpop_back('queue_1',4)
        assert_list_equal(a,['k','j','i','h'])        

        a = self.client.qpush_front('queue_1','0')
        assert_equals(a,8)
        a = self.client.qpop_front('queue_1')
        assert_list_equal(a,['0'])
        a = self.client.qpush_front('queue_1','0','1','2','3')
        assert_equals(a,11)
        a = self.client.qpop_front('queue_1',4)
        assert_list_equal(a,['3','2','1','0'])

        a = self.client.qpush_front('queue_1','0')
        assert_equals(a,8)
        a = self.client.qpop('queue_1')
        assert_list_equal(a,['0'])
        a = self.client.qpush_front('queue_1','0','1','2','3')
        assert_equals(a,11)
        a = self.client.qpop('queue_1',4)
        assert_list_equal(a,['3','2','1','0'])

        #qrem_front
        a = self.client.qrem_front('queue_1',3)
        assert_equals(a,3)
        a = self.client.qpop('queue_1',10)
        assert_list_equal(a,['d','e','f','g'])
        a = self.client.qpush('queue_1',*queue_1)
        assert_equals(a,len(queue_1))
        
        #qrem_back
        a = self.client.qrem_back('queue_1',3)
        assert_equals(a,3)
        a = self.client.qpop('queue_1',10)
        assert_list_equal(a,['a','b','c','d'])
        a = self.client.qpush('queue_1',*queue_1)
        assert_equals(a,len(queue_1))               
        
        #qpop
        a = self.client.qpop('queue_1',len(queue_1))
        assert_list_equal(a,queue_1)
        
        #qclear
        a = self.client.qclear('queue_2')
        assert_equals(a, 6)
        b = self.client.qclear('queue_1')
        assert_equals(b, 0)
