#coding=utf-8
import time
from nose.tools import assert_equals, assert_list_equal, with_setup, raises
import ssdb
from ssdb.connection import Connection


class TestConnection(object):
    
    def setUp(self):
        print('set UP')
        self.connection = Connection()
        
    def tearDown(self):
        print('tear down')
        self.connection.disconnect()

    @raises(ssdb.ConnectionError)        
    def test_on_connect_error(self):
        """
        An error in Connection.on_connect should disconnect from the server
        """
        # this assumed the ssdb server being tested against doesn't use 1023
        # port. An error should be raised on connect        
        bad_connection = Connection(port=1023)
        bad_connection.connect()

    def test_init(self):
        print('init')
        assert_equals(self.connection.host,'127.0.0.1')
        assert_equals(self.connection.port,8888)
        tmp_ssdb =  Connection(host='localhost',port=9999)
        assert_equals(tmp_ssdb.host,'localhost')
        assert_equals(tmp_ssdb.port,9999)
        
    def test_pack_command(self):
        print('')
        output = self.connection.pack_command('set','a','hi')
        assert_equals(output,"3\nset\n1\na\n2\nhi\n\n")

    def test_connect(self):
        self.connection.connect()
        print(self.connection._sock)
        assert_equals(1,1)

    def test_send(self):
        self.connection.connect()
        print(self.connection._sock)
        assert_equals(2,2)

        
    def test_set(self):
        self.connection.connect()

        #----------------------set a hi------------------------
        self.connection.send_command('set','set_value','hi')
        p = self.connection.read_response()
        assert_list_equal(p,['ok','1'])

        self.connection.send_command('get','set_value')
        p = self.connection.read_response()
        assert_list_equal(p,['ok','hi'])

        self.connection.send_command('set','set_value','')
        p = self.connection.read_response()
        assert_list_equal(p,['ok','1'])

        self.connection.send_command('get','set_value')
        p = self.connection.read_response()
        assert_list_equal(p,['ok',''])        

        self.connection.send_command('del','set_value')
        p = self.connection.read_response()
        assert_list_equal(p,['ok','1'])

        self.connection.send_command('del','set_value')
        p = self.connection.read_response()
        assert_list_equal(p,['ok','1'])

        
    def test_ttl_expire(self):
        self.connection.connect()

        self.connection.send_command('set','expire_value','expire')
        p = self.connection.read_response()
        assert_list_equal(p,['ok','1'])

        ## self.connection.send_command('ttl','expire_value')
        ## p = self.connection.read_response()
        ## assert_list_equal(p,['ok','-1'])                

        self.connection.send_command('get','expire_value')
        p = self.connection.read_response()
        assert_list_equal(p,['ok','expire'])        
        
        self.connection.send_command('expire','expire_value','3')
        p = self.connection.read_response()
        assert_list_equal(p,['ok','1'])

        self.connection.send_command('ttl','expire_value')
        p = self.connection.read_response()
        assert_list_equal(p,['ok','3'])

        self.connection.send_command('get','expire_value')
        p = self.connection.read_response()
        assert_list_equal(p,['ok','expire'])

        self.connection.send_command('expire','expire_value_not_exist','3')
        p = self.connection.read_response()
        assert_list_equal(p,['ok','0'])

        self.connection.send_command('del','expire_value')
        p = self.connection.read_response()
        assert_list_equal(p,['ok','1'])        

        ## time.sleep(4)        
        
        ## self.connection.send_command('get','expire_value')
        ## p = self.connection.read_response()
        ## assert_list_equal(p,['not_found',''])

        ## self.connection.send_command('ttl','expire_value')
        ## p = self.connection.read_response()
        ## assert_list_equal(p,['ok','-1'])

        ## self.connection.send_command('ttl','expire_value_not_exist')
        ## p = self.connection.read_response()
        ## assert_list_equal(p,['ok','-1'])

    def test_getset(self):
        self.connection.connect()

        self.connection.send_command('getset','getset_test','test')
        p = self.connection.read_response()
        assert_list_equal(p,['not_found',''])

        self.connection.send_command('set','getset_test','no value')
        p = self.connection.read_response()
        assert_list_equal(p,['ok','1'])

        self.connection.send_command('getset','getset_test','values')
        p = self.connection.read_response()
        assert_list_equal(p,['ok','no value'])

        self.connection.send_command('getset','getset_test','new value')
        p = self.connection.read_response()
        assert_list_equal(p,['ok','values'])

        self.connection.send_command('del','getset_test')
        p = self.connection.read_response()
        assert_list_equal(p,['ok','1'])

    def test_setnx(self):
        self.connection.connect()

        self.connection.send_command('setnx','setnx_test','test')
        p = self.connection.read_response()
        assert_list_equal(p,['ok','1'])

        self.connection.send_command('setnx','setnx_test','test2')
        p = self.connection.read_response()
        assert_list_equal(p,['ok','0'])

        self.connection.send_command('get','setnx_test')
        p = self.connection.read_response()
        assert_list_equal(p,['ok','test'])

        self.connection.send_command('del','setnx_test')
        p = self.connection.read_response()
        assert_list_equal(p,['ok','1'])

        
    def test_incr(self):
        self.connection.connect()
        #----------------------incr test 1------------------------
        self.connection.send_command('set','incr_test','100')
        p = self.connection.read_response()
        assert_list_equal(p,['ok','1'])
                
        self.connection.send_command('incr','incr_test','1')
        p = self.connection.read_response()
        assert_list_equal(p,['ok','101'])

        self.connection.send_command('incr','incr_test','-10')
        p = self.connection.read_response()
        assert_list_equal(p,['ok','91'])        

        self.connection.send_command('del','incr_test')
        p = self.connection.read_response()
        assert_list_equal(p,['ok','1'])

        
    def test_bit(self):
        self.connection.connect()
        #----------------------bit test------------------------
        self.connection.send_command('set','bit_test','1')
        p = self.connection.read_response()
        assert_list_equal(p,['ok','1'])
                
        self.connection.send_command('setbit','bit_test','4','0')
        p = self.connection.read_response()
        assert_list_equal(p,['ok','1'])

        self.connection.send_command('getbit','bit_test','4')
        p = self.connection.read_response()
        assert_list_equal(p,['ok','0'])        

        self.connection.send_command('get','bit_test')
        p = self.connection.read_response()
        assert_list_equal(p,['ok','!'])

        self.connection.send_command('set','bit_test','1234567890')
        p = self.connection.read_response()
        assert_list_equal(p,['ok','1'])

        self.connection.send_command('countbit','bit_test','0','1')
        p = self.connection.read_response()
        assert_list_equal(p,['ok','3'])

        self.connection.send_command('countbit','bit_test','3','-3')
        p = self.connection.read_response()
        assert_list_equal(p,['ok','16'])                        

        self.connection.send_command('del','bit_test')
        p = self.connection.read_response()
        assert_list_equal(p,['ok','1'])


        
