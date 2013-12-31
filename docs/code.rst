Code
====

.. automodule:: ssdb.client

   Client
   ------
   .. autoclass:: StrictSSDB


   Key/Value 
   ^^^^^^^^^

   .. code-block:: python

      >>> from ssdb.client import SSDB
      >>> ssdb = SSDB()
      >>> ssdb.multi_set(set_a='a', set_b='b', set_c='c', set_d='d')
      >>> ssdb.multi_set(set_abc='abc', set_count=10)

   get
   """
   .. automethod:: StrictSSDB.get

   set
   """
   .. automethod:: StrictSSDB.set

   delete
   """"""
   .. automethod:: StrictSSDB.delete

   exists
   """"""
   .. automethod:: StrictSSDB.exists

   incr
   """"
   .. automethod:: StrictSSDB.incr

   decr
   """"
   .. automethod:: StrictSSDB.decr

   multi_set
   """""""""
   .. automethod:: StrictSSDB.multi_set

   multi_get
   """""""""
   .. automethod:: StrictSSDB.multi_get


   Hash
   ^^^^

   .. automethod:: StrictSSDB.hget
   .. automethod:: StrictSSDB.hset
   .. automethod:: StrictSSDB.hdelete

   
   Zset
   ^^^^

   .. automethod:: StrictSSDB.zget
   .. automethod:: StrictSSDB.zset
   .. automethod:: StrictSSDB.zdelete
