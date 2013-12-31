#coding=utf-8
from contextlib import contextmanager

@contextmanager
def batch(ssdb_obj):
    b = ssdb_obj.batch()
    yield b
    b.execute()

pipeline = batch
