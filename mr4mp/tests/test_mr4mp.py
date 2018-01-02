from random import choice
from string import ascii_lowercase
from timeit import default_timer
from unittest import TestCase
import mr4mp

def word():
    return ''.join(choice(ascii_lowercase) for _ in range(7))

def index(id):
    return {w:{id} for w in {word() for _ in range(100)}}

def merge(i, j):
    return {k:(i.get(k,set()) | j.get(k,set())) for k in i.keys() | j.keys()}

class TestPool(TestCase):
    def test_mapreduce(self):
        pool = mr4mp.pool()
        print("Starting.")
        start = default_timer()
        result = pool.mapreduce(index, merge, range(100))
        print("Finished in " + str(default_timer()-start) +
              "s using " + str(len(pool)) + " processes.")
        self.assertEqual(type(result), dict)

## eof