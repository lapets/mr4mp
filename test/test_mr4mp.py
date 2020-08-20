from random import choice
from string import ascii_lowercase
from timeit import default_timer
from unittest import TestCase
import mr4mp

def word():
    """Create a random seven-letter word."""
    return ''.join(choice(ascii_lowercase) for _ in range(7))

def index(id):
    """Given an index value, make 100 words that map to it."""
    return {w:{id} for w in {word() for _ in range(100)}}

def merge(i, j):
    """Merge two word counts."""
    return {k:(i.get(k,set()) | j.get(k,set())) for k in i.keys() | j.keys()}

def add_one(x):
    return [x + 1]

class TestPool(TestCase):
    def test_pool_cpu_count(self):
        import multiprocessing as mp
        pool = mr4mp.pool()
        self.assertEqual(pool.cpu_count(), mp.cpu_count())

    def test_pool_mapreduce_one(self):
        pool = mr4mp.pool(1)
        print("Starting.")
        start = default_timer()
        result = pool.mapreduce(index, merge, range(100))
        print("Finished in " + str(default_timer()-start) +
              "s using " + str(len(pool)) + " processes.")
        self.assertEqual(type(result), dict)

    def test_pool_mapreduce_one_stages(self):
        pool = mr4mp.pool(1)
        result = pool.mapreduce(index, merge, range(100), stages=4)
        self.assertEqual(type(result), dict)

    def test_pool_mapreduce(self):
        pool = mr4mp.pool()
        print("Starting.")
        start = default_timer()
        result = pool.mapreduce(index, merge, range(100))
        print("Finished in " + str(default_timer()-start) +
              "s using " + str(len(pool)) + " processes.")
        self.assertEqual(type(result), dict)

    def test_pool_mapreduce_stages(self):
        pool = mr4mp.pool()
        result = pool.mapreduce(index, merge, range(100), stages=4)
        self.assertEqual(type(result), dict)

    def test_pool_mapconcat_one(self):
        pool = mr4mp.pool(1)
        result = pool.mapconcat(add_one, range(0,100))
        self.assertEqual(list(result), list(range(1,101)))

    def test_pool_mapconcat_one_stages(self):
        pool = mr4mp.pool(1)
        result = pool.mapconcat(add_one, range(0,100), stages=4)
        self.assertEqual(list(result), list(range(1,101)))

    def test_pool_mapconcat(self):
        pool = mr4mp.pool()
        result = pool.mapconcat(add_one, range(0,100))
        self.assertEqual(list(result), list(range(1,101)))

    def test_pool_mapconcat_stages(self):
        pool = mr4mp.pool()
        result = pool.mapconcat(add_one, range(0,100), stages=4)
        self.assertEqual(list(result), list(range(1,101)))

class TestMapConcat(TestCase):
    def test_mapconcat_one(self):
        results = mr4mp.mapconcat(add_one, range(0,100), processes=1)
        self.assertEqual(list(results), list(range(1,101)))

    def test_mapconcat_one_stages(self):
        results = mr4mp.mapconcat(add_one, range(0,100), processes=1, stages=4)
        self.assertEqual(list(results), list(range(1,101)))

    def test_mapconcat(self):
        results = mr4mp.mapconcat(add_one, range(0,100))
        self.assertEqual(list(results), list(range(1,101)))

    def test_mapconcat_stages(self):
        results = mr4mp.mapconcat(add_one, range(0,100), stages=4)
        self.assertEqual(list(results), list(range(1,101)))

class TestMapReduce(TestCase):
    def test_mapreduce_one(self):
        result = mr4mp.mapreduce(index, merge, range(100), processes=1)
        self.assertEqual(type(result), dict)

    def test_mapreduce_one_stages(self):
        result = mr4mp.mapreduce(index, merge, range(100), processes=1, stages=4)
        self.assertEqual(type(result), dict)

    def test_mapreduce(self):
        result = mr4mp.mapreduce(index, merge, range(100))
        self.assertEqual(type(result), dict)

    def test_mapreduce_stages(self):
        result = mr4mp.mapreduce(index, merge, range(100), stages=4)
        self.assertEqual(type(result), dict)
