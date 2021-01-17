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

def define_classes(processes):
    """
    Define and return classes of unit tests given a configuration.
    """
    class Test_pool_close(TestCase):
        def test_pool_mapreduce_pool_close(self):
            pool = mr4mp.pool(processes, close=True)
            self.assertFalse(pool.closed())
            print("Starting.")
            start = default_timer()
            result = pool.mapreduce(index, merge, range(100), close=False)
            self.assertFalse(pool.closed())
            result = pool.mapreduce(index, merge, range(100))
            self.assertTrue(pool.closed())
            print("Finished in " + str(default_timer()-start) +
                  "s using " + str(len(pool)) + " processes.")
            self.assertEqual(type(result), dict)

        def test_pool_mapreduce_function_close(self):
            pool = mr4mp.pool(processes, close=False)
            self.assertFalse(pool.closed())
            print("Starting.")
            start = default_timer()
            result = pool.mapreduce(index, merge, range(100), close=False)
            self.assertFalse(pool.closed())
            result = pool.mapreduce(index, merge, range(100), close=True)
            self.assertTrue(pool.closed())
            print("Finished in " + str(default_timer()-start) +
                  "s using " + str(len(pool)) + " processes.")
            self.assertEqual(type(result), dict)

        def test_pool_mapreduce_pool_open_reuse(self):
            pool = mr4mp.pool(processes, close=False)
            result = pool.mapreduce(index, merge, range(100))
            result = pool.mapreduce(index, merge, range(100))
            result = pool.mapreduce(index, merge, range(100))
            self.assertFalse(pool.closed())
            pool.close()
            self.assertTrue(pool.closed())
            self.assertTrue(isinstance(result, dict))

        def test_pool_mapreduce_pool_close_reuse_exception(self):
            pool = mr4mp.pool(processes, close=True)
            result = pool.mapreduce(index, merge, range(100))
            with self.assertRaises(ValueError):
                result = pool.mapreduce(index, merge, range(100))

        def test_pool_mapreduce_function_close_reuse_exception(self):
            pool = mr4mp.pool(processes, close=False)
            result = pool.mapreduce(index, merge, range(100), close=True)
            with self.assertRaises(ValueError):
                result = pool.mapreduce(index, merge, range(100))

        def test_pool_mapreduce_many_with_as(self):
            with mr4mp.pool(processes) as pool:
                result = pool.mapreduce(index, merge, range(100))
                result = pool.mapreduce(index, merge, range(100))
                result = pool.mapreduce(index, merge, range(100))
                self.assertFalse(pool.closed())
            self.assertTrue(isinstance(result, dict))

    return (
        Test_pool_close
    )

# The instantiated test classes below are discovered and executed
# (e.g., using nosetests).
(Test_pool_close_one, Test_pool_close_many) =\
    define_classes(1), define_classes(2)

class Test_pool(TestCase):
    def test_pool_cpu_count(self):
        import multiprocessing as mp
        pool = mr4mp.pool()
        self.assertEqual(pool.cpu_count(), mp.cpu_count())
        pool.close()

    def test_pool_mapreduce_one(self):
        pool = mr4mp.pool(1, close=True)
        print("Starting.")
        start = default_timer()
        result = pool.mapreduce(index, merge, range(100))
        print("Finished in " + str(default_timer()-start) +
              "s using " + str(len(pool)) + " processes.")
        self.assertEqual(type(result), dict)

    def test_pool_mapreduce_one_stages(self):
        pool = mr4mp.pool(1, close=True)
        result = pool.mapreduce(index, merge, range(100), stages=4)
        self.assertEqual(type(result), dict)

    def test_pool_mapreduce(self):
        pool = mr4mp.pool(close=True)
        print("Starting.")
        start = default_timer()
        result = pool.mapreduce(index, merge, range(100))
        print("Finished in " + str(default_timer()-start) +
              "s using " + str(len(pool)) + " processes.")
        self.assertEqual(type(result), dict)

    def test_pool_mapreduce_stages(self):
        pool = mr4mp.pool(close=True)
        result = pool.mapreduce(index, merge, range(100), stages=4)
        self.assertEqual(type(result), dict)

    def test_pool_mapconcat_one(self):
        pool = mr4mp.pool(1, close=True)
        result = pool.mapconcat(add_one, range(0,100))
        self.assertEqual(list(result), list(range(1,101)))

    def test_pool_mapconcat_one_stages(self):
        pool = mr4mp.pool(1, close=True)
        result = pool.mapconcat(add_one, range(0,100), stages=4)
        self.assertEqual(list(result), list(range(1,101)))

    def test_pool_mapconcat(self):
        pool = mr4mp.pool(close=True)
        result = pool.mapconcat(add_one, range(0,100))
        self.assertEqual(list(result), list(range(1,101)))

    def test_pool_mapconcat_stages(self):
        pool = mr4mp.pool(close=True)
        result = pool.mapconcat(add_one, range(0,100), stages=4)
        self.assertEqual(list(result), list(range(1,101)))

class Test_mapconcat(TestCase):
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

class Test_mapreduce(TestCase):
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
