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

class log():
    """Log of progress function outputs."""
    def __init__(self):
        self.logged = []

    def __call__(self, xs):
        self.logged = xs
        return xs
        
    def to_list(self):
        return list(sorted([x for xs in self.logged for x in xs]))

def define_classes_pool_close(processes):
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

    return Test_pool_close

def define_classes_pool_stages_progress(processes, stages, progress):
    """
    Define and return classes of unit tests given a configuration.
    """
    class Test_pool_stages_progress(TestCase):
        def test_pool_mapreduce(self):
            logger = log() if progress else None
            pool = mr4mp.pool(processes, close=True)
            result = pool.mapreduce(index, merge, range(100), stages=stages, progress=logger)
            self.assertEqual(type(result), dict)
            if progress:
                self.assertEqual(
                    logger.to_list(),
                    list(range(100)) if stages is not None else []
                )

        def test_pool_mapconcat(self):
            logger = log() if progress else None
            pool = mr4mp.pool(processes, close=True)
            result = pool.mapconcat(add_one, range(0,100), stages=stages, progress=logger)
            self.assertEqual(list(result), list(range(1,101)))
            if progress:
                self.assertEqual(
                    logger.to_list(),
                    list(range(100)) if stages is not None else []
                )

    return Test_pool_stages_progress

class Test_pool(TestCase):
    def test_pool_cpu_count(self):
        import multiprocessing as mp
        pool = mr4mp.pool()
        self.assertEqual(pool.cpu_count(), mp.cpu_count())
        pool.close()

    def test_pool_mapreduce(self):
        pool = mr4mp.pool(close=True)
        print("Starting.")
        start = default_timer()
        result = pool.mapreduce(index, merge, range(100))
        print("Finished in " + str(default_timer()-start) +
              "s using " + str(len(pool)) + " processes.")
        self.assertEqual(type(result), dict)

# The instantiated test classes below are discovered in the local scope
# and executed by the unit testing framework (e.g., using nosetests).
vars = locals()
for processes in (1, 2):
    name = 'Test_pool_close_' + str(processes)
    vars[name] = define_classes_pool_close(processes)
for processes in (1, 2):
    for stages in (None, 4):
        for progress in (False, True):
            name =\
                'Test_pool_stages_progress_' +\
                "_".join(map(str, [processes, stages, progress]))
            vars[name] = define_classes_pool_stages_progress(processes, stages, progress)

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
