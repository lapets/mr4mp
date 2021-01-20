from importlib import import_module
from string import ascii_lowercase
from hashlib import sha256
from functools import reduce
from timeit import default_timer
import multiprocessing as mp
from unittest import TestCase

import mr4mp.mr4mp

def api_methods():
    """
    API symbols that should be available to users upon module import.
    """
    return {'pool', 'mapreduce', 'mapconcat'}

class Test_namespace(TestCase):
    """
    Check that the exported namespace provide access to the expected
    classes and functions.
    """
    def test_module(self):
        module = import_module('mr4mp.mr4mp')
        self.assertTrue(api_methods().issubset(module.__dict__.keys()))

def word(id, k):
    """Create a random three-character word."""
    return ''.join(ascii_lowercase[i % 7] for i in sha256(bytes(id * k)).digest()[:3])

def index(id):
    """Given an index value, make 25 words that map to it."""
    return {w:{id} for w in {word(id, k) for k in range(25)}}

def merge(i, j):
    """Merge two word counts."""
    return {k:(i.get(k,set()) | j.get(k,set())) for k in i.keys() | j.keys()}

result_reference = reduce(merge, map(index, range(50)))

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

def define_class_pool_close(processes):
    """
    Define and return class of pool unit tests for the supplied pool closing
    behavior configuration.
    """
    class Test_pool_close(TestCase):
        def test_pool_mapreduce_pool_close(self):
            pool = mr4mp.pool(processes, close=True)
            self.assertFalse(pool.closed())
            print("Starting.")
            start = default_timer()
            result = pool.mapreduce(index, merge, range(50), close=False)
            self.assertFalse(pool.closed())
            result = pool.mapreduce(index, merge, range(50))
            self.assertTrue(pool.closed())
            print("Finished in " + str(default_timer()-start) +
                  "s using " + str(len(pool)) + " processes.")
            self.assertEqual(result, result_reference)

        def test_pool_mapreduce_function_close(self):
            pool = mr4mp.pool(processes, close=False)
            self.assertFalse(pool.closed())
            print("Starting.")
            start = default_timer()
            result = pool.mapreduce(index, merge, range(50), close=False)
            self.assertFalse(pool.closed())
            result = pool.mapreduce(index, merge, range(50), close=True)
            self.assertTrue(pool.closed())
            print("Finished in " + str(default_timer()-start) +
                  "s using " + str(len(pool)) + " processes.")
            self.assertEqual(result, result_reference)

        def test_pool_mapreduce_pool_open_reuse(self):
            pool = mr4mp.pool(processes, close=False)
            result = pool.mapreduce(index, merge, range(50))
            result = pool.mapreduce(index, merge, range(50))
            result = pool.mapreduce(index, merge, range(50))
            self.assertFalse(pool.closed())
            pool.close()
            self.assertTrue(pool.closed())
            self.assertEqual(result, result_reference)

        def test_pool_mapreduce_pool_close_reuse_exception(self):
            pool = mr4mp.pool(processes, close=True)
            result = pool.mapreduce(index, merge, range(50))
            with self.assertRaises(ValueError):
                result = pool.mapreduce(index, merge, range(50))

        def test_pool_mapreduce_function_close_reuse_exception(self):
            pool = mr4mp.pool(processes, close=False)
            result = pool.mapreduce(index, merge, range(50), close=True)
            with self.assertRaises(ValueError):
                result = pool.mapreduce(index, merge, range(50))

        def test_pool_mapreduce_many_with_as(self):
            with mr4mp.pool(processes) as pool:
                result = pool.mapreduce(index, merge, range(50))
                result = pool.mapreduce(index, merge, range(50))
                result = pool.mapreduce(index, merge, range(50))
                self.assertFalse(pool.closed())
            self.assertEqual(result, result_reference)

    return Test_pool_close

def define_class_pool_stages_progress(processes, stages, progress):
    """
    Define and return class of pool unit tests for the supplied stage quantity
    and progress function configuration.
    """
    class Test_pool_stages_progress(TestCase):
        def test_pool_mapreduce(self):
            logger = log() if progress else None
            pool = mr4mp.pool(processes, close=True)
            result = pool.mapreduce(index, merge, range(50), stages=stages, progress=logger)
            self.assertEqual(result, result_reference)
            if progress:
                self.assertEqual(
                    logger.to_list(),
                    list(range(50)) if stages is not None else []
                )

        def test_pool_mapconcat(self):
            logger = log() if progress else None
            pool = mr4mp.pool(processes, close=True)
            result = pool.mapconcat(add_one, range(0, 100), stages=stages, progress=logger)
            self.assertEqual(list(result), list(range(1, 101)))
            if progress:
                self.assertEqual(
                    logger.to_list(),
                    list(range(100)) if stages is not None else []
                )

    return Test_pool_stages_progress

def define_class_functions(processes, stages, progress):
    """
    Define and return class of unit tests for stand-alone functions
    for the given configuration.
    """
    class Test_functions(TestCase):
        def test_mapreduce(self):
            logger = log() if progress else None
            result = mr4mp.mapreduce(
                index, merge, range(50),
                processes=processes, stages=stages, progress=logger
            )
            self.assertEqual(result, result_reference)
            if progress:
                self.assertEqual(
                    logger.to_list(),
                    list(range(50)) if stages is not None else []
                )

        def test_mapconcat(self):
            logger = log() if progress else None
            result = mr4mp.mapconcat(
                add_one, range(0, 100),
                processes=processes, stages=stages, progress=logger
            )
            self.assertEqual(list(result), list(range(1, 101)))
            if progress:
                self.assertEqual(
                    logger.to_list(),
                    list(range(100)) if stages is not None else []
                )

    return Test_functions

class Test_pool(TestCase):
    def test_pool_cpu_count(self):
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
for processes in (1, 2):
    locals()['Test_pool_close_' + str(processes)] = define_class_pool_close(processes)
    for stages in (None, 4):
        for progress in (False, True):
            locals()[
                'Test_pool_stages_progress_' +\
                "_".join(map(str, [processes, stages, progress]))
            ] = define_class_pool_stages_progress(processes, stages, progress)
            locals()[
                'Test_functions_' +\
                "_".join(map(str, [processes, stages, progress]))
            ] = define_class_functions(processes, stages, progress)
