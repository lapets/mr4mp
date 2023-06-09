"""
Test suite containing functional unit tests for the exported class,
methods, and standalone functions.
"""
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
    # pylint: disable=missing-function-docstring
    def test_module(self):
        module = import_module('mr4mp.mr4mp')
        self.assertTrue(api_methods().issubset(module.__dict__.keys()))

def word(index_doc, index_word):
    """Generate a random (but reproducible) three-character 'word'."""
    return ''.join(
        ascii_lowercase[i % 7]
        for i in sha256((index_doc * index_word).to_bytes(2, 'little')).digest()[:3]
    )

def doc(index_doc):
    """Generate a random (but reproducible) 25-word (or fewer) 'document' and its identifier."""
    return (
        [word(index_doc, index_word) for index_word in range(25)],
        sha256(index_doc.to_bytes(2, 'little')).hexdigest()
    )

def docs():
    """Generate list of 50 random (but reproducible) 'documents'."""
    return [doc(index_doc) for index_doc in range(50)]

def word_to_doc_id_dict(document):
    """Build a dictionary mapping the 'words' in a 'document' to its identifier."""
    (words, identifier) = document
    return {w: {identifier} for w in set(words)}

def merge_dicts(d, e):
    """Merge two dictionaries ``d`` and ``e``."""
    return {w: (d.get(w, set()) | e.get(w, set())) for w in d.keys() | e.keys()}

result_reference = reduce(merge_dicts, map(word_to_doc_id_dict, docs()))

def add_one(x):
    """
    Simple function defined within module (and not within a method body)
    so that tests that use multiple processes can invoke it.
    """
    return [x + 1]

class log:
    """
    Log of progress function outputs that can be used for testing progress update
    features.
    """
    # pylint: disable=missing-function-docstring
    def __init__(self):
        self.logged = []

    def __call__(self, xs):
        self.logged = list(xs)
        return self.logged

    def to_list(self):
        return [x for xs in self.logged for x in xs]

def define_class_pool_close(processes):
    """
    Define and return class of pool unit tests for the supplied pool closing
    behavior configuration.
    """
    class Test_pool_close(TestCase):
        """
        Tests of behavior of method that closes an instance.
        """
        # pylint: disable=missing-function-docstring
        def test_pool_mapreduce_pool_close(self):
            pool = mr4mp.pool(processes, close=True)
            self.assertFalse(pool.closed())
            print("Starting.")
            start = default_timer()
            result = pool.mapreduce(word_to_doc_id_dict, merge_dicts, docs(), close=False)
            self.assertFalse(pool.closed())
            result = pool.mapreduce(word_to_doc_id_dict, merge_dicts, docs())
            self.assertTrue(pool.closed())
            print("Finished in " + str(default_timer()-start) +
                  "s using " + str(len(pool)) + " processes.")
            self.assertDictEqual(result, result_reference)

        def test_pool_mapreduce_function_close(self):
            pool = mr4mp.pool(processes, close=False)
            self.assertFalse(pool.closed())
            print("Starting.")
            start = default_timer()
            result = pool.mapreduce(word_to_doc_id_dict, merge_dicts, docs(), close=False)
            self.assertFalse(pool.closed())
            result = pool.mapreduce(word_to_doc_id_dict, merge_dicts, docs(), close=True)
            self.assertTrue(pool.closed())
            print("Finished in " + str(default_timer()-start) +
                  "s using " + str(len(pool)) + " processes.")
            self.assertDictEqual(result, result_reference)

        def test_pool_mapreduce_pool_open_reuse(self):
            pool = mr4mp.pool(processes, close=False)
            result = pool.mapreduce(word_to_doc_id_dict, merge_dicts, docs())
            result = pool.mapreduce(word_to_doc_id_dict, merge_dicts, docs())
            result = pool.mapreduce(word_to_doc_id_dict, merge_dicts, docs())
            self.assertFalse(pool.closed())
            pool.close()
            self.assertTrue(pool.closed())
            self.assertDictEqual(result, result_reference)

        def test_pool_mapreduce_pool_close_reuse_exception(self):
            pool = mr4mp.pool(processes, close=True)
            pool.mapreduce(word_to_doc_id_dict, merge_dicts, docs())
            with self.assertRaises(ValueError):
                pool.mapreduce(word_to_doc_id_dict, merge_dicts, docs())

        def test_pool_mapreduce_function_close_reuse_exception(self):
            pool = mr4mp.pool(processes, close=False)
            pool.mapreduce(word_to_doc_id_dict, merge_dicts, docs(), close=True)
            with self.assertRaises(ValueError):
                pool.mapreduce(word_to_doc_id_dict, merge_dicts, docs())

        def test_pool_mapreduce_many_with_as(self):
            with mr4mp.pool(processes) as pool:
                result = pool.mapreduce(word_to_doc_id_dict, merge_dicts, docs())
                result = pool.mapreduce(word_to_doc_id_dict, merge_dicts, docs())
                result = pool.mapreduce(word_to_doc_id_dict, merge_dicts, docs())
                self.assertFalse(pool.closed())
            self.assertDictEqual(result, result_reference)

    return Test_pool_close

def define_class_pool_stages_progress(processes, stages, progress):
    """
    Define and return class of pool unit tests for the supplied stage quantity
    and progress function configuration.
    """
    class Test_pool_stages_progress(TestCase):
        """
        Tests of feature that allows a workflow to be broken down into stages,
        with progress being reported at the end of each stage.
        """
        # pylint: disable=missing-function-docstring
        def test_pool_mapreduce(self):
            logger = log() if progress else None
            pool = mr4mp.pool(processes, close=True)
            result = pool.mapreduce(
                word_to_doc_id_dict, merge_dicts, docs(),
                stages=stages, progress=logger
            )
            self.assertDictEqual(result, result_reference)
            if progress:
                self.assertEqual(
                    logger.to_list(),
                    docs() if stages is not None else []
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
        """
        Tests of one-shot functions for executing workflows.
        """
        # pylint: disable=missing-function-docstring
        def test_mapreduce(self):
            logger = log() if progress else None
            result = mr4mp.mapreduce(
                word_to_doc_id_dict, merge_dicts, docs(),
                processes=processes, stages=stages, progress=logger
            )
            self.assertDictEqual(result, result_reference)
            if progress:
                self.assertEqual(
                    logger.to_list(),
                    docs() if stages is not None else []
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
    """
    Tests of resource pool instance methods.
    """
    # pylint: disable=missing-function-docstring
    def test_pool_cpu_count(self):
        pool = mr4mp.pool()
        self.assertEqual(pool.cpu_count(), mp.cpu_count())
        self.assertEqual(len(pool), mp.cpu_count())
        pool.close()

    def test_pool_init_negative(self):
        pool = mr4mp.pool(-1)
        self.assertEqual(len(pool), mp.cpu_count() - 1)
        pool.close()

    def test_pool_mapreduce(self):
        pool = mr4mp.pool(close=True)
        print("Starting.")
        start = default_timer()
        result = pool.mapreduce(word_to_doc_id_dict, merge_dicts, docs())
        print("Finished in " + str(default_timer()-start) +
              "s using " + str(len(pool)) + " processes.")
        self.assertDictEqual(result, result_reference)

    def test_pool_mapreduce_terminate(self):
        pool = mr4mp.pool()
        print("Starting.")
        start = default_timer()
        result = pool.mapreduce(word_to_doc_id_dict, merge_dicts, docs())
        print("Finished in " + str(default_timer()-start) +
              "s using " + str(len(pool)) + " processes.")
        self.assertDictEqual(result, result_reference)
        pool.terminate()
        self.assertTrue(pool.closed())

# The instantiated test classes below are discovered in the local scope
# and executed by the unit testing framework (e.g., using nosetests).
for _processes in (1, 2):
    locals()['Test_pool_close_' + str(_processes)] = define_class_pool_close(_processes)
    for _stages in (None, 4):
        for _progress in (False, True):
            locals()[
                'Test_pool_stages_progress_' +\
                "_".join(map(str, [_processes, _stages, _progress]))
            ] = define_class_pool_stages_progress(_processes, _stages, _progress)
            locals()[
                'Test_functions_' +\
                "_".join(map(str, [_processes, _stages, _progress]))
            ] = define_class_functions(_processes, _stages, _progress)
