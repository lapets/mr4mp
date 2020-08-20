=====
mr4mp
=====

Thin MapReduce-like layer that wraps the Python multiprocessing library.

|pypi|

.. |pypi| image:: https://badge.fury.io/py/mr4mp.svg
   :target: https://badge.fury.io/py/mr4mp
   :alt: PyPI version and link.

Package Installation and Usage
------------------------------
The package is available on PyPI::

    python -m pip install mr4mp

The library can be imported in the usual way::

    import mr4mp

Examples
--------

Word-Document Index
~~~~~~~~~~~~~~~~~~~

Suppose we have some functions that we can use to build an index of randomly generated words::

    def word(): # Generate a random 7-letter "word".
        return ''.join(choice(ascii_lowercase) for _ in range(7))
    
    def index(id): # Build an index mapping some random words to an identifier.
        return {w:{id} for w in {word() for _ in range(100)}}
    
    def merge(i, j): # Merge two index dictionaries i and j.
        return {k:(i.get(k,set()) | j.get(k,set())) for k in i.keys() | j.keys()}

We can then construct an index in the following way::

    from random import choice
    from string import ascii_lowercase
    from timeit import default_timer
    start = default_timer()
    pool = mr4mp.pool()
    pool.mapreduce(index, merge, range(100))
    print("Finished in " + str(default_timer()-start) + "s using " + str(len(pool)) + " process(es).")

The above might yield the following output::

    Finished in 0.664681524217187s using 2 process(es).

Suppose we had instead explicitly specified that only one process can be used::

    pool = mr4mp.pool(1)

After the above modification, we might see the following output from the code block::

    Finished in 2.23329004518571s using 1 process(es).

Testing and Conventions
-----------------------
All unit tests are executed and their coverage is measured when using `nose <https://nose.readthedocs.io/>`_ (see ``setup.cfg`` for configution details)::

    nosetests

Style conventions are enforced using `Pylint <https://www.pylint.org/>`_::

    pylint mr4mp

Contributions
-------------
In order to contribute to the source code, open an issue or submit a pull request on the GitHub page for this library.

Versioning
----------
Beginning with version 0.1.0, the version number format for this library and the changes to the library associated with version number increments conform with `Semantic Versioning 2.0.0 <https://semver.org/#semantic-versioning-200>`_.
