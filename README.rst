=====
mr4mp
=====

Thin MapReduce-like layer that wraps the Python multiprocessing library.

|pypi| |readthedocs| |actions| |coveralls|

.. |pypi| image:: https://badge.fury.io/py/mr4mp.svg
   :target: https://badge.fury.io/py/mr4mp
   :alt: PyPI version and link.

.. |readthedocs| image:: https://readthedocs.org/projects/mr4mp/badge/?version=latest
   :target: https://mr4mp.readthedocs.io/en/latest/?badge=latest
   :alt: Read the Docs documentation status.

.. |actions| image:: https://github.com/lapets/mr4mp/workflows/lint-test-cover-docs/badge.svg
   :target: https://github.com/lapets/mr4mp/actions/workflows/lint-test-cover-docs.yml
   :alt: GitHub Actions status.

.. |coveralls| image:: https://coveralls.io/repos/github/lapets/mr4mp/badge.svg?branch=main
   :target: https://coveralls.io/github/lapets/mr4mp?branch=main
   :alt: Coveralls test coverage summary.

Purpose
-------
This package provides a streamlined interface for the built-in Python `multiprocessing <https://docs.python.org/3/library/multiprocessing.html>`__ library. The interface makes it possible to parallelize in a succinct way (sometimes using only one line of code) a data workflow that can be expressed in a `MapReduce <https://en.wikipedia.org/wiki/MapReduce>`__-like form. More background information about this package's design and implementation, as well a detailed use case, can be found in a `related article <https://github.com/python-supply/map-reduce-and-multiprocessing>`__.

Package Installation and Usage
------------------------------
This library is available as a `package on PyPI <https://pypi.org/project/mr4mp>`__::

    python -m pip install mr4mp

The library can be imported in the usual way::

    import mr4mp

Word-Document Index Example
^^^^^^^^^^^^^^^^^^^^^^^^^^^

Suppose we have some functions that we can use to build an index of randomly generated words::

    from random import choice
    from string import ascii_lowercase

    def word(): # Generate a random 7-letter "word".
        return ''.join(choice(ascii_lowercase) for _ in range(7))
    
    def index(id): # Build an index mapping some random words to an identifier.
        return {w:{id} for w in {word() for _ in range(100)}}
    
    def merge(i, j): # Merge two index dictionaries i and j.
        return {k:(i.get(k,set()) | j.get(k,set())) for k in i.keys() | j.keys()}

We can then construct an index in the following way::

    from timeit import default_timer

    start = default_timer()
    pool = mr4mp.pool()
    pool.mapreduce(index, merge, range(100))
    pool.close()
    print("Finished in " + str(default_timer()-start) + "s using " + str(len(pool)) + " process(es).")

The above might yield the following output::

    Finished in 0.664681524217187s using 2 process(es).

Suppose that we instead explicitly specify that only one process can be used::

    pool = mr4mp.pool(1)

After the above modification, we might see the following output from the code block::

    Finished in 2.23329004518571s using 1 process(es).

Documentation
-------------
.. include:: toc.rst

The documentation can be generated automatically from the source files using `Sphinx <https://www.sphinx-doc.org>`__::

    cd docs
    python -m pip install -r requirements.txt
    sphinx-apidoc -f -E --templatedir=_templates -o _source .. ../setup.py && make html

Testing and Conventions
-----------------------
All unit tests are executed and their coverage is measured when using `pytest <https://docs.pytest.org>`__ (see ``setup.cfg`` for configuration details)::

    python -m pip install pytest pytest-cov
    python -m pytest

Some unit tests are included in the module itself and can be executed using `doctest <https://docs.python.org/3/library/doctest.html>`__::

    python mr4mp/mr4mp.py -v

Style conventions are enforced using `Pylint <https://www.pylint.org>`__::

    python -m pip install pylint
    python -m pylint mr4mp

Contributions
-------------
In order to contribute to the source code, open an issue or submit a pull request on the `GitHub page <https://github.com/lapets/mr4mp>`__ for this library.

Versioning
----------
Beginning with version 0.1.0, the version number format for this library and the changes to the library associated with version number increments conform with `Semantic Versioning 2.0.0 <https://semver.org/#semantic-versioning-200>`__.

Publishing
----------
This library can be published as a `package on PyPI <https://pypi.org/project/mr4mp>`__ by a package maintainer. Install the `wheel <https://pypi.org/project/wheel>`__ package, remove any old build/distribution files, and package the source into a distribution archive::

    python -m pip install wheel
    rm -rf dist *.egg-info
    python setup.py sdist bdist_wheel

Next, install the `twine <https://pypi.org/project/twine>`__ package and upload the package distribution archive to PyPI::

    python -m pip install twine
    python -m twine upload dist/*
