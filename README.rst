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

Installation and Usage
----------------------
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
    
    def index(identifier): # Build an index mapping some random words to an identifier.
        return {w:{identifier} for w in {word() for _ in range(100)}}
    
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

Development
-----------
All installation and development dependencies are fully specified in ``pyproject.toml``. The ``project.optional-dependencies`` object is used to `specify optional requirements <https://peps.python.org/pep-0621>`__ for various development tasks. This makes it possible to specify additional options (such as ``docs``, ``lint``, and so on) when performing installation using `pip <https://pypi.org/project/pip>`__::

    python -m pip install .[docs,lint]

Documentation
^^^^^^^^^^^^^
The documentation can be generated automatically from the source files using `Sphinx <https://www.sphinx-doc.org>`__::

    python -m pip install .[docs]
    cd docs
    sphinx-apidoc -f -E --templatedir=_templates -o _source .. && make html

Testing and Conventions
^^^^^^^^^^^^^^^^^^^^^^^
All unit tests are executed and their coverage is measured when using `pytest <https://docs.pytest.org>`__ (see the ``pyproject.toml`` file for configuration details)::

    python -m pip install .[test]
    python -m pytest

Some unit tests are included in the module itself and can be executed using `doctest <https://docs.python.org/3/library/doctest.html>`__::

    python src/mr4mp/mr4mp.py -v

Style conventions are enforced using `Pylint <https://pylint.pycqa.org>`__::

    python -m pip install .[lint]
    python -m pylint src/mr4mp test/test_mr4mp.py

Contributions
^^^^^^^^^^^^^
In order to contribute to the source code, open an issue or submit a pull request on the `GitHub page <https://github.com/lapets/mr4mp>`__ for this library.

Versioning
^^^^^^^^^^
Beginning with version 0.1.0, the version number format for this library and the changes to the library associated with version number increments conform with `Semantic Versioning 2.0.0 <https://semver.org/#semantic-versioning-200>`__.

Publishing
^^^^^^^^^^
This library can be published as a `package on PyPI <https://pypi.org/project/mr4mp>`__ by a package maintainer. First, install the dependencies required for packaging and publishing::

    python -m pip install .[publish]

Ensure that the correct version number appears in the ``pyproject.toml`` file and in any links to this package's Read the Docs documentation that exist in this README document. Also ensure that the Read the Docs project for this library has an `automation rule <https://docs.readthedocs.io/en/stable/automation-rules.html>`__ that activates and sets as the default all tagged versions. Create and push a tag for this version (replacing ``?.?.?`` with the version number)::

    git tag ?.?.?
    git push origin ?.?.?

Remove any old build/distribution files. Then, package the source into a distribution archive using the `wheel <https://pypi.org/project/wheel>`__ package::

    rm -rf build dist src/*.egg-info
    python -m build --sdist --wheel .

Finally, upload the package distribution archive to `PyPI <https://pypi.org>`__ using the `twine <https://pypi.org/project/twine>`__ package::

    python -m twine upload dist/*
