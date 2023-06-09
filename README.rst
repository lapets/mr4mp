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
This package provides a streamlined interface for the built-in Python `multiprocessing <https://docs.python.org/3/library/multiprocessing.html>`__ library. The interface makes it possible to parallelize in a succinct way (sometimes using only one line of code) a data workflow that can be expressed in a  `MapReduce <https://en.wikipedia.org/wiki/MapReduce>`__-like form. More background information about this package's design and implementation, as well a detailed use case, can be found in a `related article <https://github.com/python-supply/map-reduce-and-multiprocessing>`__.

Installation and Usage
----------------------
This library is available as a `package on PyPI <https://pypi.org/project/mr4mp>`__:

.. code-block:: bash

    python -m pip install mr4mp

The library can be imported in the usual way:

.. code-block:: python

    import mr4mp

Examples
^^^^^^^^

In addition to the `use case in a related article <https://github.com/python-supply/map-reduce-and-multiprocessing>`__ and the example below, smaller examples for each of the methods can be found in the `documentation <https://mr4mp.readthedocs.io>`__.

Word-Document Index
~~~~~~~~~~~~~~~~~~~

Assume there exists a collection of documents and that each document contains a collection of 7-character "words". This example demonstrates how a dictionary that associates each word to the collection of documents in which that word appears can be built by leveraging `multiprocessing <https://docs.python.org/3/library/multiprocessing.html>`__ and the `MapReduce <https://en.wikipedia.org/wiki/MapReduce>`__ paradigm. Suppose the function definitions below are found within a module ``example.py``:

.. code-block:: python

    from random import choice
    from string import ascii_lowercase
    from uuid import uuid4

    def word():
        """Generate a random 7-character 'word'."""
        return ''.join(choice(ascii_lowercase) for _ in range(7))

    def doc():
        """Generate a random 25-word 'document' and its identifier."""
        return ([word() for _ in range(25)], uuid4())

    def docs():
        """Generate list of 50 random 'documents'."""
        return [doc() for _ in range(50)]

    def word_to_doc_id_dict(document):
        """Build a dictionary mapping the 'words' in a 'document' to its identifier."""
        (words, identifier) = document
        return {w: {identifier} for w in words}

    def merge_dicts(d, e):
        """Merge two dictionaries ``d`` and ``e``."""
        return {w: (d.get(w, set()) | e.get(w, set())) for w in d.keys() | e.keys()}

.. |pool| replace:: ``pool``
.. _pool: https://mr4mp.readthedocs.io/en/2.7.0/_source/mr4mp.html#mr4mp.mr4mp.pool

The code below (also included in ``example.py``) constructs a dictionary that maps each individual word to the set of document identifiers in which that word appears. The code does so by incrementally building up larger and larger dictionaries (starting from one dictionary per document via the ``word_to_doc_id_dict`` function and merging them via the ``merge_dicts`` function), all while using the maximum number of processes supported by the system: 

.. code-block:: python

    if __name__ == '__main__':
        import mr4mp
        from timeit import default_timer

        start = default_timer()
        p = mr4mp.pool()
        p.mapreduce(word_to_doc_id_dict, merge_dicts, docs())
        p.close()
        print(
            "Finished in " + str(default_timer()-start) + "s " +
            "using " + str(len(p)) + " process(es)."
        )

Note that any code invoking library methods must be protected inside an ``if __name__ == '__main__':`` block to ensure that the `multiprocessing <https://docs.python.org/3/library/multiprocessing.html>`__ library methods can `safely load the module <https://docs.python.org/3/library/multiprocessing.html#the-process-class>`__ without causing side effects. Executing the module might yield the output below:

.. code-block:: bash

    python example.py
    Finished in 0.664681524217187s using 2 process(es).

Suppose that it is explicitly indicated (by adding ``processes=1`` to the invocation of |pool|_) that only one process can be used:

.. code-block:: python

    p = mr4mp.pool(processes=1)

After the above modification, executing the module might yield the output below:

.. code-block:: bash

    python example.py
    Finished in 2.23329004518571s using 1 process(es).

Development
-----------
All installation and development dependencies are fully specified in ``pyproject.toml``. The ``project.optional-dependencies`` object is used to `specify optional requirements <https://peps.python.org/pep-0621>`__ for various development tasks. This makes it possible to specify additional options (such as ``docs``, ``lint``, and so on) when performing installation using `pip <https://pypi.org/project/pip>`__:

.. code-block:: bash

    python -m pip install .[docs,lint]

Documentation
^^^^^^^^^^^^^
The documentation can be generated automatically from the source files using `Sphinx <https://www.sphinx-doc.org>`__:

.. code-block:: bash

    python -m pip install .[docs]
    cd docs
    sphinx-apidoc -f -E --templatedir=_templates -o _source .. && make html

Testing and Conventions
^^^^^^^^^^^^^^^^^^^^^^^
All unit tests are executed and their coverage is measured when using `pytest <https://docs.pytest.org>`__ (see the ``pyproject.toml`` file for configuration details):

.. code-block:: bash

    python -m pip install .[test]
    python -m pytest

Some unit tests are included in the module itself and can be executed using `doctest <https://docs.python.org/3/library/doctest.html>`__:

.. code-block:: bash

    python src/mr4mp/mr4mp.py -v

Style conventions are enforced using `Pylint <https://pylint.readthedocs.io>`__:

.. code-block:: bash

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
This library can be published as a `package on PyPI <https://pypi.org/project/mr4mp>`__ by a package maintainer. First, install the dependencies required for packaging and publishing:

.. code-block:: bash

    python -m pip install .[publish]

Ensure that the correct version number appears in ``pyproject.toml``, and that any links in this README document to the Read the Docs documentation of this package (or its dependencies) have appropriate version numbers. Also ensure that the Read the Docs project for this library has an `automation rule <https://docs.readthedocs.io/en/stable/automation-rules.html>`__ that activates and sets as the default all tagged versions. Create and push a tag for this version (replacing ``?.?.?`` with the version number):

.. code-block:: bash

    git tag ?.?.?
    git push origin ?.?.?

Remove any old build/distribution files. Then, package the source into a distribution archive:

.. code-block:: bash

    rm -rf build dist src/*.egg-info
    python -m build --sdist --wheel .

Finally, upload the package distribution archive to `PyPI <https://pypi.org>`__:

.. code-block:: bash

    python -m twine upload dist/*
