from setuptools import setup

with open("README.rst", "r") as fh:
    long_description = fh.read().replace(".. include:: toc.rst\n\n", "")

# The lines below can be parsed by `docs/conf.py`.
name = "mr4mp"
version = "2.2.2"

setup(
    name=name,
    version=version,
    packages=[name,],
    install_requires=["parts~=1.2",],
    license="MIT",
    url="https://github.com/lapets/mr4mp",
    author="Andrei Lapets",
    author_email="a@lapets.io",
    description="Thin MapReduce-like layer that wraps the "+\
                "Python multiprocessing library.",
    long_description=long_description,
    long_description_content_type="text/x-rst",
    test_suite="nose.collector",
    tests_require=["nose"],
)
