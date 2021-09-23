from setuptools import setup

with open("README.rst", "r") as fh:
    long_description = fh.read().replace(".. include:: toc.rst\n\n", "")

# The line below is parsed by `docs/conf.py`.
version = "2.1.2"

setup(
    name="mr4mp",
    version=version,
    packages=["mr4mp",],
    install_requires=["parts~=1.1.2",],
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
