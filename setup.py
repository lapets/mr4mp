from setuptools import setup

setup(
    name             = 'mr4mp',
    version          = '0.0.4.0',
    packages         = ['mr4mp',],
    install_requires = ['parts',],
    license          = 'MIT',
    url              = 'https://github.com/lapets/mr4mp',
    author           = 'Andrei Lapets',
    author_email     = 'a@lapets.io',
    description      = 'Thin MapReduce-like layer on top of the Python multiprocessing library.',
    long_description = open('README.rst').read(),
    test_suite       = 'nose.collector',
    tests_require    = ['nose'],
)
