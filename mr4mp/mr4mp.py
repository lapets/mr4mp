###############################################################################
## 
## mr4mp.py
## https://github.com/lapets/mr4mp
##
## Thin MapReduce-like layer on top of the Python multiprocessing library.
##
##

import multiprocessing as mp
from functools import reduce, partial
from parts import parts
import doctest

###############################################################################
##

class Pool():
    """
    Class for a MapReduce-for-multiprocessing pool.
    
    >>> len(Pool()) == mp.cpu_count()
    True
    """
    def __init__(self, processes=mp.cpu_count()):
        self.pool = mp.Pool(processes=processes)
        self.size = processes

    def map(self, op, xs):
        return self.pool.map(partial(map, op), parts(xs, self.pool._processes))

    def reduce(self, op, xs_per_part):
        return reduce(op, self.pool.map(partial(reduce, op), xs_per_part))

    def mapreduce(self, m, r, xs):
        return self.reduce(r, self.map(m, xs))

    def cpu_count(self):
        return mp.cpu_count()

    def __len__(self):
        return self.size

pool = Pool # Alternative synonym.

if __name__ == "__main__": 
    doctest.testmod()

## eof