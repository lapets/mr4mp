"""MapReduce for multiprocessing.

Thin MapReduce-like layer on top of the Python multiprocessing
library.
"""

import multiprocessing as mp
from functools import reduce, partial
from parts import parts
import doctest

class Pool():
    """
    Class for a MapReduce-for-multiprocessing pool.
    
    >>> len(Pool()) == mp.cpu_count()
    True
    """
    def __init__(self, processes=mp.cpu_count()):
        """Initialize a pool given the target number of processes."""
        self.pool = mp.Pool(processes=processes)
        self.size = processes

    def map(self, op, xs):
        """
        Split data (one part per process) and map the operation
        onto each part.
        """
        return self.pool.map(partial(map, op), parts(xs, self.pool._processes))

    def reduce(self, op, xs_per_part):
        """
        Apply the specified binary operator to the results
        obtained from multiple processes.
        """
        return reduce(op, self.pool.map(partial(reduce, op), xs_per_part))

    def mapreduce(self, m, r, xs):
        """Perform the map and reduce operations in sequence."""
        result = self.reduce(r, self.map(m, xs))
        self.pool.close()
        return result

    def cpu_count(self):
        return mp.cpu_count()

    def __len__(self):
        return self.size

pool = Pool # Alternative synonym.

if __name__ == "__main__": 
    doctest.testmod()
