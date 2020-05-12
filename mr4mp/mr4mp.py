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

    def mapreduce(self, m, r, xs, close = True):
        """
        Perform the map and reduce operations in sequence and then
        release the resources if directed to do so.
        """
        result = self.reduce(r, self.map(m, xs))

        if close:
            self.close()

        return result

    def close(self):
        """Release resources."""
        self.pool.close()

    def cpu_count(self):
        return mp.cpu_count()

    def __len__(self):
        return self.size

pool = Pool # Alternative synonym.

def mapreduce(m, r, xs, processes = None):
    """
    One-shot synonym (no explicit object management
    or resource allocation).
    """
    p = pool() if processes is None else pool(processes)
    return p.mapreduce(m, r, xs)

if __name__ == "__main__": 
    doctest.testmod()
