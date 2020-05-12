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

    def _map(self, op, xs):
        """
        Split data (one part per process) and map the operation
        onto each part.
        """
        return self.pool.map(partial(map, op), parts(xs, self.pool._processes))

    def _reduce(self, op, xs_per_part):
        """
        Apply the specified binary operator to the results
        obtained from multiple processes.
        """
        return reduce(op, self.pool.map(partial(reduce, op), xs_per_part))

    def map(self, m, xs, stages = None, progress = None, close = True):
        """
        Perform the map operation (optionally in stages on subsequences
        of the data) and then release resources if directed to do so.
        """
        if stages is None:
            results = [y for ys in self._map(m, xs) for y in ys]
        else:
            # Separate input into specified number of stages.
            xss = parts(xs, stages)
            xss = list(xss) if progress is not None else xss

            # In case there is no progress function, create placeholder.
            progress = progress if progress is not None else (lambda ss: ss)

            # Perform each stage sequentially.
            results = []
            for xs in progress(xss):
                results.extend([y for ys in self._map(m, xs) for y in ys])

        # Release resources if directed to do so.
        if close:
            self.close()

        return results

    def mapreduce(self, m, r, xs, stages = None, progress = None, close = True):
        """
        Perform the map and reduce operations (optionally in stages on
        subsequences of the data) and then release resources if directed
        to do so.
        """
        if stages is None:
            result = self._reduce(r, self._map(m, xs))
        else:
            # Separate input into specified number of stages.
            xss = parts(xs, stages)
            xss = list(xss) if progress is not None else xss

            # In case there is no progress function, create placeholder.
            progress = progress if progress is not None else (lambda ss: ss)

            # Perform each stage sequentially.
            result = None
            for xs in progress(xss):
                result_stage = self._reduce(r, self._map(m, xs))
                result = result_stage if result is None else r(result, result_stage)

        # Release resources if directed to do so.
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

def map_(m, xs, processes = None, stages = None, progress = None):
    """
    One-shot synonym (no explicit object management
    or resource allocation).
    """
    p = pool() if processes is None else pool(processes)
    return p.map(m, xs, stages, progress, True)

def mapreduce(m, r, xs, processes = None, stages = None, progress = None):
    """
    One-shot synonym (no explicit object management
    or resource allocation).
    """
    p = pool() if processes is None else pool(processes)
    return p.mapreduce(m, r, xs, stages, progress, True)

if __name__ == "__main__": 
    doctest.testmod()
