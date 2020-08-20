"""MapReduce for multiprocessing.

Thin MapReduce-like layer on top of the Python multiprocessing
library.
"""

import doctest
import multiprocessing as mp
from operator import concat
from functools import reduce, partial
from parts import parts

class Pool():
    """
    Class for a MapReduce-for-multiprocessing pool.

    >>> len(Pool()) == Pool().cpu_count()
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
        if self.size == 1:
            return [[op(x) for x in xs]]
        else:
            return self.pool.map(
                partial(map, op),
                parts(xs, self.pool._processes)
            )

    def _reduce(self, op, xs_per_part):
        """
        Apply the specified binary operator to the results
        obtained from multiple processes.
        """
        if self.size == 1 and len(xs_per_part) == 1:
            return reduce(op, map(partial(reduce, op), xs_per_part))
        else:
            return reduce(op, self.pool.map(partial(reduce, op), xs_per_part))

    def mapreduce(self, m, r, xs, stages=None, progress=None, close=True):
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
            for xs_ in progress(xss):
                result_stage = self._reduce(r, self._map(m, xs_))
                result = result_stage if result is None else r(result, result_stage)

        # Release resources if directed to do so.
        if close:
            self.close()

        return result

    def mapconcat(self, m, xs, stages=None, progress=None, close=True):
        """
        Perform the map operation (optionally in stages on subsequences
        of the data) and then release resources if directed to do so.
        """
        return self.mapreduce(m, concat, xs, stages, progress, close)

    def close(self):
        """Release resources."""
        self.pool.close()

    def cpu_count(self):
        """Return number of available CPUs."""
        return mp.cpu_count()

    def __len__(self):
        return self.size

pool = Pool # Alternative synonym.

def mapreduce(m, r, xs, processes=None, stages=None, progress=None):
    """
    One-shot synonym (no explicit object management
    or resource allocation is required from the user).
    """
    if processes == 1:
        progress = progress if progress is not None else (lambda ss: ss)
        if stages is not None:
            return reduce(r, [
                m(x)
                for xs in progress(list(parts(xs, stages)))
                for x in xs
            ])
        else:
            return reduce(r, [m(x) for x in xs])
    else:
        pool_ = pool() if processes is None else pool(processes)
        return pool_.mapreduce(m, r, xs, stages, progress, True)

def mapconcat(m, xs, processes=None, stages=None, progress=None):
    """
    One-shot synonym (no explicit object management
    or resource allocation is required from the user).
    """
    return mapreduce(m, concat, xs, processes, stages, progress)

if __name__ == "__main__":
    doctest.testmod() # pragma: no cover
