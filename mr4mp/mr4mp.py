"""MapReduce for multiprocessing.

Thin MapReduce-like layer that wraps the Python multiprocessing
library.
"""

import doctest
import multiprocessing as mp
from operator import concat
from functools import reduce, partial
from parts import parts

class pool():
    """
    Class for a MapReduce-for-multiprocessing pool.

    >>> len(pool()) == pool().cpu_count()
    True
    >>> def add_one(x):
    ...     return [x + 1]
    >>> with pool(1, close=True) as pool_:
    ...     results = pool_.mapconcat(m=add_one, xs=range(3))
    >>> results
    [1, 2, 3]
    """
    def __init__(self, processes=mp.cpu_count(), stages=None, progress=None, close=False):
        """Initialize a pool given the target number of processes."""
        if processes != 1:
            self._pool = mp.Pool(processes=processes)
        self._processes = processes
        self._stages = stages
        self._progress = progress
        self._close = close # Indicates whether to close the pool after first `mapreduce` call.
        self._closed = False
        self._terminated = False

    def __enter__(self):
        """
        Placeholder to enable use of `with` construct.
        """
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        """
        Close the pool; exceptions are not suppressed.
        """
        self.close()

    def _map(self, op, xs):
        """
        Split data (one part per process) and map the operation
        onto each part.
        """
        if self._processes == 1:
            return [[op(x) for x in xs]]
        else:
            return self._pool.map(
                partial(map, op),
                parts(xs, self._pool._processes)
            )

    def _reduce(self, op, xs_per_part):
        """
        Apply the specified binary operator to the results
        obtained from multiple processes.
        """
        if self._processes == 1 and len(xs_per_part) == 1:
            return reduce(op, map(partial(reduce, op), xs_per_part))
        else:
            return reduce(op, self._pool.map(partial(reduce, op), xs_per_part))

    def mapreduce(self, m, r, xs, stages=None, progress=None, close=None):
        """
        Perform the map and reduce operations (optionally in stages on
        subsequences of the data) and then release resources if directed
        to do so.
        """
        if self.closed():
            raise ValueError('Pool not running')

        # Update state to enforce semantics of closing.
        self._closed = close if close is not None else self._closed

        stages = self._stages if stages is None else stages
        progress = self._progress if progress is None else progress
        close = self._close if close is None else close

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

    def mapconcat(self, m, xs, stages=None, progress=None, close=None):
        """
        Perform the map operation (optionally in stages on subsequences
        of the data) and then release resources if directed to do so.
        """
        return self.mapreduce(m, concat, xs, stages, progress, close)

    def close(self):
        """Prevent any additional work to be added to the pool."""
        self._closed = True
        if self._processes != 1:
            self._pool.close()

    def closed(self):
        """Indicate whether the pool has been closed."""
        if self._processes == 1:
            return self._closed
        else:
            return self._closed or self._pool._state in ('CLOSE', 'TERMINATE')

    def terminate(self):
        """Terminate the pool."""
        self._closed = True
        self._terminated = True
        if self._processes != 1:
            self._pool.terminate()

    def cpu_count(self):
        """Return number of available CPUs."""
        return mp.cpu_count()

    def __len__(self):
        """Return number of processes supplied as a configuration parameter."""
        return self._processes

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
        return pool_.mapreduce(m, r, xs, stages=stages, progress=progress, close=True)

def mapconcat(m, xs, processes=None, stages=None, progress=None):
    """
    One-shot synonym (no explicit object management
    or resource allocation is required from the user).
    """
    return mapreduce(m, concat, xs, processes, stages=stages, progress=progress)

if __name__ == "__main__":
    doctest.testmod() # pragma: no cover
