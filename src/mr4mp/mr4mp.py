"""
Thin MapReduce-like layer that wraps the Python multiprocessing
library.
"""
from __future__ import annotations
from typing import Any, Optional, Callable, Sequence, Iterable
import doctest
import collections.abc
import multiprocessing
from operator import concat
from functools import reduce, partial
import parts

def _parts(xs: Iterable, quantity: int) -> Sequence:
    """
    Wrapper for the partitioning function :obj:`~parts.parts.parts`. This
    wrapper returns a :obj:`~collections.abc.Sized` list of parts if the
    original input iterable is :obj:`~collections.abc.Sized`.
    """
    xss = parts.parts(xs, quantity)
    return list(xss) if isinstance(xss, collections.abc.Sized) else xss

class pool:
    """
    Class for a MapReduce-for-multiprocessing resource pool that can be used to
    run MapReduce-like workflows across multiple processes.

    :param processes: Number of processes to allocate and to employ in executing workflows.
    :param stages: Number of stages (progress updates are provided once per stage).
    :param progress: Function that wraps an iterable (can be used to also report progress).
    :param close: Flag indicating whether this instance should be closed after one workflow.

    >>> from operator import inv, add
    >>> with pool() as pool_:
    ...     results = pool_.mapreduce(m=inv, r=add, xs=range(3))
    ...     results
    -6
    """
    def __init__(
            self: pool,
            processes: Optional[int] = None,
            stages: Optional[int] = None,
            progress: Optional[Callable[[Iterable], Iterable]] = None,
            close: Optional[bool] = False
        ):
        """
        Initialize a :obj:`pool` instance given the target number of processes.
        """
        # Use the maximum number of available processes as the default.
        # If a negative number of processes is designated, wrap around
        # and subtract from the maximum.
        if isinstance(processes, int) and processes <= 0:
            processes = multiprocessing.cpu_count() + processes
        elif processes is None:
            processes = multiprocessing.cpu_count()

        # Only create a multiprocessing pool if necessary.
        if processes != 1:
            # pylint: disable=consider-using-with
            self._pool = multiprocessing.Pool(processes=processes)

        self._processes = processes
        self._stages = stages
        self._progress = progress
        self._close = close # Indicates whether to close pool after first ``mapreduce`` call.
        self._closed = False
        self._terminated = False

    def __enter__(self: pool):
        """
        Placeholder to enable use of ``with`` construct.
        """
        return self

    def __exit__(self: pool, *exc_details):
        """
        Close this instance; exceptions are not suppressed.
        """
        self.close()

    def _map(self: pool, op: Callable, xs: Iterable):
        """
        Split data (one part per process) and map the operation
        onto each part.
        """
        if self._processes == 1:
            return [[op(x) for x in xs]]

        return self._pool.map(partial(map, op), parts.parts(xs, len(self)))

    def _reduce(self: pool, op: Callable, xs_per_part: Iterable):
        """
        Apply the specified binary operator to the results
        obtained from multiple processes.
        """
        if self._processes == 1 and len(xs_per_part) == 1:
            return reduce(op, map(partial(reduce, op), xs_per_part))

        return reduce(op, self._pool.map(partial(reduce, op), xs_per_part))

    def mapreduce(
            self: pool,
            m: Callable[..., Any],
            r: Callable[..., Any],
            xs: Iterable,
            stages: Optional[int] = None,
            progress: Optional[Callable[[Iterable], Iterable]] = None,
            close: Optional[bool] = None
        ):
        """
        Perform the map operation ``m`` and the reduce operation ``r`` over the
        supplied inputs ``xs`` (optionally in stages on subsequences of the data)
        and then release resources if directed to do so.

        :param m: Operation to be applied to each element in the input iterable.
        :param r: Operation that can combine two outputs from itself, the map operation, or a mix.
        :param xs: Input to process using the map and reduce operations.
        :param stages: Number of stages (progress updates are provided once per stage).
        :param progress: Function that wraps an iterable (can be used to also report progress).
        :param close: Flag indicating whether this instance should be closed after one workflow.

        The ``stages``, ``progress``, and ``close`` parameter values each revert by
        default to those of this :obj:`pool` instance if they are not explicitly
        supplied. Supplying a value for any one of these parameters when invoking
        this method overrides this instance's value for that parameter *only during
        that invocation of the method* (this instance's value does not change).

        >>> from operator import inv, add
        >>> with pool() as pool_:
        ...     pool_.mapreduce(m=inv, r=add, xs=range(3))
        -6
        """
        # A :obj:`ValueError` is returned to maintain consistency with the
        # behavior of the underlying :obj:`multiprocessing` ``Pool`` object.
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
            xss = _parts(xs, stages)

            # Perform each stage sequentially.
            result = None
            for xs_ in (progress(xss) if progress is not None else xss):
                result_stage = self._reduce(r, self._map(m, xs_))
                result = result_stage if result is None else r(result, result_stage)

        # Release resources if directed to do so.
        if close:
            self.close()

        return result

    def mapconcat(
            self: pool,
            m: Callable[..., Sequence],
            xs: Iterable,
            stages: Optional[int] = None,
            progress: Optional[Callable[[Iterable], Iterable]] = None,
            close: Optional[bool] = None
        ):
        """
        Perform the map operation ``m`` over the elements in the iterable
        ``xs`` (optionally in stages on subsequences of the data) and then
        release resources if directed to do so.

        :param m: Operation to be applied to each element in the input iterable.
        :param xs: Input to process using the map operation.
        :param stages: Number of stages (progress updates are provided once per stage).
        :param progress: Function that wraps an iterable (can be used to also report progress).
        :param close: Flag indicating whether this instance should be closed after one workflow.

        In contrast to the :obj:`pool.mapreduce` method, the map operation ``m``
        *must return a* :obj:`~collections.abc.Sequence`, as the results of this operation are
        combined using :obj:`operator.concat`.

        The ``stages``, ``progress``, and ``close`` parameter values each revert by
        default to those of this :obj:`pool` instance if they are not explicitly
        supplied. Supplying a value for any one of these parameters when invoking
        this method overrides this instance's value for that parameter *only during
        that invocation of the method* (this instance's value does not change).

        >>> with pool() as pool_:
        ...     pool_.mapconcat(m=tuple, xs=[[1], [2], [3]])
        (1, 2, 3)
        """
        return self.mapreduce(m, concat, xs, stages, progress, close)

    def close(self: pool):
        """
        Prevent any additional work from being added to this instance and
        release resources associated with this instance.

        >>> from operator import inv
        >>> pool_ = pool()
        >>> pool_.close()
        >>> pool_.mapconcat(m=inv, xs=range(3))
        Traceback (most recent call last):
          ...
        ValueError: Pool not running
        """
        self._closed = True
        if self._processes != 1:
            self._pool.close()

    def closed(self: pool) -> bool:
        """
        Return a boolean indicating whether this instance has been closed.

        >>> pool_ = pool()
        >>> pool_.close()
        >>> pool_.closed()
        True
        """
        if self._processes == 1:
            return self._closed

        return (
            self._closed or
            self._pool._state in ('CLOSE', 'TERMINATE') # pylint: disable=protected-access
        )

    def terminate(self: pool):
        """

        .. |Pool| replace:: ``Pool``
        .. _Pool: https://docs.python.org/3/library/multiprocessing.html#using-a-pool-of-workers

        Terminate the underlying :obj:`multiprocessing` |Pool|_ instance
        (associated resources will eventually be released, or they will be
        released when the instance is closed).
        """
        self._closed = True
        self._terminated = True
        if self._processes != 1:
            self._pool.terminate()

    def cpu_count(self: pool) -> int:
        """
        Return number of available CPUs.

        >>> with pool() as pool_:
        ...     isinstance(pool_.cpu_count(), int)
        True
        """
        return multiprocessing.cpu_count()

    def __len__(self: pool) -> int:
        """
        Return number of processes supplied as a configuration parameter
        when this instance was created.

        >>> with pool(1) as pool_:
        ...     len(pool_)
        1
        """
        return self._processes

def mapreduce(
        m: Callable[..., Any],
        r: Callable[..., Any],
        xs: Iterable,
        processes: Optional[int] = None,
        stages: Optional[int] = None,
        progress: Optional[Callable[[Iterable], Iterable]] = None
    ):
    """
    One-shot function for performing a workflow (no explicit object
    management or resource allocation is required on the user's part).

    :param m: Operation to be applied to each element in the input iterable.
    :param r: Operation that can combine two outputs from itself, the map operation, or a mix.
    :param xs: Input to process using the map and reduce operations.
    :param processes: Number of processes to allocate and to employ in executing the workflow.
    :param stages: Number of stages (progress updates are provided once per stage).
    :param progress: Function that wraps an iterable (can be used to also report progress).

    >>> from operator import inv, add
    >>> mapreduce(m=inv, r=add, xs=range(3))
    -6
    """
    if processes == 1:
        if stages is not None:
            xss = _parts(xs, stages) # Create one part per stage.
            return reduce(r, [
                m(x)
                for xs in (progress(xss) if progress is not None else xss)
                for x in xs
            ])

        return reduce(r, [m(x) for x in xs])

    pool_ = pool() if processes is None else pool(processes)
    return pool_.mapreduce(m, r, xs, stages=stages, progress=progress, close=True)

def mapconcat(
        m: Callable[..., Sequence],
        xs: Iterable,
        processes: Optional[int] = None,
        stages: Optional[int] = None,
        progress: Optional[Callable[[Iterable], Iterable]] = None
    ):
    """
    One-shot function for applying an operation across an iterable and
    assembling the results back into a :obj:`list` (no explicit object
    management or resource allocation is required on the user's part).

    :param m: Operation to be applied to each element in the input iterable.
    :param xs: Input to process using the map and reduce operations.
    :param processes: Number of processes to allocate and to employ in executing the workflow.
    :param stages: Number of stages (progress updates are provided once per stage).
    :param progress: Function that wraps an iterable (can be used to also report progress).

    In contrast to the :obj:`mapreduce` function, the map operation ``m``
    *must return a* :obj:`~collections.abc.Sequence`, as the results of this operation are
    combined using :obj:`operator.concat`.

    >>> mapconcat(m=list, xs=[[1], [2], [3]])
    [1, 2, 3]
    """
    return mapreduce(m, concat, xs, processes, stages=stages, progress=progress)

if __name__ == '__main__':
    doctest.testmod() # pragma: no cover
