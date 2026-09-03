#
# ro_executor.py -- run tinyrpc servers on a g2base Task.ThreadPool
#
"""tinyrpc's :py:class:`~tinyrpc.server.executor.RPCServerExecutor` wants a
:py:class:`concurrent.futures.Executor`, while Gen2 services pass around a
:py:class:`g2base.Task.ThreadPool`, usually one shared with the rest of the
application.  This adapts the latter to the former so that a service can go
on handing its own pool to :py:class:`~remoteObjects.remoteObjectServer` and
have RPC handlers run on it, as they do today.
"""

from concurrent.futures import Future

from g2base import Task


class ThreadPoolExecutor:
    """Present a :py:class:`g2base.Task.ThreadPool` as an Executor.

    Only :py:meth:`submit` is implemented, which is all a tinyrpc server
    uses.  Note that the pool is *not* owned: it is typically shared with the
    rest of the application, so :py:meth:`shutdown` deliberately does not
    stop it.  Stop the pool through its own ``stopall``, or through the
    ``ev_quit`` the pool and the server share.

    :param threadPool: The pool to run work on.  It must already be started,
        or be started before the first call is dispatched.
    """

    def __init__(self, threadPool):
        self.threadPool = threadPool

    def submit(self, fn, *args, **kwargs) -> Future:
        """Run ``fn`` on the thread pool and report the outcome in a Future.

        The Future is what an Executor is expected to return; the tinyrpc
        servers ignore it, but a caller that does look is not misled about
        whether the work succeeded.
        """
        future = Future()

        def run():
            if not future.set_running_or_notify_cancel():
                return
            try:
                future.set_result(fn(*args, **kwargs))
            except BaseException as e:
                future.set_exception(e)

        self.threadPool.addTask(Task.FuncTask2(run))
        return future

    def shutdown(self, wait=True, *, cancel_futures=False):
        """Do nothing: the pool belongs to the caller, not to us."""
        return

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.shutdown()
        return False
