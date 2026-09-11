#
# ro_asyncio.py -- serve RPC from an event loop, handle it on a thread pool
#
"""A server shape for bursty traffic.

The threaded carriers give every accepted connection a thread of its own.
That is cheap per connection and honest about a client that stalls -- it
costs one thread rather than blocking the service -- but a burst of a
thousand connections costs a thousand threads, and Gen2 processes already
carry several large pools.

This runs the *I/O* on one event loop thread, where a connection costs a
coroutine, and hands each request to a thread pool to be handled.  The pool
is not optional here: a Gen2 pubsub runs local subscriber callbacks inline
inside ``remote_update`` (see :py:meth:`PubSub.PubSub.subscribe_cb`), which
is arbitrary synchronous application code, and running that on the loop
would stall every other connection at once.

The pool may be a :py:class:`concurrent.futures.ThreadPoolExecutor` or a
Gen2 :py:class:`g2base.Task.ThreadPool` wrapped by
:py:class:`ro_executor.ThreadPoolExecutor`; both present ``submit``, which
is all that is wanted.
"""

import asyncio
import socket
import threading

from tinyrpc.server import AsyncioRPCServer
from tinyrpc.transports.tcp import AsyncioTcpServerTransport


class PreboundAsyncioTcpServerTransport(AsyncioTcpServerTransport):
    """An asyncio server transport that listens on a socket already bound.

    ``remoteObjectServer`` binds every port before it registers the service,
    so that the registration names ports that are already listening.  The
    stock asyncio transport binds inside ``start()``, which is a coroutine
    and so runs too late for that.  Handing it a bound socket keeps the
    existing order intact.
    """

    def __init__(self, sock, **kwargs):
        super().__init__(sock.getsockname()[:2], **kwargs)
        self._prebound = sock

    async def start(self):
        if self._server is not None:
            return
        self._server = await asyncio.start_server(self._serve_connection,
                                                  sock=self._prebound)
        try:
            await self._server.serve_forever()
        except asyncio.CancelledError:
            pass


class AsyncioServerRunner:
    """Present an :py:class:`AsyncioRPCServer` as a start/stop server.

    ``remoteObjectServer`` starts and stops its listeners synchronously, so
    the loop is owned here: :py:meth:`start` returns once it is running and
    :py:meth:`stop` waits for it to unwind.
    """

    def __init__(self, transport, protocol, dispatcher, executor,
                 ev_quit=None, logger=None):
        self.transport = transport
        self.protocol = protocol
        self.dispatcher = dispatcher
        self.executor = executor
        self.ev_quit = ev_quit
        self.logger = logger
        self.authenticator = None
        self._loop = None
        self._thread = None
        self._server = None
        self._running = threading.Event()

    def start(self):
        if self._thread is not None:
            return
        self._thread = threading.Thread(target=self._run,
                                        name='ro-asyncio-loop')
        self._thread.daemon = True
        self._thread.start()
        self._running.wait(timeout=10.0)

    def _run(self):
        loop = asyncio.new_event_loop()
        self._loop = loop
        asyncio.set_event_loop(loop)

        async def main():
            self._server = AsyncioRPCServer(self.transport, self.protocol,
                                            self.dispatcher,
                                            executor=self.executor)
            if self.authenticator is not None:
                self._server.authenticator = self.authenticator
            self._running.set()
            try:
                await self._server.serve_forever()
            except asyncio.CancelledError:
                pass

        try:
            loop.run_until_complete(main())
        except Exception:
            if self.logger is not None:
                self.logger.error('asyncio server stopped', exc_info=True)
        finally:
            self._running.set()          # never leave start() waiting
            self._close(loop)

    @staticmethod
    def _close(loop):
        """Let what is still pending finish before closing the loop.

        Closing underneath a coroutine that is waiting on a queue leaves it
        to cancel a getter on a loop that no longer exists, which surfaces
        as ``RuntimeError: Event loop is closed`` on the way out.  Stopping
        a service should not print a traceback.
        """
        try:
            pending = [task for task in asyncio.all_tasks(loop)
                       if not task.done()]
            for task in pending:
                task.cancel()
            if pending:
                loop.run_until_complete(
                    asyncio.gather(*pending, return_exceptions=True))
            loop.run_until_complete(loop.shutdown_asyncgens())
        except Exception:
            pass
        finally:
            try:
                loop.close()
            except Exception:
                pass

    def stop(self):
        """Ask the serve loop to finish, and wait for its thread.

        The loop is not stopped from underneath: the server's own stop sets
        its quit event and the transport's poll interval brings the serve
        loop back to check it, so it unwinds on its own.  Only a loop that
        will not come back that way is stopped outright.
        """
        loop, server = self._loop, self._server
        thread = self._thread
        if loop is None or server is None:
            return
        try:
            future = asyncio.run_coroutine_threadsafe(server.stop(), loop)
            future.result(timeout=5.0)
        except Exception:
            pass

        if thread is not None:
            thread.join(timeout=5.0)
            if thread.is_alive():
                # It did not come back to check; take the loop out from
                # under it rather than leaving the thread behind.
                loop.call_soon_threadsafe(loop.stop)
                thread.join(timeout=2.0)

        self._loop = None
        self._server = None
        self._thread = None
