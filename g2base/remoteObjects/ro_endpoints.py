#
# ro_endpoints.py -- where a service's providers are, and how to call them
#
"""Two independent things decide how a remote call is made:

* **where the providers are** -- a list given up front, or whatever the name
  service currently reports, which can be asked again when one stops
  answering; and
* **what to do with them** -- call one and fail over to another if it does
  not answer, or call all of them and collect the results.

The old ``servicePack`` plus the ``remoteObjectSP*`` class hierarchy bound
those two together with inheritance, so only three of the four combinations
existed and the missing one was the useful one: the manager service wants to
call *all* providers, so it could not use the name-service-backed class and
had to build its endpoint list by hand.  Here the two are separate and
compose, so a name-service-backed call-all costs nothing.

``servicePack`` also grew a third job, probing providers with ``ro_echo`` to
find live ones.  That is gone.  Nothing called it -- and two of its three
methods raised NameError on the first line, which is how we know -- and it
duplicated, less well, the ping and purge the name service already does.
"""

import threading


class Endpoints:
    """The providers of one service.

    Subclasses decide where the list comes from.  Instances are shared
    between calling threads, so access to the resolved list is locked.
    """

    def __init__(self, name, logger=None):
        self.name = name
        self.logger = logger
        self._lock = threading.RLock()
        self._clients = None

    def _resolve(self):
        """Return a fresh list of clients.  Implemented by subclasses."""
        raise NotImplementedError

    @staticmethod
    def _release(clients):
        """Let go of clients that are no longer ours to use.

        Asked of the *class* rather than the instance on purpose.  A client's
        unknown attributes are remote method names, so
        ``getattr(client, 'close', None)`` does not report whether the client
        can be closed -- it hands back a closure that would call a method
        named ``close`` on the far end.  Looking the name up on the type
        cannot be answered that way.
        """
        for client in clients:
            release = getattr(type(client), 'ro_release', None)
            if release is None:
                continue
            try:
                release(client)
            except Exception:
                pass

    def clients(self):
        """The current providers, resolving them if that has not happened."""
        with self._lock:
            if self._clients is None:
                self._clients = self._resolve()
            return list(self._clients)

    def refresh(self):
        """Resolve again, releasing what we thought we knew.

        Called when a provider stops answering: the set of providers may have
        changed, and the name service is the authority on it.

        The clients being discarded are released rather than dropped.  For
        the connectionless carriers that is nothing either way, since they
        hold no connection between calls; for one that holds a connection it
        is the difference between a socket this process will reuse and one it
        will keep open until it exits, and a multiplexing client also has a
        reply-collecting thread that holds a reference to itself, so nothing
        would ever collect it.

        A thread that is mid-call on a discarded client will see that call
        fail.  That is the same outcome as the provider being down, which is
        what prompted the refresh, and it reaches the same failover path --
        so it recovers by the route it would have taken anyway.
        """
        with self._lock:
            # _resolve() first: if it raises, the clients we have are still
            # the best we know and must not be released.
            previous, self._clients = self._clients, self._resolve()
            result = list(self._clients)
        # Released outside the lock, since closing a socket is not something
        # to hold a lock other callers need.
        self._release(previous or [])
        return result

    def close(self):
        """Release every client, and resolve again on the next use.

        A proxy that is finished with, or one being reconfigured, has no
        other way to give back what its clients hold.
        """
        with self._lock:
            previous, self._clients = self._clients, None
        self._release(previous or [])

    def __len__(self):
        return len(self.clients())

    def __repr__(self):
        with self._lock:
            known = 'unresolved' if self._clients is None \
                else '%d provider(s)' % len(self._clients)
        return '<%s %s: %s>' % (type(self).__name__, self.name, known)


class StaticEndpoints(Endpoints):
    """Providers given as an explicit list of ``(host, port)`` pairs.

    :py:meth:`refresh` re-builds the clients but cannot discover new
    providers: the caller named the ones it meant.
    """

    def __init__(self, name, hostports, make_client, logger=None):
        super().__init__(name, logger=logger)
        self.hostports = list(hostports)
        self.make_client = make_client

    def _resolve(self):
        return [self.make_client(host, port) for host, port in self.hostports]


class NameSvcEndpoints(Endpoints):
    """Providers looked up in the name service, by service name.

    Each registration carries the transport and encoding that provider
    speaks, so a service pack may legitimately hold providers speaking
    different protocols; ``make_client`` is given the whole record.
    """

    def __init__(self, name, ns, make_client, logger=None):
        super().__init__(name, logger=logger)
        self.ns = ns
        self.make_client = make_client

    def _resolve(self):
        if not self.ns:
            raise LookupError(
                "no name server configured, so '%s' cannot be looked up"
                % (self.name,))

        info = self.ns.getInfo(self.name)
        if not info:
            raise LookupError(
                "no remote object server found for '%s'" % (self.name,))

        return [self.make_client(rec) for rec in info]
