#
# test_endpoint_lifecycle.py -- giving back what a proxy's clients hold
#
"""A proxy resolves its providers into clients and replaces them when one
stops answering.  Replacing is not the same as giving back what they held.

For the connectionless carriers there is nothing to give back, which is why
this went unnoticed: a client that dials per call holds no connection
between calls.  A carrier that holds one is the opposite, and a multiplexing
client holds a reply-collecting thread as well -- a thread that refers to the
client, so dropping the last other reference does not collect it.

The awkward part is that a client cannot simply be asked whether it can be
closed.  Its unknown attributes are the service's method names, so
``getattr(client, 'close', None)`` returns a closure that would call
``close()`` on the far end rather than telling you anything.  That is what
``ro_release``, looked up on the type, is for.
"""

import pytest

from g2base.remoteObjects import remoteObjectNameSvc as ns_mod
from g2base.remoteObjects import remoteObjects as ro
from g2base.remoteObjects import ro_endpoints

HOST = '127.0.0.1'
HELD = 'g2rpc-tcp-persistent'
PER_CALL = 'g2rpc-tcp'


class Service:
    def echo(self, value):
        return value


@pytest.fixture
def nameservice():
    return ns_mod.remoteObjectNameService('names', ro.nullLogger(), HOST)


@pytest.fixture
def service(nameservice):
    started = []

    def _make(transport=HELD):
        server = ro.remoteObjectServer(
            svcname='life', obj=Service(), host=HOST, logger=ro.nullLogger(),
            usethread=True, ns=nameservice, default_auth=False,
            transport=transport, numthreads=16, method_list=['echo'])
        server.ro_start(wait=True, timeout=15)
        started.append(server)
        return server

    yield _make
    for server in started:
        try:
            server.ro_stop(wait=True, timeout=15)
        except Exception:
            pass


def proxy_for(nameservice, transport):
    return ro.remoteObjectProxy('life', ns=nameservice, transport=transport,
                                default_auth=False, logger=ro.nullLogger())


def transports_of(client):
    """The transports a client is holding, without going through
    __getattr__ -- which would make a remote call named 'proxy'."""
    return list(vars(vars(client)['proxy'])['_all'])


# --------------------------------------------- what refresh gives back --

def test_refresh_releases_the_clients_it_discards(service, nameservice):
    """The clients are replaced either way; this is about the connection the
    discarded one was holding."""
    service()
    proxy = proxy_for(nameservice, HELD)
    assert proxy.echo('before') == 'before'

    discarded = proxy.endpoints.clients()[0]
    held = transports_of(discarded)
    assert held, 'the client should be holding one'
    # This client's own readers, not every thread in the process that happens
    # to carry the same name: the suite exercises other held-connection
    # carriers, and a set taken by name counts theirs as ours.
    mine = [t._reader for t in held if getattr(t, '_reader', None) is not None]
    assert mine, 'a held connection has a reader thread'

    proxy.endpoints.refresh()

    assert transports_of(discarded) == [], 'the connection was not given back'
    for reader in mine:
        reader.join(timeout=5)
    assert not any(r.is_alive() for r in mine), 'a reader outlived its client'
    assert proxy.echo('after') == 'after', 'the proxy still works'


def test_refresh_keeps_what_it_has_when_resolution_fails(nameservice):
    """Nothing is released on the strength of a lookup that did not
    succeed: the clients in hand are still the best we know."""
    released = []

    class Client:
        def ro_release(self):
            released.append(self)

    first = [Client()]
    answers = [first]

    class Flaky(ro_endpoints.Endpoints):
        def _resolve(self):
            if answers:
                return answers.pop(0)
            raise LookupError('the name service is unreachable')

    endpoints = Flaky('life')
    assert endpoints.clients() == first

    with pytest.raises(LookupError):
        endpoints.refresh()

    assert released == [], 'released clients it then had to keep using'
    assert endpoints.clients() == first


# ------------------------------------------------------ what close does --

def test_close_gives_back_the_connections(service, nameservice):
    service()
    proxy = proxy_for(nameservice, HELD)
    assert proxy.echo('a') == 'a'
    client = proxy.endpoints.clients()[0]
    assert transports_of(client)

    proxy.close()

    assert transports_of(client) == []


def test_a_closed_proxy_resolves_again_on_the_next_call(service,
                                                        nameservice):
    """Closing gives back the connections, not the proxy."""
    service()
    proxy = proxy_for(nameservice, HELD)
    assert proxy.echo('a') == 'a'

    proxy.close()

    assert proxy.echo('b') == 'b'


def test_closing_a_connectionless_proxy_is_harmless(service, nameservice):
    """Which is why this went unnoticed: there is nothing held to give
    back, and the next call dials as it always did."""
    service(transport=PER_CALL)
    proxy = proxy_for(nameservice, PER_CALL)
    assert proxy.echo('a') == 'a'

    proxy.close()

    assert proxy.echo('b') == 'b'


# ------------------------------------- the collector has to be stopped --

def test_releasing_a_multiplexing_client_stops_its_collector(service,
                                                             nameservice):
    """It holds a thread as well as a connection, and that thread holds a
    reference to the client, so letting go of the client is not enough."""
    server = service()
    client = ro.multiplexingClient(HOST, server.port, name='life',
                                   default_auth=False, transport=HELD,
                                   timeout=15)
    assert client.echo('a') == 'a'
    collector = vars(client)['_thread']
    assert collector is not None and collector.is_alive()

    ro_endpoints.Endpoints._release([client])

    collector.join(timeout=10)
    assert not collector.is_alive(), 'the collector thread outlived it'


def test_both_client_kinds_answer_to_the_same_name():
    """What lets a holder of mixed clients release them without knowing
    which kind each is."""
    for cls in (ro.remoteObjectClient, ro.multiplexingClient):
        assert callable(getattr(cls, 'ro_release', None)), cls.__name__


# ------------------------------- asking the type, not the instance --

def test_releasing_does_not_call_the_service(service, nameservice):
    """The trap this avoids.  A client forwards unknown attributes to the
    far end, so asking an *instance* whether it has a 'close' or an
    'ro_release' is answered with a closure either way -- and calling it
    would be a remote call, on a provider we have just decided to stop
    using."""
    asked = []

    class ClientLikeProxy:
        """Forwards everything it does not have, as a client does."""

        def __getattr__(self, name):
            asked.append(name)

            def call(*args, **kwargs):
                asked.append('CALLED %s' % (name,))
            return call

    ro_endpoints.Endpoints._release([ClientLikeProxy()])

    assert not any(a.startswith('CALLED') for a in asked), (
        'made a remote call while releasing: %r' % (asked,))


def test_a_client_that_cannot_be_released_is_simply_left(service,
                                                         nameservice):
    """Nothing in the registry has to grow a release method for this to be
    safe to call."""
    class Plain:
        pass

    ro_endpoints.Endpoints._release([Plain(), Plain()])
