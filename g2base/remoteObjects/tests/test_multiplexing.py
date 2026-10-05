#
# test_multiplexing.py -- several calls in flight over one connection
#
"""A connection held open, and calls that overlap on it.

Every other client here dials for each call, so a caller waits a full round
trip and the next call cannot start until this one finishes.  This one holds
the connection and tells replies apart by the correlation id g2rpc puts on
them, so independent calls overlap.

Holding a connection is what makes reconnection necessary: a connection that
is never held cannot go stale.  Both halves are covered here.
"""

import threading
import time

import pytest

from g2base.remoteObjects import remoteObjects as ro
from g2base.remoteObjects import ro_transport

HOST = '127.0.0.1'
TRANSPORT = 'g2rpc-tcp-persistent'


class ServiceObject:
    def echo(self, value):
        return value

    def add(self, a, b, c=0):
        return a + b + c

    def slow(self, value, seconds=0.3):
        time.sleep(seconds)
        return value

    def boom(self):
        raise ValueError('kaboom')


@pytest.fixture
def service():
    """Services that can be stopped and restarted on the same port."""
    started = []

    def _make(port=None, **kwargs):
        kwargs.setdefault('svcname', None)
        kwargs.setdefault('name', 'muxsvc')
        kwargs.setdefault('obj', ServiceObject())
        kwargs.setdefault('host', HOST)
        kwargs.setdefault('logger', ro.nullLogger())
        kwargs.setdefault('usethread', True)
        kwargs.setdefault('ns', False)
        kwargs.setdefault('default_auth', False)
        kwargs.setdefault('transport', TRANSPORT)
        kwargs.setdefault('numthreads', 8)
        kwargs.setdefault('method_list', ['echo', 'add', 'slow', 'boom'])
        svc = ro.remoteObjectServer(port=port, **kwargs)
        svc.ro_start(wait=True, timeout=10.0)
        started.append(svc)
        return svc

    yield _make

    for svc in started:
        try:
            svc.ro_stop(wait=True, timeout=10.0)
        except Exception:
            pass


@pytest.fixture
def client():
    made = []

    def _make(svc, **kwargs):
        handle = ro.multiplexingClient(HOST, svc.port, name='muxsvc',
                                       timeout=20.0, **kwargs)
        handle.start()
        made.append(handle)
        return handle

    yield _make

    for handle in made:
        try:
            handle.stop()
        except Exception:
            pass


# ------------------------------------------------------------- the basics --

def test_it_makes_ordinary_calls(service, client):
    svc = service()
    handle = client(svc)

    assert handle.echo('hi') == 'hi'
    assert handle.echo(None) is None
    assert handle.echo(2 ** 70) == 2 ** 70
    assert handle.add(1, 2, c=3) == 6


def test_a_failing_method_reaches_the_caller(service, client):
    svc = service()
    with pytest.raises(ro.remoteObjectError) as excinfo:
        client(svc).boom()
    assert 'kaboom' in str(excinfo.value)


def test_it_refuses_a_protocol_that_cannot_multiplex():
    """Dialling per call means one in flight at a time and nothing to
    multiplex, so asking for it is a mistake worth naming."""
    with pytest.raises(ro.remoteObjectError) as excinfo:
        ro.multiplexingClient(HOST, 8000, transport='g2rpc-tcp')
    assert 'nothing to multiplex' in str(excinfo.value)


def test_only_a_persistent_transport_claims_to_multiplex():
    assert ro_transport.get(TRANSPORT).supports_multiplexing
    for name in ('g2rpc', 'g2rpc-tcp', 'g2rpc-zmq', 'xmlrpc'):
        assert not ro_transport.get(name).supports_multiplexing


# ------------------------------------------------------------ overlapping --

def test_calls_overlap_rather_than_queueing(service, client):
    """The point of the whole thing: six calls that each sleep 0.3s should
    take about 0.3s between them, not 1.8s."""
    svc = service()
    handle = client(svc)

    started = time.time()
    pending = [handle.begin_call('slow', (n,)) for n in range(6)]
    results = [handle.collect(p) for p in pending]
    elapsed = time.time() - started

    assert results == list(range(6))
    assert elapsed < 1.2, (
        "six 0.3s calls took %.2fs, so they did not overlap" % elapsed)


def test_replies_are_matched_to_their_own_calls(service, client):
    """Several calls outstanding on one connection, answered in whatever
    order the service gets to them."""
    svc = service()
    handle = client(svc)

    pending = [(n, handle.begin_call('echo', ('value-%d' % n,)))
               for n in range(20)]
    for n, p in pending:
        assert handle.collect(p) == 'value-%d' % n


def test_concurrent_callers_share_one_connection(service, client):
    svc = service()
    handle = client(svc)

    wrong, lock = [], threading.Lock()

    def work(tid):
        for i in range(15):
            want = '%d-%d' % (tid, i)
            got = handle.echo(want)
            if got != want:
                with lock:
                    wrong.append((want, got))

    threads = [threading.Thread(target=work, args=(t,)) for t in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=60)

    assert not wrong, "replies were crossed: %r" % (wrong[:3],)


def test_the_tracking_board_empties(service, client):
    """Every completed call must be forgotten, or a long-lived client leaks
    one entry per call."""
    svc = service()
    handle = client(svc)

    for _ in range(10):
        handle.echo('hi')
    assert handle.client.tracking_board == {}


# ---------------------------------------------------------- reconnection --

def test_it_dials_again_after_the_service_restarts(service, client):
    """Holding a connection is what makes this necessary: a service can be
    restarted underneath a client that outlives it."""
    svc = service()
    handle = client(svc)
    assert handle.echo('before') == 'before'

    port = svc.port
    svc.ro_stop(wait=True, timeout=10.0)

    with pytest.raises(ro.remoteObjectError):
        handle.echo('while it is down')

    service(port=port)
    time.sleep(0.6)             # past the transport's reconnect interval

    deadline = time.time() + 15.0
    while time.time() < deadline:
        try:
            assert handle.echo('after') == 'after'
            return
        except ro.remoteObjectError:
            time.sleep(0.2)
    pytest.fail("never reconnected after the service came back")


def test_it_connects_even_if_the_service_starts_later(service, client):
    """The connection is dialled on the first call, not at construction, so
    a client and a service can still be started in either order."""
    from tinyrpc.transports.tcp import NonBlockingTcpClientTransport

    # Nothing is listening yet.
    transport = NonBlockingTcpClientTransport((HOST, 9))
    assert not transport.connected, "must not dial until it has to"
    transport.close()

    svc = service()
    handle = client(svc)
    assert handle.echo('hi') == 'hi'


def test_a_lost_connection_does_not_leak_the_call(service, client):
    """A call in flight when the connection dies cannot be answered.  It has
    to time out and be forgotten, not sit on the board for ever."""
    svc = service()
    handle = client(svc)
    handle.echo('warm up')

    svc.ro_stop(wait=True, timeout=10.0)
    with pytest.raises(ro.remoteObjectError):
        handle.echo('lost')

    assert handle.client.tracking_board == {}


# ------------------------------- asking for it through a normal proxy --
#
# The pattern this exists for in Gen2 is several threads of a pool sharing
# one proxy, each making an ordinary blocking call.  They need no new idiom:
# '+multiplex' on the transport string selects the client, and
# proxy.method() goes on meaning what it meant.


@pytest.fixture
def nameservice():
    from g2base.remoteObjects import remoteObjectNameSvc as ns_mod
    return ns_mod.remoteObjectNameService('names', ro.nullLogger(), HOST)


@pytest.fixture
def registered(nameservice):
    started = []

    def _make(transport=TRANSPORT):
        svc = ro.remoteObjectServer(
            svcname='muxsvc', obj=ServiceObject(), host=HOST,
            logger=ro.nullLogger(), usethread=True, ns=nameservice,
            default_auth=False, transport=transport, numthreads=16,
            method_list=['echo', 'add', 'slow', 'boom'])
        svc.ro_start(wait=True, timeout=10.0)
        started.append(svc)
        return svc

    yield _make
    for svc in started:
        try:
            svc.ro_stop(wait=True, timeout=10.0)
        except Exception:
            pass


def proxy_for(nameservice, transport):
    return ro.remoteObjectProxy('muxsvc', ns=nameservice, transport=transport,
                                default_auth=False, logger=ro.nullLogger())


def test_the_string_selects_the_multiplexing_client(registered, nameservice):
    registered()
    proxy = proxy_for(nameservice, 'g2rpc/tcp-persistent+multiplex')

    assert proxy.echo('hello') == 'hello'
    assert type(proxy.endpoints.clients()[0]) is ro.multiplexingClient
    proxy.close()


def test_without_it_the_plain_client_is_built(registered, nameservice):
    registered()
    proxy = proxy_for(nameservice, 'g2rpc/tcp-persistent')

    assert proxy.echo('hello') == 'hello'
    assert type(proxy.endpoints.clients()[0]) is ro.remoteObjectClient
    proxy.close()


def test_the_pin_is_the_registered_name_either_way(registered, nameservice):
    """'+multiplex' is not part of what the service registered, so it must
    not leak into the name choose_endpoint matches against."""
    registered()
    muxed = proxy_for(nameservice, 'g2rpc/tcp-persistent+multiplex')
    plain = proxy_for(nameservice, 'g2rpc/tcp-persistent')

    assert muxed.transport == plain.transport == 'g2rpc-tcp-persistent'
    assert muxed.multiplex and not plain.multiplex
    muxed.close()
    plain.close()


def test_pool_threads_sharing_one_proxy_get_their_own_replies(registered,
                                                              nameservice):
    """The case it is for.  Every call is an ordinary blocking
    proxy.method(); what differs is that one connection carries them all."""
    registered()
    proxy = proxy_for(nameservice, 'g2rpc/tcp-persistent+multiplex')
    proxy.echo('warm')

    wrong, lock = [], threading.Lock()

    def work(tid):
        for i in range(20):
            want = '%d-%d' % (tid, i)
            got = proxy.echo(want)
            if got != want:
                with lock:
                    wrong.append((want, got))

    threads = [threading.Thread(target=work, args=(t,), daemon=True)
               for t in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=60)

    assert not wrong, 'replies were crossed: %r' % (wrong[:3],)
    client = proxy.endpoints.clients()[0]
    transport = vars(client)['rpc_transport']
    assert transport.connected, 'one connection carried all of it'
    proxy.close()


def test_a_service_refuses_to_be_told_to_multiplex(nameservice):
    """It cannot tell a caller that multiplexes from one that does not, so
    there is nothing for it to do with this."""
    with pytest.raises(ro.remoteObjectError) as caught:
        ro.remoteObjectServer(
            svcname='muxsvc', obj=ServiceObject(), host=HOST,
            logger=ro.nullLogger(), usethread=True, ns=nameservice,
            default_auth=False, method_list=['echo'],
            transport='g2rpc/tcp-persistent+multiplex')
    assert "caller's choice" in str(caught.value)


def test_a_carrier_that_dials_per_call_is_refused(registered, nameservice):
    """Checked by the grammar, before anything is built."""
    with pytest.raises(ro_transport.UnknownTransport) as caught:
        proxy_for(nameservice, 'g2rpc/tcp+multiplex')
    assert 'nothing to multiplex' in str(caught.value)


def test_a_refusing_service_is_still_told_apart_from_a_silent_one(
        registered, nameservice):
    """call_failover classifies from the original exception, so a
    multiplexed client must not wrap it the way attribute access does."""
    registered()
    proxy = proxy_for(nameservice, 'g2rpc/tcp-persistent+multiplex')

    with pytest.raises(ro.remoteObjectError) as caught:
        proxy.boom()
    assert 'kaboom' in str(caught.value)
    assert proxy.echo('still here') == 'still here', 'it did not fail over'
    proxy.close()


def test_closing_the_proxy_releases_the_client(registered, nameservice):
    """Whatever the client was holding -- a connection, and a collecting
    thread if the transport did not deliver replies itself -- closing the
    proxy has to give back."""
    registered()
    proxy = proxy_for(nameservice, 'g2rpc/tcp-persistent+multiplex')
    proxy.echo('a')
    client = proxy.endpoints.clients()[0]
    collector = vars(client)['_thread']
    transport = vars(client)['rpc_transport']
    assert transport.connected

    proxy.close()

    if collector is not None:
        collector.join(timeout=10)
        assert not collector.is_alive()
    assert not transport.connected


def test_the_reader_thread_delivers_rather_than_a_second_thread(registered,
                                                                nameservice):
    """The reply path, and why there is no collecting thread to stop.

    The transport's reader thread already holds a whole reply; handing it to
    a queue meant waking a second thread to parse it and give it to the
    waiting call.  It does that itself now, which is one thread and one
    wakeup per reply fewer.
    """
    registered()
    proxy = proxy_for(nameservice, 'g2rpc/tcp-persistent+multiplex')
    assert proxy.echo('a') == 'a'
    client = proxy.endpoints.clients()[0]

    assert vars(client)['client'].pushed, 'the client is not being delivered to'
    assert vars(client)['_thread'] is None, (
        'a collecting thread was started with nothing for it to do')
    assert vars(client)['rpc_transport'].delivers_to_callback
    proxy.close()


def test_it_works_over_the_asyncio_server_too(registered, nameservice):
    """The combination with no thread per connection on either count: a
    coroutine for the connection, and one connection for the pool."""
    registered(transport='g2rpc-tcp-asyncio-persistent')
    proxy = proxy_for(nameservice,
                      'g2rpc/tcp-asyncio-persistent+multiplex')

    assert proxy.echo('hello') == 'hello'
    assert type(proxy.endpoints.clients()[0]) is ro.multiplexingClient
    proxy.close()


# ----------------------------------------- one call path, two client kinds --

def test_both_client_kinds_make_a_call_the_same_way(service, client):
    """call_remote holds whichever client an Endpoints produced, so there has
    to be one call it can make without knowing which kind it has.  It used to
    reach in as client.proxy.call(...) -- a plain client's internals -- and a
    multiplexing client has no 'proxy'.  Worse than absent: unknown
    attributes are the service's method names, so asking for one returned a
    closure for a remote method called 'proxy'."""
    svc = service()
    muxed = client(svc, transport=TRANSPORT, default_auth=False)
    plain = ro.remoteObjectClient(HOST, svc.port, name='muxsvc',
                                  default_auth=False, transport=TRANSPORT,
                                  timeout=20.0)

    for handle in (plain, muxed):
        assert handle.ro_call('echo', ('a',), {}) == 'a', type(handle).__name__
        assert ro.call_remote(handle, 'add', (1, 2), {'c': 3}) == (ro.OK, 6)


def test_a_refusal_is_not_dressed_up_as_a_transport_failure(service, client):
    """call_remote tells "did not answer" from "answered and refused" by the
    exception's type, so ro_call must not wrap it -- attribute access does,
    and a remoteObjectError is in neither tuple, which would make every
    refusal look like something worth failing over for."""
    svc = service()
    muxed = client(svc, transport=TRANSPORT, default_auth=False)
    plain = ro.remoteObjectClient(HOST, svc.port, name='muxsvc',
                                  default_auth=False, transport=TRANSPORT,
                                  timeout=20.0)

    for handle in (plain, muxed):
        flag, _message = ro.call_remote(handle, 'boom', (), {})
        assert flag == ro.ERROR_FATAL, (
            '%s: a service that answered and refused should not be retried '
            'elsewhere' % (type(handle).__name__,))
