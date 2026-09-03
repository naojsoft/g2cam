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
