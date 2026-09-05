#
# test_transport_reuse.py -- holding a connection, or not
#
"""Whether a client keeps its connection between calls is per-carrier, and
getting it wrong is expensive in one direction and incorrect in the other.

Dialling every time costs a connection per call.  Sharing one held connection
between threads costs correctness: a held TCP connection carries no way to tell
replies apart, so a thread waiting on one takes whichever arrives first --
possibly another thread's.  The answer is one connection per calling thread.
"""

import threading

import pytest

from g2base.remoteObjects import remoteObjects as ro
from g2base.remoteObjects import ro_transport

HOST = '127.0.0.1'


class Service:
    def echo(self, value):
        return value

    def slow_echo(self, value):
        import time
        time.sleep(0.05)
        return value


@pytest.fixture
def service():
    started = []

    def _make(transport):
        server = ro.remoteObjectServer(
            svcname=None, name='reuse', obj=Service(), host=HOST,
            logger=ro.nullLogger(), usethread=True, ns=False,
            default_auth=False, transport=transport, numthreads=16,
            method_list=['echo', 'slow_echo'])
        server.ro_start(wait=True, timeout=15)
        started.append(server)
        return server

    yield _make
    for server in started:
        try:
            server.ro_stop(wait=True, timeout=15)
        except Exception:
            pass


def client_for(server, transport):
    return ro.remoteObjectClient(HOST, server.port, name='reuse',
                                 default_auth=False, transport=transport,
                                 timeout=30)


# ------------------------------------------------------ what each wants --

def test_a_held_connection_is_actually_held():
    """Regression.  The spec said persistent and the proxy dialled anyway, so
    it paid for a connection *and* a reader thread on every call and threw
    both away -- making the carrier named "persistent" the slowest of the
    three rather than the fastest."""
    spec = ro_transport.get('g2rpc-tcp-persistent')
    assert spec.reuse_client_transport
    assert spec.client_transport_per_thread


def test_the_connectionless_carrier_still_dials_every_time():
    """Which is what lets a client and a service restart in any order."""
    spec = ro_transport.get('g2rpc-tcp')
    assert not spec.reuse_client_transport


def test_zmq_shares_one_transport_between_threads():
    """It keeps a socket per thread inside the transport already."""
    spec = ro_transport.get('g2rpc-zmq')
    assert spec.reuse_client_transport
    assert not spec.client_transport_per_thread


# -------------------------------------------------- replies stay put --

@pytest.mark.parametrize('transport', ['g2rpc-tcp-persistent', 'g2rpc-zmq',
                                       'g2rpc-tcp'])
def test_concurrent_callers_each_get_their_own_replies(service, transport):
    """The reason a held connection is not simply shared.

    Every thread asks for something only it asked for, so a crossed reply
    shows up as a wrong answer rather than as a hang.
    """
    server = service(transport)
    client = client_for(server, transport)

    wrong = []
    barrier = threading.Barrier(6)

    def hammer(tag):
        barrier.wait()
        for i in range(40):
            want = '%s-%d' % (tag, i)
            got = client.echo(want)
            if got != want:
                wrong.append((want, got))

    threads = [threading.Thread(target=hammer, args=('t%d' % n,))
               for n in range(6)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=60)
        assert not t.is_alive(), 'a caller never finished'

    assert not wrong, 'replies were crossed: %r' % (wrong[:4],)


def test_overlapping_slow_calls_do_not_cross(service):
    """The same, with the replies deliberately in flight together."""
    transport = 'g2rpc-tcp-persistent'
    server = service(transport)
    client = client_for(server, transport)

    wrong = []
    barrier = threading.Barrier(4)

    def call(tag):
        barrier.wait()
        for i in range(5):
            want = '%s-%d' % (tag, i)
            got = client.slow_echo(want)
            if got != want:
                wrong.append((want, got))

    threads = [threading.Thread(target=call, args=('s%d' % n,))
               for n in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=60)

    assert not wrong, 'replies were crossed: %r' % (wrong[:4],)


# ------------------------------------------------------------ lifecycle --

def test_a_held_connection_is_re_dialled_after_the_service_restarts(service):
    """Holding a connection is what makes this necessary."""
    transport = 'g2rpc-tcp-persistent'
    server = service(transport)
    client = client_for(server, transport)
    assert client.echo('before') == 'before'

    port = server.port
    server.ro_stop(wait=True, timeout=15)

    with pytest.raises(ro.remoteObjectError):
        client.echo('while it is down')

    again = ro.remoteObjectServer(
        svcname=None, name='reuse', obj=Service(), host=HOST,
        logger=ro.nullLogger(), usethread=True, ns=False, default_auth=False,
        transport=transport, port=port, numthreads=16,
        method_list=['echo', 'slow_echo'])
    again.ro_start(wait=True, timeout=15)
    try:
        import time
        deadline = time.time() + 20.0
        while time.time() < deadline:
            try:
                assert client.echo('after') == 'after'
                return
            except ro.remoteObjectError:
                time.sleep(0.2)
        pytest.fail('never re-dialled after the service came back')
    finally:
        again.ro_stop(wait=True, timeout=15)


def test_closing_releases_every_thread_connection(service):
    """One per calling thread means close() has to find them all, including
    those belonging to threads that have already gone."""
    transport = 'g2rpc-tcp-persistent'
    server = service(transport)
    client = client_for(server, transport)

    def call():
        client.echo('hi')

    threads = [threading.Thread(target=call) for _ in range(3)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=30)

    proxy = client.proxy
    assert len(proxy._all) == 3, 'expected one transport per calling thread'

    proxy.close()
    assert proxy._all == []
