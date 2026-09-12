#
# test_asyncio_carrier.py -- serving from an event loop, handling on a pool
#
"""The threaded carriers give every accepted connection a thread.  That is
honest about a client that stalls, but Gen2 traffic is bursty and its
processes already carry several large pools, so a thousand connections
arriving at once is a thousand threads that did no work.

``g2rpc-tcp-asyncio`` runs the I/O on one event loop and hands each request
to the thread pool the service already had.  The pool is not an optimisation
here: a pubsub runs local subscriber callbacks inline inside
``remote_update`` (:py:meth:`PubSub.PubSub.subscribe_cb`), which is arbitrary
synchronous application code, so handling on the loop would stall every
connection at once.  That is what
:py:func:`test_a_blocking_handler_does_not_stall_other_connections` pins.
"""

import socket
import threading
import time

import pytest

from g2base.remoteObjects import Monitor
from g2base.remoteObjects import remoteObjectNameSvc as ns_mod
from g2base.remoteObjects import remoteObjects as ro
from g2base.remoteObjects import ro_asyncio, ro_transport

HOST = '127.0.0.1'
CARRIER = 'g2rpc-tcp-asyncio'


class FakePubSub:
    def subscribe(self, channel):
        pass

    def add_callback(self, channel, fn):
        pass

    def publish(self, channel, envelope, pack_info):
        pass


class Service:
    def __init__(self):
        self.entered = threading.Event()

    def echo(self, value):
        return value

    def block(self, seconds):
        """Exactly what a subscribe_cb callback may do: sleep in sync-land."""
        self.entered.set()
        time.sleep(seconds)
        return 'done'


@pytest.fixture
def nameservice():
    service = ns_mod.remoteObjectNameService('names', FakePubSub(),
                                             ro.nullLogger(), HOST)
    previous, ro.default_ns = ro.default_ns, service
    yield service
    ro.default_ns = previous


@pytest.fixture
def service(nameservice):
    started = []

    def _make(obj=None, transports=('xmlrpc', CARRIER)):
        obj = obj if obj is not None else Service()
        server = ro.remoteObjectServer(
            svcname='async-svc', obj=obj, transport=list(transports),
            host=HOST, logger=ro.nullLogger(), usethread=True,
            ns=nameservice, default_auth=False,
            method_list=['echo', 'block'])
        server.ro_start(wait=True, timeout=15)
        started.append(server)
        return server

    yield _make
    for server in started:
        try:
            server.ro_stop(wait=True, timeout=15)
        except Exception:
            pass


def port_for(nameservice, name, protocol):
    for offered, port, _encoding in ro.endpoints_in(
            nameservice.getInfo(name)[0]):
        if offered == protocol:
            return port
    raise AssertionError('%s does not offer %s' % (name, protocol))


# ------------------------------------------------------ it is a carrier --

def test_the_carrier_is_registered_and_available():
    spec = ro_transport.get(CARRIER)
    assert spec.available()
    assert spec.encoding == 'msgpack'


def test_it_is_opt_in():
    """Nothing reaches for it on its own: a service has to name it.

    It is the same as g2rpc-tcp to a caller -- the client transport is
    identical -- and differs only in what the service pays for a burst of
    connections.  It is also slower per call, since every request crosses
    from the event loop to the pool and back, so it is worth naming where
    the connections are many and not worth it otherwise.
    """
    from g2base.remoteObjects import PubSub

    assert CARRIER not in ro.default_protocol_preference
    assert CARRIER not in PubSub.default_pubsub_transport


def test_a_call_is_answered(service, nameservice):
    service()
    proxy = ro.remoteObjectProxy('async-svc', transport=CARRIER,
                                 default_auth=False)

    assert proxy.echo('hi') == 'hi'


def test_the_port_is_bound_before_it_is_registered(service, nameservice):
    """remoteObjectServer registers after binding and before serving, so a
    transport that binds lazily would publish a port nothing listens on.

    The stock asyncio transport binds inside ``start()``, which is a
    coroutine and so runs too late; this carrier hands it a socket bound
    already.
    """
    service()
    port = port_for(nameservice, 'async-svc', CARRIER)

    with socket.create_connection((HOST, port), timeout=5) as sock:
        assert sock is not None, 'registered port is listening'


def test_the_client_side_is_the_ordinary_tcp_one(service, nameservice):
    """Only the server shape differs, which is what lets a caller reach it
    without knowing anything about event loops."""
    spec = ro_transport.get(CARRIER)
    plain = ro_transport.get('g2rpc-tcp')

    assert type(spec.make_client_transport(HOST, 9)) is type(
        plain.make_client_transport(HOST, 9))


# ---------------------------------------- why the pool is not optional --

def test_a_blocking_handler_does_not_stall_other_connections(service,
                                                             nameservice):
    """The property the whole design rests on.

    Handlers run on the service's thread pool, not on the event loop, so a
    method that sleeps holds up only its own caller.  Were it dispatched on
    the loop, the second call could not be answered until the first
    returned, and a single slow subscriber callback would stop the service.
    """
    obj = Service()
    service(obj=obj)

    slow = ro.remoteObjectProxy('async-svc', transport=CARRIER,
                                default_auth=False)
    done = threading.Event()

    def call_slow():
        try:
            slow.block(3.0)
        finally:
            done.set()

    caller = threading.Thread(target=call_slow, daemon=True)
    caller.start()
    assert obj.entered.wait(10), 'the slow call reached the handler'

    fast = ro.remoteObjectProxy('async-svc', transport=CARRIER,
                                default_auth=False)
    started = time.perf_counter()
    assert fast.echo('quick') == 'quick'
    elapsed = time.perf_counter() - started

    assert elapsed < 1.5, (
        'answered in %.2fs while a handler was sleeping 3s; that is the '
        'loop being blocked' % (elapsed,))
    done.wait(10)
    caller.join(timeout=10)


# ------------------------------------------------- what it is here for --

def test_a_burst_of_connections_costs_no_threads(service, nameservice):
    """The reason to prefer it.  A threaded carrier spends a thread on each
    of these; this spends a coroutine, and the service keeps answering."""
    service()
    port = port_for(nameservice, 'async-svc', CARRIER)
    before = threading.active_count()

    held = []
    try:
        for _ in range(200):
            sock = socket.socket()
            sock.connect((HOST, port))
            held.append(sock)
        time.sleep(0.5)
        grew = threading.active_count() - before

        assert grew <= 2, (
            '%d connections added %d threads' % (len(held), grew))

        proxy = ro.remoteObjectProxy('async-svc', transport=CARRIER,
                                     default_auth=False)
        assert proxy.echo('still here') == 'still here'
    finally:
        for sock in held:
            sock.close()


def test_a_pubsub_delivers_over_it_and_runs_local_callbacks(nameservice):
    """The case it was built for: the subscriber's own callbacks are the
    synchronous code that must not run on the loop."""
    publisher = Monitor.Monitor('ap-pub', ro.nullLogger(), numthreads=20)
    subscriber = Monitor.Monitor('ap-sub', ro.nullLogger(), numthreads=20)
    seen = []
    arrived = threading.Event()
    try:
        publisher.start()
        publisher.start_server(svcname='ap-pub', host=HOST, ns=nameservice,
                               default_auth=False, transport=['xmlrpc'],
                               usethread=True, wait=True)
        subscriber.start()
        subscriber.start_server(svcname='ap-sub', host=HOST, ns=nameservice,
                                default_auth=False,
                                transport=['xmlrpc', CARRIER],
                                usethread=True, wait=True)

        def on_update(value, names, channels):
            seen.append(value)
            arrived.set()

        subscriber.subscribe_cb(on_update, ['ap-pub'])
        publisher.subscribe('ap-sub', ['ap-pub'],
                            {'unsub': False, 'transport': [CARRIER]})
        time.sleep(0.5)
        publisher.update('TSCS.X', {'n': 1}, ['ap-pub'])

        assert arrived.wait(15), 'the update reached the local callback'
        chosen = publisher._partner['ap-sub'].proxy.endpoints.clients()[0]
        assert chosen.transport == CARRIER
    finally:
        for monitor in (subscriber, publisher):
            try:
                monitor.stop_server()
            except Exception:
                pass
            monitor.stop()


# ------------------------------------------------------------- shutdown --

def test_stopping_it_takes_the_loop_thread_with_it(service, nameservice):
    """A service that starts and stops repeatedly must not leak a loop
    thread each time, and must not take seconds about it.

    The runner has a last-resort path that stops the loop outright when the
    serve loop will not come back to check its quit event.  That path always
    reclaims the thread, so counting threads alone cannot tell a clean
    shutdown from one that waited out a timeout -- hence the clock.
    """
    before = threading.active_count()
    slowest = 0.0
    for _ in range(3):
        server = ro.remoteObjectServer(
            svcname='cycle', obj=Service(), transport=[CARRIER],
            host=HOST, logger=ro.nullLogger(), usethread=True,
            ns=nameservice, default_auth=False, method_list=['echo'])
        server.ro_start(wait=True, timeout=15)
        proxy = ro.remoteObjectProxy('cycle', transport=CARRIER,
                                     default_auth=False)
        assert proxy.echo('x') == 'x'
        started = time.perf_counter()
        server.ro_stop(wait=True, timeout=15)
        slowest = max(slowest, time.perf_counter() - started)
    time.sleep(1.0)

    assert threading.active_count() - before <= 2, 'loop threads left behind'
    assert slowest < 3.0, (
        'slowest stop took %.1fs; the serve loop is not noticing that it '
        'was asked to finish' % (slowest,))


# ------------------------------------------------------------ the seam --

def test_a_spec_chooses_the_server_that_drives_it():
    """The shape of the server belongs to the carrier, so the default is
    unchanged for everything that existed before."""
    from tinyrpc.server.executor import RPCServerExecutor

    assert ro_transport.get('g2rpc-tcp').make_rpc_server.__func__ is \
        ro_transport.TransportSpec.make_rpc_server
    assert RPCServerExecutor is not None

    spec = ro_transport.get(CARRIER)
    assert spec.make_rpc_server.__func__ is not \
        ro_transport.TransportSpec.make_rpc_server


def test_the_runner_presents_start_and_stop():
    """What __cmd_loop calls on every listener it owns."""
    assert callable(ro_asyncio.AsyncioServerRunner.start)
    assert callable(ro_asyncio.AsyncioServerRunner.stop)
