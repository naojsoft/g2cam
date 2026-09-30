#
# test_persistent_reconnect.py -- a held connection that dies underneath a caller
#
"""What a caller on g2rpc-tcp-persistent sees when the connection drops.

The connectionless carrier has nothing to recover: it dials per call, so a
drop between calls is not an event.  A held connection is the opposite -- it
is exactly what can die while nobody is looking -- and "it reconnects" is not
by itself an answer, because what matters to a caller is which of its calls
pay for the drop.

Three cases, and they behave differently:

* **Nothing in flight.**  The drop is invisible.  The next call finds no
  socket, dials, and succeeds; the caller never learns it happened.
* **A call in flight.**  Its reply is gone and cannot be recovered, so that
  call fails -- promptly, which is the part worth testing: a caller that gave
  no timeout would otherwise wait for a reply that provably cannot come.
* **A drop within the redial pacing.**  ``reconnect_interval`` refuses to
  dial again immediately, so a service flapping faster than that is asked
  politely rather than continuously, and calls fail meanwhile.

Through the name service there is one more twist: :py:func:`call_failover`
retries a connection-level failure, so even the in-flight call usually
succeeds -- at the price of running the method twice at the service.  That is
the right default for a fault-tolerant read and the wrong one for a method
that counts something, and it is asserted here so that it stays a choice
rather than a surprise.

See also test_transport_reuse.py, which covers the service being restarted
rather than the connection being dropped.
"""

import socket
import threading
import time

import pytest

from g2base.remoteObjects import remoteObjectNameSvc as ns_mod
from g2base.remoteObjects import remoteObjects as ro

HOST = '127.0.0.1'
TRANSPORT = 'g2rpc/tcp-persistent'

#: Older than the transport's reconnect_interval (0.5s), so the redial
#: pacing is not what is being measured.  Every connection in a service that
#: has been up for a second is at least this old.
AGED = 0.6


class Service:
    def __init__(self):
        self.calls = []
        self.lock = threading.Lock()

    def echo(self, value):
        return value

    def slow(self, secs):
        """Long enough to still be running when the connection goes."""
        with self.lock:
            self.calls.append(time.monotonic())
        time.sleep(secs)
        return 'slept'

    def call_count(self):
        with self.lock:
            return len(self.calls)


@pytest.fixture
def nameservice():
    return ns_mod.remoteObjectNameService('names', ro.nullLogger(), HOST)


@pytest.fixture
def service(nameservice):
    """A running service, and the object it serves."""
    obj = Service()
    server = ro.remoteObjectServer(
        svcname='recon', obj=obj, host=HOST, logger=ro.nullLogger(),
        usethread=True, ns=nameservice, default_auth=False,
        transport=TRANSPORT, numthreads=16,
        method_list=['echo', 'slow', 'call_count'])
    server.ro_start(wait=True, timeout=15)
    try:
        yield server, obj
    finally:
        try:
            server.ro_stop(wait=True, timeout=15)
        except Exception:
            pass


def direct(server, timeout=30):
    return ro.remoteObjectClient(HOST, server.port, name='recon',
                                 default_auth=False, transport=TRANSPORT,
                                 timeout=timeout)


def through_name_service(nameservice, timeout=30):
    return ro.remoteObjectProxy('recon', ns=nameservice, default_auth=False,
                                transport=TRANSPORT, timeout=timeout,
                                logger=ro.nullLogger())


def held(client):
    """The transports a client is holding.

    ``vars()`` rather than ``getattr()``: on either client shape an unknown
    attribute is a remote method call, so ``getattr`` hands back a closure
    instead of raising.
    """
    own = vars(client)
    if 'proxy' in own:                          # remoteObjectClient
        return list(vars(own['proxy'])['_all'])
    transports = []                             # remoteObjectProxy
    for one in own['endpoints'].clients():
        transports.extend(vars(vars(one)['proxy'])['_all'])
    return transports


def drop(client):
    """End the connection the way the far end going away ends it.

    shutdown() delivers a FIN, so the reader's recv returns nothing and the
    reader is the one that notices -- which is the usual order of events,
    since a connection that dies while nobody is calling dies at the far end
    and the news arrives on the connection itself.  The service stays up, so
    what is tested is the connection dying rather than the provider going.
    """
    dropped = 0
    for transport in held(client):
        sock = getattr(transport, '_sock', None)
        if sock is None:
            continue
        try:
            sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        dropped += 1
    return dropped


def dropped(client, timeout=5.0):
    """Wait for the reader to clear the dead connection.  True if it did."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if not any(getattr(t, '_sock', None) is not None
                   for t in held(client)):
            return True
        time.sleep(0.01)
    return False


def readers():
    return {t for t in threading.enumerate() if t.name == 'tcp-client-reader'}


# ------------------------------------------------ nothing in flight --

def test_a_drop_the_reader_saw_is_invisible(service):
    """The reader clears the dead connection, so the next call finds none,
    dials inside the call, and succeeds.  The caller is never told about a
    connection it did not know it had."""
    server, _ = service
    client = direct(server)
    assert client.echo('before') == 'before'
    time.sleep(AGED)

    assert drop(client) == 1
    assert dropped(client), 'the reader never noticed'

    assert client.echo('after') == 'after'


def test_a_drop_costs_at_most_one_call(service):
    """Whether a drop is invisible is a race, and this is the part that is
    not: losing one call, never two.

    A call that arrives before the reader has processed the FIN goes out on
    the dying socket and fails there -- and the transport does not resend it,
    because a request that may already have reached the service is not safe
    to repeat at this layer.  What has to hold is that the failure is
    bounded: the drop is dealt with by the time the next call is made.

    Dropping and calling immediately, with no wait in between, is what makes
    the race happen at all -- a caller that pauses for as long as a reader
    takes to be scheduled would mostly see the invisible case above.
    """
    server, _ = service
    client = direct(server)
    client.echo('warm')

    lost = 0
    for n in range(25):
        time.sleep(AGED)
        drop(client)
        try:
            client.echo('x')
        except ro.remoteObjectError:
            lost += 1
            # The whole claim: the connection is replaced by now.
            assert client.echo('recovered') == 'recovered', (
                'a second call in a row failed, on drop %d' % n)
    print('\n    %d of 25 drops cost the caller a call' % lost)


def test_repeated_drops_do_not_accumulate_reader_threads(service):
    """A client outlives many disconnections, so anything kept per dead
    connection is kept for the life of the process."""
    server, _ = service
    client = direct(server)
    client.echo('warm')
    before = readers()

    for _ in range(8):
        time.sleep(AGED)
        drop(client)
        try:
            client.echo('x')
        except ro.remoteObjectError:
            client.echo('x')    # lost the racing call; see the test below

    time.sleep(1.0)
    mine = readers() - before
    assert len(mine) <= 1, 'a reader outlived its connection'
    assert client.echo('end') == 'end'


# --------------------------------------------------- a call in flight --

def test_a_lost_reply_does_not_strand_a_caller_who_gave_no_timeout(service):
    """The reply to a call that was in flight is gone, and no timeout means
    nothing else would ever end the wait.  The transport notices that the
    connection it is waiting on has died, and says so."""
    server, _ = service
    client = direct(server, timeout=None)
    client.echo('warm')
    time.sleep(AGED)

    outcome = {}

    def call():
        try:
            outcome['returned'] = client.slow(5)
        except Exception as e:
            outcome['raised'] = e

    caller = threading.Thread(target=call, daemon=True)
    caller.start()
    time.sleep(0.3)             # let the request reach the service
    drop(client)

    caller.join(timeout=20)
    assert not caller.is_alive(), 'the caller is still waiting'
    assert isinstance(outcome.get('raised'), ro.remoteObjectError), outcome
    assert 'went away' in str(outcome['raised'])


def test_the_connection_is_usable_again_after_a_lost_reply(service):
    """The call is lost; the client is not."""
    server, _ = service
    client = direct(server, timeout=None)
    client.echo('warm')
    time.sleep(AGED)

    caller = threading.Thread(target=lambda: _swallow(client.slow, 5),
                              daemon=True)
    caller.start()
    time.sleep(0.3)
    drop(client)
    caller.join(timeout=20)

    assert client.echo('after') == 'after'


def _swallow(fn, *args):
    try:
        fn(*args)
    except Exception:
        pass


# ------------------------------------------- what failover adds to it --

def test_the_name_service_proxy_retries_a_lost_call(service, nameservice):
    """A connection-level failure is what failover is for, so the proxy
    re-resolves and calls again -- and the caller sees a success where the
    direct client saw an error."""
    server, _ = service
    proxy = through_name_service(nameservice, timeout=None)
    proxy.echo('warm')
    time.sleep(AGED)

    outcome = {}

    def call():
        try:
            outcome['returned'] = proxy.slow(3)
        except Exception as e:
            outcome['raised'] = e

    caller = threading.Thread(target=call, daemon=True)
    caller.start()
    time.sleep(0.3)
    drop(proxy)
    caller.join(timeout=30)

    assert not caller.is_alive()
    assert outcome.get('returned') == 'slept', outcome


def test_that_retry_runs_the_method_twice(service, nameservice):
    """Which is the price of it, and the reason a method that counts
    something wants a proxy that does not retry.

    The first invocation is not cancelled by the drop -- it is already
    running at the service, and runs to completion with nowhere to send its
    reply -- so the retry is a second execution, not a replacement for a
    first that never happened.
    """
    server, obj = service
    proxy = through_name_service(nameservice, timeout=None)
    proxy.echo('warm')
    time.sleep(AGED)
    assert obj.call_count() == 0

    caller = threading.Thread(target=lambda: _swallow(proxy.slow, 2),
                              daemon=True)
    caller.start()
    time.sleep(0.3)
    drop(proxy)
    caller.join(timeout=30)

    # Both invocations have to finish before the count settles.
    deadline = time.time() + 15
    while time.time() < deadline and obj.call_count() < 2:
        time.sleep(0.1)
    assert obj.call_count() == 2, (
        'expected the lost call and its retry, got %d' % obj.call_count())


# ------------------------------------------------- the redial pacing --

def test_a_drop_inside_the_pacing_window_costs_calls(service):
    """reconnect_interval is what keeps a client from dialling a downed
    service continuously, and a connection that dies within it cannot be
    replaced until it has passed.  Only reachable when the drop follows the
    dial closely -- a service flapping, or a connection that has only just
    come up."""
    server, _ = service
    client = direct(server)
    client.echo('warm')         # dials; _last_attempt is now

    drop(client)                # no AGED sleep: inside the window

    # The first of these fails on the dead socket; what the pacing adds is
    # that the ones after it are refused before any socket is touched.
    refusals = []
    for _ in range(4):
        try:
            client.echo('x')
        except ro.remoteObjectError as e:
            refusals.append(str(e))
    assert len(refusals) == 4, 'expected every call inside the window to fail'
    assert any('not reconnecting' in r for r in refusals), refusals

    # And it recovers once the interval has passed.
    time.sleep(AGED)
    assert client.echo('after') == 'after'
