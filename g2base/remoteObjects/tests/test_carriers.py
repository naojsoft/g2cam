#
# test_carriers.py -- carrying a protocol over HTTP, TCP and 0mq
#
"""The transport is a separate choice from the protocol.

g2rpc is registered over three carriers, so the same envelope and the same
encodings run over HTTP, a bare TCP socket, or 0mq.  What differs between
them is what the carrier can offer: HTTP has a header for credentials and can
be encrypted, and the other two have neither.

Every carrier here dials once per call.  That is what makes a client and a
service restartable in any order -- there is no held connection to go stale
-- and it is why none of them needs reconnection logic.
"""

import time

import pytest

from g2base.remoteObjects import remoteObjects as ro
from g2base.remoteObjects import ro_g2rpc, ro_transport

HOST = '127.0.0.1'

#: The carriers g2rpc is registered over.
CARRIERS = ['g2rpc', 'g2rpc-tcp', 'g2rpc-zmq']

#: Carriers with no way to convey a caller's credentials.
NO_CREDENTIALS = ['g2rpc-tcp', 'g2rpc-zmq']


class ServiceObject:
    def echo(self, value):
        return value

    def add(self, a, b, c=0):
        return a + b + c

    def boom(self):
        raise ValueError('kaboom')


@pytest.fixture
def service():
    started = []

    def _make(transport, encoding=None, **kwargs):
        kwargs.setdefault('svcname', None)
        kwargs.setdefault('name', 'carried')
        kwargs.setdefault('obj', ServiceObject())
        kwargs.setdefault('host', HOST)
        kwargs.setdefault('logger', ro.nullLogger())
        kwargs.setdefault('usethread', True)
        kwargs.setdefault('ns', False)
        kwargs.setdefault('default_auth', False)
        kwargs.setdefault('method_list', ['echo', 'add', 'boom'])
        svc = ro.remoteObjectServer(transport=transport, encoding=encoding,
                                    **kwargs)
        svc.ro_start(wait=True, timeout=10.0)
        started.append(svc)
        return svc

    yield _make

    for svc in started:
        try:
            svc.ro_stop(wait=True, timeout=10.0)
        except Exception:
            pass


def client_for(svc, transport, encoding=None):
    return ro.remoteObjectClient(HOST, svc.port, name='carried',
                                 default_auth=False, transport=transport,
                                 encoding=encoding, timeout=10.0)


# ------------------------------------------------------------- carrying --

@pytest.mark.parametrize('transport', CARRIERS)
@pytest.mark.parametrize('encoding', ro_g2rpc.ENCODINGS)
def test_a_call_crosses_every_carrier(service, transport, encoding):
    svc = service(transport, encoding)
    client = client_for(svc, transport, encoding)

    assert client.echo('hi') == 'hi'
    assert client.echo(None) is None
    assert client.echo(2 ** 70) == 2 ** 70
    assert client.add(1, 2, c=3) == 6, "keyword arguments survive"


@pytest.mark.parametrize('transport', CARRIERS)
def test_successive_calls_each_stand_alone(service, transport):
    """Each call dials afresh, so nothing carries over between them."""
    svc = service(transport)
    client = client_for(svc, transport)
    assert [client.echo(i) for i in range(10)] == list(range(10))


@pytest.mark.parametrize('transport', CARRIERS)
def test_a_large_payload_arrives_whole(service, transport):
    """Gen2 ships image and header buffers through ordinary calls.

    The TCP carrier used to default to a packer that read one 4096-byte
    chunk with no length prefix, so anything larger arrived truncated.
    """
    svc = service(transport)
    payload = 'x' * 400000
    assert client_for(svc, transport).echo(payload) == payload


@pytest.mark.parametrize('transport', CARRIERS)
def test_a_failing_method_reaches_the_caller(service, transport):
    svc = service(transport)
    with pytest.raises(ro.remoteObjectError) as excinfo:
        client_for(svc, transport).boom()
    assert 'kaboom' in str(excinfo.value)


@pytest.mark.parametrize('transport', CARRIERS)
def test_the_server_stops_promptly(service, transport):
    svc = service(transport)
    client_for(svc, transport).echo('hi')

    started = time.time()
    svc.ro_stop(wait=True, timeout=10.0)
    assert time.time() - started < 5.0


@pytest.mark.parametrize('transport', CARRIERS)
def test_a_transport_gives_up_waiting_for_a_request(transport):
    """Regression, and the load-bearing half of stopping a server.

    Every one of these transports used to block indefinitely waiting for the
    next request -- accept(), a queue get, or recv_multipart() -- so setting
    ev_quit did nothing until a request happened to arrive.  The serve loop
    then never returned, and because a ThreadPoolExecutor's threads are not
    daemons, the *process* would not exit either.

    That failure mode is a hang rather than a wrong answer, which no
    assertion about ro_stop() can catch, so the timeout is tested here
    directly.
    """
    import threading

    from tinyrpc.transports import TransportTimeout

    spec = ro_transport.get(transport)
    server = spec.make_server_transport(HOST, 0, poll_timeout=0.2)

    # Waited for on a thread of its own: if the transport goes back to
    # blocking, this test should fail rather than hang with it.
    outcome = []

    def wait_once():
        try:
            outcome.append(server.receive_message())
        except BaseException as e:
            outcome.append(e)

    waiter = threading.Thread(target=wait_once)
    waiter.daemon = True
    try:
        waiter.start()
        waiter.join(timeout=5.0)

        assert outcome, ("receive_message() never gave up waiting, so a "
                         "server using this transport could not be stopped")
        assert isinstance(outcome[0], TransportTimeout), outcome[0]
    finally:
        server.stop()


@pytest.mark.parametrize('transport', CARRIERS)
def test_a_client_need_not_share_the_service_encoding(service, transport):
    """The packed envelope names its own packer, so both ends read whatever
    they are sent."""
    svc = service(transport, 'msgpack')
    assert client_for(svc, transport, 'json').echo('hi') == 'hi'


# ------------------------------------------------- what a carrier offers --

def test_the_carriers_are_registered():
    for name in CARRIERS:
        assert ro_transport.get(name).encoding_is_selectable


@pytest.mark.parametrize('transport', NO_CREDENTIALS)
def test_a_service_needing_credentials_refuses_a_carrier_without_them(
        transport):
    """A bare socket has nowhere to put credentials, so the authenticator
    would find none and refuse every call.  Failing at construction says what
    is actually wrong rather than looking like a network fault."""
    with pytest.raises(ro.remoteObjectError) as excinfo:
        ro.remoteObjectServer(svcname='authed', name='authed',
                              obj=ServiceObject(), host=HOST,
                              logger=ro.nullLogger(), transport=transport,
                              method_list=['echo'])
    assert 'cannot carry credentials' in str(excinfo.value)


def test_the_http_carrier_does_take_credentials(service):
    svc = service('g2rpc', svcname='authed', default_auth=True)
    client = ro.remoteObjectClient(HOST, svc.port, name='authed',
                                   auth=('authed', 'authed'),
                                   transport='g2rpc', timeout=10.0)
    assert client.echo('hi') == 'hi'

    wrong = ro.remoteObjectClient(HOST, svc.port, name='authed',
                                  auth=('authed', 'nope'),
                                  transport='g2rpc', timeout=10.0)
    with pytest.raises(ro.remoteObjectError):
        wrong.echo('hi')


@pytest.mark.parametrize('transport', NO_CREDENTIALS)
def test_a_carrier_that_cannot_be_encrypted_says_so(transport):
    spec = ro_transport.get(transport)
    assert not spec.supports_tls
    with pytest.raises(ValueError) as excinfo:
        spec.make_client_transport(HOST, 8000, secure=True)
    assert 'encrypted' in str(excinfo.value)


def test_the_old_transport_names_reach_the_new_carriers():
    """A service registered before the upgrade said 'socket' or 'zmqrpc';
    those were this protocol over these carriers all along."""
    assert ro_transport.get('socket') is ro_transport.get('g2rpc-tcp')
    assert ro_transport.get('zmqrpc') is ro_transport.get('g2rpc-zmq')


def test_each_carrier_reports_the_port_it_bound(service):
    """0mq describes its endpoint as a URL rather than a pair, so the port
    has to be read back through the spec."""
    for transport in CARRIERS:
        svc = service(transport)
        assert isinstance(svc.port, int) and svc.port > 0
        assert svc.nsopts['protocol'] == transport
