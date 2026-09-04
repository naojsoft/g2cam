#
# test_g2rpc.py -- Gen2's own protocol, and its interchangeable packer
#
"""The one protocol here whose encoding is a real choice.

XML-RPC, JSON-RPC and msgpack-RPC each fix their encoding, so for those the
name service's ``encoding`` field means nothing.  g2rpc restores what the old
socket and 0mq transports had -- one envelope, several codecs -- so this is
where that machinery is actually exercised.

It also has two things XML-RPC cannot offer: keyword arguments, and a
correlation id on the reply, without which several calls cannot be in flight
on one connection.
"""

import pytest

from g2base.remoteObjects import remoteObjects as ro
from g2base.remoteObjects import ro_g2rpc, ro_packer, ro_transport

HOST = '127.0.0.1'
ENCODINGS = list(ro_g2rpc.ENCODINGS)


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

    def _make(encoding=None, **kwargs):
        kwargs.setdefault('svcname', None)
        kwargs.setdefault('name', 'g2svc')
        kwargs.setdefault('obj', ServiceObject())
        kwargs.setdefault('host', HOST)
        kwargs.setdefault('logger', ro.nullLogger())
        kwargs.setdefault('usethread', True)
        kwargs.setdefault('ns', False)
        kwargs.setdefault('default_auth', False)
        kwargs.setdefault('transport', 'g2rpc')
        kwargs.setdefault('encoding', encoding)
        kwargs.setdefault('method_list', ['echo', 'add', 'boom'])
        svc = ro.remoteObjectServer(**kwargs)
        svc.ro_start(wait=True, timeout=10.0)
        started.append(svc)
        return svc

    yield _make

    for svc in started:
        svc.ro_stop(wait=True, timeout=10.0)


def client_for(svc, encoding=None):
    return ro.remoteObjectClient(HOST, svc.port, name='g2svc',
                                 default_auth=False, transport='g2rpc',
                                 encoding=encoding, timeout=10.0)


# ------------------------------------------------------------- protocol --

@pytest.mark.parametrize('encoding', ENCODINGS)
def test_a_call_round_trips(encoding):
    protocol = ro_g2rpc.G2RPCProtocol(encoding=encoding)
    request = protocol.create_request('add', (1, 2), {'c': 3})

    served = protocol.parse_request(request.serialize())
    assert served.method == 'add'
    assert served.args == [1, 2]
    assert served.kwargs == {'c': 3}, "keyword arguments survive"
    assert served.unique_id == request.unique_id

    reply = protocol.parse_reply(served.respond(6).serialize())
    assert reply.result == 6
    assert reply.unique_id == request.unique_id, "the reply says which call"


@pytest.mark.parametrize('encoding', ENCODINGS)
@pytest.mark.parametrize('value', [
    None, True, 0, -1, 2 ** 70, 3.5, '', 'すばる望遠鏡',
    [], [1, None, 'x'], {}, {'a': [1, {'b': None}]},
], ids=repr)
def test_payloads_survive_every_encoding(encoding, value):
    protocol = ro_g2rpc.G2RPCProtocol(encoding=encoding)
    served = protocol.parse_request(
        protocol.create_request('echo', (value,), None).serialize())
    got = protocol.parse_reply(served.respond(served.args[0]).serialize())
    assert got.result == value


def test_an_unknown_encoding_is_refused():
    with pytest.raises(ValueError) as excinfo:
        ro_g2rpc.G2RPCProtocol(encoding='pickle')
    assert 'msgpack, json, xml' in str(excinfo.value)


def _encoding_of(packet):
    """Which encoding a packet says it used.

    It is a byte of the framing header now, where it used to be a field in a
    JSON header wrapped around the body."""
    from tinyrpc.serializers import serializer_by_id
    return serializer_by_id(packet[4]).name


def test_a_reply_is_packed_the_way_the_request_was():
    """The envelope names its own encoding, so both ends can read anything.
    Answering in the caller's saves it decoding something it never asked
    for."""
    caller = ro_g2rpc.G2RPCProtocol(encoding='json')
    callee = ro_g2rpc.G2RPCProtocol(encoding='msgpack')

    request = caller.create_request('add', (1, 2), None).serialize()
    assert _encoding_of(request) == 'json'

    reply = callee.parse_request(request).respond(3).serialize()
    assert _encoding_of(reply) == 'json'
    assert caller.parse_reply(reply).result == 3


def test_errors_are_distinguishable_from_results():
    protocol = ro_g2rpc.G2RPCProtocol()
    served = protocol.parse_request(
        protocol.create_request('boom', (), None).serialize())

    reply = protocol.parse_reply(
        served.error_respond(ValueError('kaboom')).serialize())
    assert reply.code == ro_g2rpc.ERROR_APPLICATION
    assert 'kaboom' in reply.error
    assert 'ValueError' in reply.error, "the type is a clue worth keeping"


def test_a_one_way_request_expects_no_reply():
    protocol = ro_g2rpc.G2RPCProtocol()
    request = protocol.create_request('notify', (1,), None, one_way=True)
    assert request.unique_id is None
    assert protocol.parse_request(request.serialize()).respond(1) is None


def test_a_corrupt_message_is_refused():
    protocol = ro_g2rpc.G2RPCProtocol()
    with pytest.raises(Exception):
        protocol.parse_request(b'not a packed envelope at all')


def test_an_envelope_from_the_future_is_refused():
    """Refusing an unknown version beats guessing at its shape.

    There are two versions now and they move independently: the envelope's,
    and the body's inside it.  Both are header bytes, so both are checked
    before anything is decoded."""
    from tinyrpc import framing
    from tinyrpc.protocols.flexrpc import BODY_VERSION

    protocol = ro_g2rpc.G2RPCProtocol()
    good = protocol.create_request('x', (), None).serialize()

    newer_body = bytearray(good)
    newer_body[5] = BODY_VERSION + 1
    with pytest.raises(Exception) as excinfo:
        protocol.parse_request(bytes(newer_body))
    assert 'version' in str(excinfo.value)

    newer_envelope = bytearray(good)
    newer_envelope[2] = framing.VERSION + 1
    with pytest.raises(Exception) as excinfo:
        protocol.parse_request(bytes(newer_envelope))
    assert 'version' in str(excinfo.value)


def test_it_can_be_multiplexed():
    """The reply carries an id, so several calls can be outstanding at once
    -- which is the whole reason to want TCP or 0mq under it."""
    from tinyrpc.client_multiplexing import MultiplexingRPCClient
    assert ro_g2rpc.G2RPCProtocol.supports_reply_correlation is True

    class FakeTransport:
        def send_message_noblock(self, message):
            pass

        def receive_reply(self, timeout=None):
            raise TimeoutError()

    MultiplexingRPCClient(ro_g2rpc.G2RPCProtocol(), FakeTransport())


# -------------------------------------------------------------- registry --

def test_the_spec_offers_a_choice_of_encoding():
    """The first spec for which the name service's encoding field means
    something."""
    spec = ro_transport.get('g2rpc')
    assert spec.encoding_is_selectable
    assert set(spec.encodings) == set(ENCODINGS)
    assert spec.encoding == ro_g2rpc.DEFAULT_ENCODING


@pytest.mark.parametrize('encoding', ENCODINGS)
def test_the_registry_builds_the_encoding_asked_for(encoding):
    protocol = ro_transport.get('g2rpc', encoding=encoding) \
        .make_protocol(encoding)
    assert protocol.encoding == encoding


def test_the_registry_refuses_an_encoding_g2rpc_cannot_produce():
    with pytest.raises(ro_transport.UnknownTransport):
        ro_transport.get('g2rpc', encoding='pickle')


# ------------------------------------------------------------ end to end --

@pytest.mark.parametrize('encoding', ENCODINGS)
def test_a_service_and_client_talk(service, encoding):
    svc = service(encoding=encoding)
    client = client_for(svc, encoding)

    assert client.echo('hi') == 'hi'
    assert client.echo(2 ** 70) == 2 ** 70
    assert client.echo(None) is None
    assert client.add(1, 2) == 3


@pytest.mark.parametrize('encoding', ENCODINGS)
def test_keyword_arguments_reach_the_method(service, encoding):
    """What XML-RPC cannot do: the old stack raised TypeError client-side
    rather than carrying these."""
    svc = service(encoding=encoding)
    assert client_for(svc, encoding).add(1, 2, c=3) == 6


def test_a_failing_method_reaches_the_caller(service):
    svc = service()
    with pytest.raises(ro.remoteObjectError) as excinfo:
        client_for(svc).boom()
    assert 'kaboom' in str(excinfo.value)


def test_a_service_records_the_encoding_it_speaks(service):
    """The name service should describe what is actually on the wire, which
    for this protocol is a per-service choice."""
    svc = service(encoding='json')
    assert svc.nsopts['protocol'] == 'g2rpc'
    assert svc.nsopts['encoding'] == 'json'


def test_a_service_defaults_to_the_protocols_own_encoding(service):
    svc = service(encoding=None)
    assert svc.nsopts['encoding'] == ro_g2rpc.DEFAULT_ENCODING


def test_a_client_may_use_a_different_encoding_from_the_service(service):
    """Both ends read whatever they are sent, so the two need not agree."""
    svc = service(encoding='msgpack')
    assert client_for(svc, 'json').echo('hi') == 'hi'
    assert client_for(svc, 'xml').echo('hi') == 'hi'
