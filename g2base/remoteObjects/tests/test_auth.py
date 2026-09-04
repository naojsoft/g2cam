#
# test_auth.py -- proving who is calling, over every carrier
#
"""Gen2 has always had an ``authDict``: a name mapped to a shared secret,
known to everyone allowed to call a service.  Two things were wrong with how
it was used, and both are fixed here without changing the table itself.

The password crossed the wire on every authenticated call, so anything able
to watch the traffic could take it and use it forever.  And a bare socket had
nowhere to put it, so services on the TCP and 0mq carriers could not
authenticate at all.

Signing fixes both: the secret proves possession without travelling, and it
lives in the protocol's envelope rather than an HTTP header, so it works
wherever the protocol does.
"""

import pytest

from g2base.remoteObjects import remoteObjects as ro
from g2base.remoteObjects import ro_g2rpc, ro_transport

HOST = '127.0.0.1'
SIGNING = ['g2rpc', 'g2rpc-tcp', 'g2rpc-tcp-persistent', 'g2rpc-zmq']
BASIC = ['xmlrpc', 'jsonrpc']


class Service:
    def echo(self, value):
        return value


@pytest.fixture
def service():
    started = []

    def _make(transport, authDict=None, svcname='authsvc'):
        server = ro.remoteObjectServer(
            svcname=svcname, name=svcname, obj=Service(), host=HOST,
            logger=ro.nullLogger(), usethread=True, ns=False,
            transport=transport, default_auth=authDict is None,
            authDict=authDict, method_list=['echo'])
        server.ro_start(wait=True, timeout=10)
        started.append(server)
        return server

    yield _make
    for server in started:
        try:
            server.ro_stop(wait=True, timeout=10)
        except Exception:
            pass


def client(transport, port, auth, svcname='authsvc'):
    return ro.remoteObjectClient(HOST, port, name=svcname,
                                 transport=transport, auth=auth,
                                 default_auth=False, timeout=10)


# ------------------------------------------------ every carrier can now --

@pytest.mark.parametrize('transport', SIGNING + BASIC)
def test_a_caller_with_the_credential_is_admitted(transport, service):
    server = service(transport)
    assert client(transport, server.port,
                  ('authsvc', 'authsvc')).echo('hi') == 'hi'


@pytest.mark.parametrize('transport', SIGNING + BASIC)
def test_a_caller_with_the_wrong_password_is_refused(transport, service):
    server = service(transport)
    with pytest.raises(ro.remoteObjectError):
        client(transport, server.port, ('authsvc', 'wrong')).echo('hi')


@pytest.mark.parametrize('transport', SIGNING + BASIC)
def test_a_caller_with_no_credential_at_all_is_refused(transport, service):
    server = service(transport)
    with pytest.raises(ro.remoteObjectError):
        client(transport, server.port, None).echo('hi')


@pytest.mark.parametrize('transport', SIGNING)
def test_the_bare_socket_carriers_can_authenticate_now(transport):
    """They could not before: there is nowhere in a TCP or 0mq frame to put
    an HTTP header, so a service needing authentication had to use HTTP.
    Signing lives in the protocol's envelope, so it travels anywhere."""
    spec = ro_transport.get(transport)
    assert spec.auth_mechanism == 'signature'
    assert spec.carries_credentials


def test_a_carrier_that_still_cannot():
    """Nothing has quietly gained the ability; only the specs whose protocol
    has an envelope of its own."""
    class Bare(ro_transport.TcpTransportSpec):
        pass

    spec = Bare('bare', object, content_type='x', encoding='json')
    assert spec.auth_mechanism is None
    assert not spec.carries_credentials


# ------------------------------------------------- what actually changed --

def make_pair(authDict, service='authsvc', caller=None):
    """A server protocol and a client protocol, as the wiring builds them."""
    server = ro_g2rpc.G2RPCProtocol(
        framing=ro_g2rpc.signing_framing(authDict, service=service))
    name, password = caller or next(iter(authDict.items()))
    client_side = ro_g2rpc.G2RPCProtocol(
        framing=ro_g2rpc.signing_framing({name: password}, service=service,
                                         sign_as=name, require=False))
    return server, client_side


def test_the_password_does_not_travel():
    """The change worth having: today an authenticated call puts the
    password on the wire, where anyone between the two ends can take it."""
    server, caller = make_pair({'status': 'sekrit-password'}, 'status')
    wire = caller.create_request('echo', ['hi']).serialize()

    assert b'sekrit-password' not in wire

    # The *name* does travel, and is meant to: the receiver has to know
    # which key to check against, and a service name is not a secret --
    # the name service publishes them.  It is a claim that selects a key,
    # and a caller that does not hold the key cannot make use of it.
    assert b'status' in wire
    assert server.parse_request(wire).principal == 'status'


def test_a_message_altered_on_the_way_is_refused():
    """HTTP Basic says nothing about the body, so a caller's credentials
    could be attached to a request it never made."""
    server, caller = make_pair({'status': 'pw'}, 'status')
    wire = bytearray(caller.create_request('echo', ['hi']).serialize())
    wire[-1] ^= 0xFF

    with pytest.raises(Exception) as excinfo:
        server.parse_request(bytes(wire))
    assert 'signature' in str(excinfo.value)


def test_a_call_captured_on_the_way_to_one_service_will_not_work_at_another():
    """Even between peers that legitimately talk to both."""
    authDict = {'gateway': 'pw'}
    _status, caller = make_pair(authDict, 'status', caller=('gateway', 'pw'))
    taskmgr = ro_g2rpc.G2RPCProtocol(
        framing=ro_g2rpc.signing_framing(authDict, service='taskmgr'))

    with pytest.raises(Exception) as excinfo:
        taskmgr.parse_request(caller.create_request('echo').serialize())
    assert 'not signed for this service' in str(excinfo.value)


def test_one_password_reused_for_two_services_does_not_become_one_key():
    """Salting by service means a secret shared with one service does not
    silently authorise its holder to the other."""
    assert ro_g2rpc.key_for('same', 'status') != ro_g2rpc.key_for('same',
                                                                  'taskmgr')


def test_deriving_a_key_happens_once():
    """It is deliberately slow -- that is what makes a weak password
    expensive -- and a protocol is built for every call."""
    assert ro_g2rpc.key_for('pw', 'svc') is ro_g2rpc.key_for('pw', 'svc')


# -------------------------------------------------- the authenticator --

def test_the_signature_authenticator_admits_a_proven_name():
    authenticator = ro.make_authenticator({'status': 'pw'}, ro.nullLogger(),
                                          'signature')
    server, caller = make_pair({'status': 'pw'}, 'status')
    arrived = server.parse_request(caller.create_request('echo').serialize())

    authenticator(None, arrived)


def test_the_signature_authenticator_refuses_a_name_it_does_not_know():
    authenticator = ro.make_authenticator({'other': 'pw'}, ro.nullLogger(),
                                          'signature')
    server, caller = make_pair({'status': 'pw'}, 'status')
    arrived = server.parse_request(caller.create_request('echo').serialize())

    with pytest.raises(ro.remoteObjectError):
        authenticator(None, arrived)


def test_the_signature_authenticator_is_not_fooled_by_a_claimed_name():
    """A caller can put any name in credentials; only the principal was
    proved, and only the principal is looked at."""
    from tinyrpc.layers import Credentials
    authenticator = ro.make_authenticator({'status': 'pw'}, ro.nullLogger(),
                                          'signature')
    plain = ro_g2rpc.G2RPCProtocol()
    liar = ro_g2rpc.G2RPCProtocol(credentials=Credentials('status', 'pw'))
    arrived = plain.parse_request(liar.create_request('echo').serialize())

    with pytest.raises(ro.remoteObjectError):
        authenticator(None, arrived)


def test_a_service_with_several_credentials_and_none_of_its_own():
    """It has no obvious identity to sign replies as, and guessing would
    leave callers unable to check what came back."""
    with pytest.raises(ValueError) as excinfo:
        ro_g2rpc.signing_framing({'alice': 'a', 'bob': 'b'}, service='status')
    assert 'name it' in str(excinfo.value)


def test_a_service_signs_as_itself_when_it_holds_its_own_credential():
    framing = ro_g2rpc.signing_framing({'status': 'pw', 'alice': 'a'},
                                       service='status')
    assert framing.layers[0]._sign_as == b'status'


def test_a_service_named_but_not_registered_still_binds_to_the_right_name():
    """The audience has to be the name the caller used, since that is what
    the caller's signature is bound to.  A service without an svcname is
    known to its callers by its name, so that is what both ends must use."""
    import logging

    logging.disable(logging.CRITICAL)
    server = ro.remoteObjectServer(
        svcname=None, name='named', obj=Service(), host=HOST,
        logger=ro.nullLogger(), usethread=True, ns=False,
        transport='g2rpc-tcp', authDict={'named': 'pw'},
        method_list=['echo'])
    server.ro_start(wait=True, timeout=10)
    try:
        assert client('g2rpc-tcp', server.port, ('named', 'pw'),
                      svcname='named').echo('hi') == 'hi'
    finally:
        server.ro_stop(wait=True, timeout=10)
        logging.disable(logging.NOTSET)
