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


def test_a_signing_service_needs_a_registered_name():
    """The signature is bound to the service it was made for, so a call
    meant for one cannot be replayed at another.  That needs a name both
    ends agree on, and the only such name is the registered one -- a
    server's `name` is a thread label that never leaves the process, so a
    caller has no way to arrive at it.

    Saying so at construction beats binding to two different strings and
    refusing every call.
    """
    with pytest.raises(ro.remoteObjectError) as excinfo:
        ro.remoteObjectServer(svcname=None, name='named', obj=Service(),
                              host=HOST, logger=ro.nullLogger(),
                              transport='g2rpc-tcp',
                              authDict={'named': 'pw'},
                              method_list=['echo'])
    assert 'svcname' in str(excinfo.value)


def test_the_thread_label_has_no_say_in_it():
    """`name` and `svcname` are different things, and only one is an
    identity.  A service registered as 'real' is addressed as 'real' no
    matter what its threads are called."""
    server = ro.remoteObjectServer(
        svcname='real', name='a-thread-label', obj=Service(), host=HOST,
        logger=ro.nullLogger(), usethread=True, ns=False,
        transport='g2rpc-tcp', default_auth=True, method_list=['echo'])
    server.ro_start(wait=True, timeout=10)
    try:
        assert client('g2rpc-tcp', server.port, ('real', 'real'),
                      svcname='real').echo('hi') == 'hi'
        with pytest.raises(ro.remoteObjectError):
            client('g2rpc-tcp', server.port, ('a-thread-label',
                                              'a-thread-label'),
                   svcname='a-thread-label').echo('hi')
    finally:
        server.ro_stop(wait=True, timeout=10)


# ------------------------------------------- the cheap envelope mechanism --
#
# envelope_auth = 'credentials' carries the name and password in the header
# instead of signing with a key derived from the password.  It costs 0.3us a
# message against 6.5us, and does not grow with the message -- but it is a
# claim rather than a proof, and the password crosses the wire in the clear.
# It is a guard against a caller that has wandered into the wrong service.

CREDS = ['g2rpc', 'g2rpc-tcp', 'g2rpc-zmq']


@pytest.fixture
def creds_service():
    started = []

    def _make(transport, authDict=None, svcname='credsvc'):
        server = ro.remoteObjectServer(
            svcname=svcname, name=svcname, obj=Service(), host=HOST,
            logger=ro.nullLogger(), usethread=True, ns=False,
            transport=transport, default_auth=authDict is None,
            authDict=authDict, envelope_auth='credentials',
            method_list=['echo'])
        server.ro_start(wait=True, timeout=10)
        started.append(server)
        return server

    yield _make
    for server in started:
        try:
            server.ro_stop(wait=True, timeout=10)
        except Exception:
            pass


def creds_client(transport, port, auth, svcname='credsvc'):
    return ro.remoteObjectClient(HOST, port, name=svcname,
                                 transport=transport, auth=auth,
                                 default_auth=False, timeout=10,
                                 envelope_auth='credentials')


@pytest.mark.parametrize('transport', CREDS)
def test_credentials_admit_a_caller_that_knows_the_password(transport,
                                                            creds_service):
    server = creds_service(transport)
    handle = creds_client(transport, server.port, ('credsvc', 'credsvc'))

    assert handle.echo('hi') == 'hi'


@pytest.mark.parametrize('transport', CREDS)
def test_credentials_refuse_the_wrong_password(transport, creds_service):
    """The one that matters: a mechanism that admits everyone would pass the
    test above and be worthless."""
    server = creds_service(transport)
    handle = creds_client(transport, server.port, ('credsvc', 'wrong'))

    with pytest.raises(ro.remoteObjectError):
        handle.echo('hi')


@pytest.mark.parametrize('transport', CREDS)
def test_credentials_refuse_an_unknown_caller(transport, creds_service):
    server = creds_service(transport)
    handle = creds_client(transport, server.port, ('stranger', 'credsvc'))

    with pytest.raises(ro.remoteObjectError):
        handle.echo('hi')


@pytest.mark.parametrize('transport', CREDS)
def test_credentials_refuse_a_caller_that_sends_none(transport, creds_service):
    """The framing requires the section, so this is refused before the
    authenticator sees it."""
    server = creds_service(transport)
    handle = ro.remoteObjectClient(HOST, server.port, transport=transport,
                                   auth=None, default_auth=False, timeout=10)

    with pytest.raises(ro.remoteObjectError):
        handle.echo('hi')


def test_a_signing_caller_is_refused_by_a_credentials_service(creds_service):
    """Both ends have to agree.  A signer sends no credentials section, so
    the service refuses it -- which is the failure to expect if the config
    is changed at one end only."""
    server = creds_service('g2rpc-tcp')
    signer = ro.remoteObjectClient(HOST, server.port, name='credsvc',
                                   transport='g2rpc-tcp',
                                   auth=('credsvc', 'credsvc'),
                                   default_auth=False, timeout=10,
                                   envelope_auth='signature')

    with pytest.raises(ro.remoteObjectError):
        signer.echo('hi')


def test_the_password_does_travel_which_is_the_whole_trade(creds_service):
    """The counterpart of test_the_password_does_not_travel above: this
    mechanism is cheap because it sends the secret, and a test should say so
    rather than leave it to the docstring."""
    from tinyrpc.framing import Credentials

    creds = ro.client_credentials(ro_transport.get('g2rpc-tcp'),
                                  ('credsvc', 'sekrit'),
                                  mechanism='credentials')

    assert isinstance(creds, Credentials)
    assert b'sekrit' in creds.encode()


def test_signing_remains_the_default():
    """Nothing changes for a service that says nothing."""
    spec = ro_transport.get('g2rpc-tcp')

    assert ro.envelope_auth == 'signature'
    assert ro.client_credentials(spec, ('svc', 'pw')) is None
    assert ro.client_framing(spec, ('svc', 'pw'), 'svc') is not None


def test_a_credentials_service_insists_on_the_section_in_its_framing():
    """Belt as well as braces.  The authenticator refuses a request with no
    credentials on its own, so the framing's requirement is not what makes
    the service safe -- but it refuses one layer earlier, before anything is
    dispatched, and that is worth asserting directly since no end-to-end
    call can tell the two rejections apart.
    """
    from tinyrpc.framing import FLAG_CREDENTIALS

    framing = ro.server_framing(ro_transport.get('g2rpc-tcp'),
                                {'credsvc': 'credsvc'}, 'credsvc',
                                mechanism='credentials')

    assert framing.require & FLAG_CREDENTIALS


def credentials_arrived(username, password):
    """A request as it reaches a server, carrying the given credentials."""
    from tinyrpc.framing import Credentials
    plain = ro_g2rpc.G2RPCProtocol()
    sender = ro_g2rpc.G2RPCProtocol(
        credentials=Credentials(username, password))
    return plain.parse_request(sender.create_request('echo').serialize())


def test_the_credentials_authenticator_admits_a_matching_password():
    authenticator = ro.make_authenticator({'status': 'pw'}, ro.nullLogger(),
                                          'credentials')

    authenticator(None, credentials_arrived('status', 'pw'))


def test_the_credentials_authenticator_refuses_a_wrong_password():
    authenticator = ro.make_authenticator({'status': 'pw'}, ro.nullLogger(),
                                          'credentials')

    with pytest.raises(ro.remoteObjectError):
        authenticator(None, credentials_arrived('status', 'nope'))


def test_the_credentials_authenticator_refuses_a_name_it_does_not_know():
    """And refuses it in its own words.  Without the membership check the
    password comparison raises KeyError instead, which still refuses the
    caller but says nothing useful in the log and is not the error the rest
    of the module reports."""
    authenticator = ro.make_authenticator({'other': 'pw'}, ro.nullLogger(),
                                          'credentials')

    with pytest.raises(ro.remoteObjectError):
        authenticator(None, credentials_arrived('status', 'pw'))
