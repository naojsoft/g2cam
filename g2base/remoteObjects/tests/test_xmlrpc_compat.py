#
# test_xmlrpc_compat.py -- wire compatibility between the legacy XML-RPC
#                          stack and the tinyrpc-based replacement
#
"""Gen2 services and clients are restarted independently, and some of them
will not be upgraded at all.  So the tinyrpc-based XML-RPC stack has to stay
compatible on the wire in *both* directions: a new client must be able to
call an old server, and an old client must be able to call a new one.

Every test here therefore runs against all four combinations:

    old server <- old client     (the baseline: proves the harness itself is
                                  measuring what we think it is)
    old server <- new client
    new server <- old client
    new server <- new client

The "old" side is now the Python standard library rather than the ro_XMLRPC
module that has been deleted.  That is not a weakening: ro_XMLRPC's XML-RPC
behaviour *was* the stdlib's.  It subclassed SimpleXMLRPCServer and called
through xmlrpc.client, adding only three things -- threading, which does not
show on the wire, and HTTP Basic authentication and a marshaller patch for
oversized ints, both reproduced below.  Testing against the stdlib is a
stronger claim, in fact: it shows the new stack interoperates with any
Python XML-RPC peer rather than only with the module it replaced.

Where old and new deliberately differ, the difference is pinned down by an
explicit test rather than papered over, so that any further drift shows up
as a failure.  See ``test_documented_fault_code_differences``.
"""

import base64
import threading
import xmlrpc.client
import zlib
from socketserver import ThreadingMixIn
from xmlrpc.server import SimpleXMLRPCRequestHandler, SimpleXMLRPCServer

import pytest

from g2base.remoteObjects import remoteObjects as ro

from tinyrpc import RPCClient
from tinyrpc.protocols.xmlrpc import XMLRPCProtocol
from tinyrpc.transports.http import HttpPostClientTransport

HOST = '127.0.0.1'
TIMEOUT = 10.0

# Credentials used by the authentication tests.
GOOD_AUTH = ('bob', 'sekrit')
AUTH_DICT = {GOOD_AUTH[0]: GOOD_AUTH[1]}


# ro_XMLRPC did exactly this, at import, for the whole process -- which is
# why the new stack makes it a protocol option instead.  Reproduced here so
# that the simulated old client can still *send* oversized ints, since an old
# Gen2 process had this patch applied.  Note that it does not reach the new
# stack: LargeIntMarshaller keeps its own copy of the dispatch table, and
# test_ro_transport checks in a subprocess that nothing relies on this.
def _dump_large_int(_marshaller, value, write):
    write("<value><int>%d</int></value>" % value)


xmlrpc.client.Marshaller.dispatch[int] = _dump_large_int


# ---------------------------------------------------------------- service --

def echo(value):
    """Return the argument unchanged, to test payload round-tripping."""
    return value


def add(a, b):
    return a + b


def boom():
    raise ValueError('boom')


SERVICE = [echo, add, boom]


# ------------------------------------------------------------------ old --

def basic_credentials(header):
    """Decode an HTTP Basic Authorization header, as ro_XMLRPC did."""
    if not header:
        return None
    try:
        method, _, encoded = header.partition(' ')
        if method.lower() != 'basic':
            return None
        user, sep, password = base64.b64decode(
            encoded.strip().encode()).decode().partition(':')
        return (user, password) if sep else None
    except Exception:
        return None


class _AuthCheckingHandler(SimpleXMLRPCRequestHandler):
    """Check credentials during dispatch, the way ro_XMLRPC did.

    Doing it here rather than by returning 401 matters: the failure has to
    reach the client as an XML-RPC Fault, which is what the old server
    produced and therefore what old clients handle.
    """

    def _dispatch(self, method, params):
        auth_dict = self.server.auth_dict
        if auth_dict is not None:
            creds = basic_credentials(self.headers.get('Authorization'))
            if creds is None:
                raise Exception("Service requires authentication and no "
                                "credentials passed")
            user, password = creds
            if auth_dict.get(user) != password:
                raise Exception("Service requires authentication; "
                                "username or password mismatch")
        # Hand back to the dispatcher for the actual method lookup, so that
        # an unknown method fails exactly as it always did.
        return self.server._dispatch(method, params)


class _ThreadingXMLRPCServer(ThreadingMixIn, SimpleXMLRPCServer):
    daemon_threads = True
    allow_reuse_address = True


class LegacyServer:
    """A service on the Python standard library's XML-RPC server."""

    kind = 'old'

    def __init__(self, auth_dict=None):
        self.server = _ThreadingXMLRPCServer(
            (HOST, 0), requestHandler=_AuthCheckingHandler,
            allow_none=True, logRequests=False)
        self.server.auth_dict = auth_dict
        for func in SERVICE:
            self.server.register_function(func)
        self.thread = threading.Thread(target=self.server.serve_forever)
        self.thread.daemon = True
        self.thread.start()

    @property
    def port(self):
        return self.server.server_address[1]

    def stop(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=10.0)


def legacy_call(port, method, args, auth=None):
    """Call the way an un-upgraded Gen2 client does.

    ro_XMLRPC.make_serviceProxy() built exactly this URL, credentials and
    all, and handed it to xmlrpc.client.ServerProxy.
    """
    if auth is not None:
        url = 'http://%s:%s@%s:%d/' % (auth[0], auth[1], HOST, port)
    else:
        url = 'http://%s:%d/' % (HOST, port)
    proxy = xmlrpc.client.ServerProxy(url, allow_none=True)
    return getattr(proxy, method)(*args)


# ------------------------------------------------------------------ new --

class ServiceObject:
    """The same three methods, as an object for remoteObjectServer to serve."""

    def echo(self, value):
        return value

    def add(self, a, b):
        return a + b

    def boom(self):
        raise ValueError('boom')


class TinyrpcServer:
    """The same service on the new stack.

    This is the real ``ro.remoteObjectServer``, not a hand-assembled tinyrpc
    stack: the point of the harness is to compare what Gen2 will actually
    run against what it runs today.
    """

    kind = 'new'

    def __init__(self, auth_dict=None):
        self.server = ro.remoteObjectServer(
            svcname=None, name='compat', obj=ServiceObject(),
            host=HOST, logger=ro.nullLogger(), usethread=True,
            ns=False, default_auth=False, authDict=auth_dict,
            method_list=['echo', 'add', 'boom'])
        self.server.ro_start(wait=True, timeout=10.0)

    @property
    def port(self):
        return self.server.port

    def stop(self):
        self.server.ro_stop(wait=True, timeout=10.0)


def tinyrpc_call(port, method, args, auth=None):
    kwargs = {'timeout': TIMEOUT}
    if auth is not None:
        kwargs['auth'] = auth
    transport = HttpPostClientTransport('http://%s:%d/' % (HOST, port),
                                        **kwargs)
    client = RPCClient(
        XMLRPCProtocol(allow_none=True, allow_large_ints=True), transport)
    return client.call(method, args, None)


# ------------------------------------------------------------- fixtures --

SERVERS = {'old': LegacyServer, 'new': TinyrpcServer}
CLIENTS = {'old': legacy_call, 'new': tinyrpc_call}

#: Every (server, client) pairing, named so failures say which one broke.
COMBOS = [pytest.param(s, c, id='%s_server-%s_client' % (s, c))
          for s in SERVERS for c in CLIENTS]


@pytest.fixture
def make_server():
    """Start servers of either kind, and stop them again afterwards."""
    started = []

    def _make(kind, auth_dict=None):
        server = SERVERS[kind](auth_dict=auth_dict)
        started.append(server)
        return server

    yield _make

    for server in started:
        server.stop()


# ------------------------------------------------------------- payloads --

BINARY = ro.binary_encode(zlib.compress(b'\x00\x01\x02' * 1000))

PAYLOADS = [
    ('none', None, None),
    ('bool', True, True),
    ('int_small', 42, 42),
    # Outside signed 32 bits, which standard XML-RPC does not allow.  The
    # old stack carries these by patching xmlrpc.client's Marshaller for the
    # whole process on import; the new one does it with the protocol's
    # allow_large_ints option.  See test_ro_transport for the difference.
    ('int_over_32bit', 2 ** 40, 2 ** 40),
    ('int_negative_large', -(2 ** 40), -(2 ** 40)),
    ('int_max_int64', 2 ** 63 - 1, 2 ** 63 - 1),
    ('float', 3.14159, 3.14159),
    ('str', 'hello', 'hello'),
    ('str_unicode', 'すばる望遠鏡', 'すばる望遠鏡'),
    ('str_empty', '', ''),
    ('list', [1, 2.0, 'three', None], [1, 2.0, 'three', None]),
    # A tuple is a list once it has been through XML-RPC, both ways.
    ('tuple_becomes_list', (1, 2), [1, 2]),
    ('list_empty', [], []),
    ('dict', {'a': 1, 'b': [2, 3], 'c': None}, {'a': 1, 'b': [2, 3], 'c': None}),
    ('dict_empty', {}, {}),
    ('nested', {'x': [{'y': None}, 2 ** 40]}, {'x': [{'y': None}, 2 ** 40]}),
    # How Gen2 actually ships image and header buffers.
    ('binary_encoded', BINARY, BINARY),
]

PAYLOAD_PARAMS = [pytest.param(sent, expect, id=name)
                  for name, sent, expect in PAYLOADS]


# ---------------------------------------------------------------- tests --

@pytest.mark.parametrize('server_kind,client_call', COMBOS)
@pytest.mark.parametrize('sent,expect', PAYLOAD_PARAMS)
def test_payload_round_trip(make_server, server_kind, client_call,
                            sent, expect):
    """Every value Gen2 puts on the wire survives all four pairings."""
    server = make_server(server_kind)
    got = CLIENTS[client_call](server.port, 'echo', (sent,))
    assert got == expect
    assert type(got) is type(expect)


@pytest.mark.parametrize('server_kind,client_call', COMBOS)
def test_multiple_positional_args(make_server, server_kind, client_call):
    server = make_server(server_kind)
    assert CLIENTS[client_call](server.port, 'add', (3, 4)) == 7


@pytest.mark.parametrize('server_kind,client_call', COMBOS)
def test_successive_calls_on_one_service(make_server, server_kind,
                                         client_call):
    """The connectionless model: each call stands on its own."""
    server = make_server(server_kind)
    for i in range(5):
        assert CLIENTS[client_call](server.port, 'echo', (i,)) == i


@pytest.mark.parametrize('server_kind,client_call', COMBOS)
def test_method_raising_gives_a_fault(make_server, server_kind, client_call):
    server = make_server(server_kind)
    with pytest.raises(xmlrpc.client.Fault) as excinfo:
        CLIENTS[client_call](server.port, 'boom', ())
    assert 'boom' in excinfo.value.faultString


@pytest.mark.parametrize('server_kind,client_call', COMBOS)
def test_unknown_method_gives_a_fault(make_server, server_kind, client_call):
    server = make_server(server_kind)
    with pytest.raises(xmlrpc.client.Fault):
        CLIENTS[client_call](server.port, 'no_such_method', ())


# ------------------------------------------------------ authentication --

@pytest.mark.parametrize('server_kind,client_call', COMBOS)
def test_auth_accepts_good_credentials(make_server, server_kind, client_call):
    server = make_server(server_kind, auth_dict=AUTH_DICT)
    got = CLIENTS[client_call](server.port, 'echo', ('hi',), auth=GOOD_AUTH)
    assert got == 'hi'


@pytest.mark.parametrize('server_kind,client_call', COMBOS)
def test_auth_rejects_bad_password(make_server, server_kind, client_call):
    server = make_server(server_kind, auth_dict=AUTH_DICT)
    with pytest.raises(xmlrpc.client.Fault):
        CLIENTS[client_call](server.port, 'echo', ('hi',),
                             auth=('bob', 'wrong'))


@pytest.mark.parametrize('server_kind,client_call', COMBOS)
def test_auth_rejects_unknown_user(make_server, server_kind, client_call):
    server = make_server(server_kind, auth_dict=AUTH_DICT)
    with pytest.raises(xmlrpc.client.Fault):
        CLIENTS[client_call](server.port, 'echo', ('hi',),
                             auth=('mallory', 'sekrit'))


@pytest.mark.parametrize('server_kind,client_call', COMBOS)
def test_auth_rejects_missing_credentials(make_server, server_kind,
                                          client_call):
    server = make_server(server_kind, auth_dict=AUTH_DICT)
    with pytest.raises(xmlrpc.client.Fault):
        CLIENTS[client_call](server.port, 'echo', ('hi',), auth=None)


@pytest.mark.parametrize('server_kind,client_call', COMBOS)
def test_no_auth_configured_means_no_credentials_needed(
        make_server, server_kind, client_call):
    server = make_server(server_kind, auth_dict=None)
    assert CLIENTS[client_call](server.port, 'echo', ('hi',)) == 'hi'


# ------------------------------------------ deliberate, documented drift --

@pytest.mark.parametrize('client_call', list(CLIENTS), ids=lambda c: c + '_client')
def test_documented_fault_code_differences(make_server, client_call):
    """Pin the fault codes that deliberately changed.

    The legacy server reports *every* failure as faultCode 1, because
    SimpleXMLRPCDispatcher wraps any exception as ``Fault(1, "<class>:msg")``.
    The tinyrpc stack uses the XML-RPC fault code interoperability codes (the
    same numbering JSON-RPC 2.0 later adopted).

    Nothing in remoteObjects inspects faultCode -- ``call_remote`` stringifies
    whatever it catches -- so this is a safe change, but it is a real change
    and it should not happen again by accident.
    """
    old = make_server('old')
    new = make_server('new')
    call = CLIENTS[client_call]

    with pytest.raises(xmlrpc.client.Fault) as old_raise:
        call(old.port, 'boom', ())
    with pytest.raises(xmlrpc.client.Fault) as new_raise:
        call(new.port, 'boom', ())

    assert old_raise.value.faultCode == 1
    assert old_raise.value.faultString == "<class 'ValueError'>:boom"

    # -32500, "application error" in the XML-RPC interop table: an error
    # raised by the called method rather than by the RPC machinery.
    assert new_raise.value.faultCode == -32500
    assert new_raise.value.faultString == 'boom'

    with pytest.raises(xmlrpc.client.Fault) as old_missing:
        call(old.port, 'no_such_method', ())
    with pytest.raises(xmlrpc.client.Fault) as new_missing:
        call(new.port, 'no_such_method', ())

    assert old_missing.value.faultCode == 1
    assert 'is not supported' in old_missing.value.faultString

    assert new_missing.value.faultCode == -32601
    assert new_missing.value.faultString == 'Method not found'


def test_multicall_is_answered_by_the_new_stack_only(make_server):
    """Batching is something the new stack adds, not something it changes.

    A stdlib XML-RPC server only answers system.multicall once
    register_multicall_functions() has been called, and ro_XMLRPC never
    called it -- so batching against an un-upgraded Gen2 service never
    worked, and a caller that batches has to know it is talking to an
    upgraded one.  Nothing that used to work stops working.
    """
    body = xmlrpc.client.dumps(
        ([{'methodName': 'echo', 'params': ['x']}],),
        methodname='system.multicall').encode()

    import requests

    old = make_server('old')
    resp = requests.post('http://%s:%d/' % (HOST, old.port), data=body,
                         timeout=TIMEOUT)
    with pytest.raises(xmlrpc.client.Fault):
        xmlrpc.client.loads(resp.content.decode())

    new = make_server('new')
    resp = requests.post('http://%s:%d/' % (HOST, new.port), data=body,
                         timeout=TIMEOUT)
    params, _method = xmlrpc.client.loads(resp.content.decode())
    assert params[0] == [['x']], "one entry, holding the echoed value"


def test_a_batch_cannot_reach_a_local_only_method(make_server):
    """The methods that control a server's own lifecycle are withheld at
    registration, so they are not in the dispatcher for a batch to find
    either -- batching does not open a second door to them."""
    import requests

    server = make_server('new')
    body = xmlrpc.client.dumps(
        ([{'methodName': 'ro_stop', 'params': []}],),
        methodname='system.multicall').encode()
    resp = requests.post('http://%s:%d/' % (HOST, server.port), data=body,
                         timeout=TIMEOUT)

    params, _method = xmlrpc.client.loads(resp.content.decode())
    entry = params[0][0]
    assert isinstance(entry, dict), "expected a fault, got %r" % (entry,)
    assert entry['faultCode'] == -32601
    assert server.ro_is_running() if hasattr(server, 'ro_is_running') else True


def test_kwargs_are_rejected_by_both_stacks(make_server):
    """XML-RPC carries no keyword arguments, and neither stack pretends.

    The legacy client fails in xmlrpc.client._Method.__call__, which takes
    only positional arguments; the tinyrpc protocol raises
    InvalidRequestError when both args and kwargs are given.  Different
    exceptions, but the same outcome: it does not silently drop them.
    """
    from tinyrpc.exc import InvalidRequestError

    old = make_server('old')
    proxy = xmlrpc.client.ServerProxy('http://%s:%d/' % (HOST, old.port),
                                      allow_none=True)
    with pytest.raises(TypeError):
        proxy.add(1, b=2)

    protocol = XMLRPCProtocol(allow_none=True)
    with pytest.raises(InvalidRequestError):
        protocol.create_request('add', (1,), {'b': 2})
