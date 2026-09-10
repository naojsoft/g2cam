#
# test_multiprotocol.py -- one service, reachable several ways
#
"""A service can only be spoken to in the protocol it was started with, so
upgrading one has meant upgrading everything that calls it on the same day.
Listening several ways at once removes that: XML-RPC for whatever has not
been upgraded, something faster for whatever has.

The distinction the design turns on is between a *provider* and a *way in*.
Two providers of a name are alternatives to fail over between.  Two ways into
one provider are the same service, and retrying a failed call on another port
of the same process is not failover at all -- so they travel on one
registration rather than as separate entries.
"""

import pytest

from g2base.remoteObjects import remoteObjectNameSvc as ns_mod
from g2base.remoteObjects import remoteObjects as ro
from g2base.remoteObjects import ro_transport

HOST = '127.0.0.1'
WAYS = ['xmlrpc', 'g2rpc-tcp', 'g2rpc-zmq']


class FakePubSub:
    def subscribe(self, channel):
        pass

    def add_callback(self, channel, fn):
        pass

    def publish(self, channel, envelope, pack_info):
        pass


class Service:
    def echo(self, value):
        return value


@pytest.fixture
def nameservice():
    return ns_mod.remoteObjectNameService('names', FakePubSub(),
                                          ro.nullLogger(), HOST)


@pytest.fixture
def service(nameservice):
    started = []

    def _make(transports=WAYS, **kwargs):
        server = ro.remoteObjectServer(
            svcname='multi', obj=Service(), transport=transports,
            host=HOST, logger=ro.nullLogger(), usethread=True,
            ns=nameservice, default_auth=True, method_list=['echo'],
            **kwargs)
        server.ro_start(wait=True, timeout=15)
        started.append(server)
        return server

    yield _make
    for server in started:
        try:
            server.ro_stop(wait=True, timeout=15)
        except Exception:
            pass


# ------------------------------------------------------- one registration --

def test_several_listeners_are_one_registration(service, nameservice):
    """Not three providers of the same name, which is what registering each
    separately would have said."""
    server = service()
    info = nameservice.getInfo('multi')

    assert len(info) == 1
    assert len(server.ports) == 3
    assert len(set(server.ports)) == 3, "each listener has its own port"


def test_the_primary_is_what_an_un_upgraded_client_reads(service,
                                                         nameservice):
    """Such a client knows nothing of alternates and reads the top-level
    fields, so the first transport has to be the widely spoken one."""
    service()
    rec = nameservice.getInfo('multi')[0]

    assert rec['protocol'] == 'xmlrpc'
    assert rec['transport'] == 'xmlrpc'
    assert rec['port'] == rec['port']


def test_the_other_ways_are_listed_as_alternates(service, nameservice):
    server = service()
    rec = nameservice.getInfo('multi')[0]

    offered = {a['protocol']: a['port'] for a in rec['alternates']}
    assert set(offered) == {'g2rpc-tcp', 'g2rpc-zmq'}
    assert set(offered.values()) | {rec['port']} == set(server.ports)


def test_every_way_in_reaches_the_same_service(service):
    server = service()
    for transport, port in zip(WAYS, server.ports):
        client = ro.remoteObjectClient(HOST, port, name='multi',
                                       transport=transport,
                                       auth=('multi', 'multi'),
                                       default_auth=False, timeout=10)
        assert client.echo(transport) == transport


def test_one_transport_is_still_ordinary():
    """A list of one is a server like any other, not a special case."""
    server = ro.remoteObjectServer(svcname='one', obj=Service(), host=HOST,
                                   logger=ro.nullLogger(), usethread=True,
                                   ns=False, default_auth=False,
                                   transport=['xmlrpc'],
                                   method_list=['echo'])
    assert server.transports == ['xmlrpc']
    assert server.alternates == []


def test_a_bare_string_still_means_one_transport():
    """Which is how every existing service declares itself."""
    server = ro.remoteObjectServer(svcname='bare', obj=Service(), host=HOST,
                                   logger=ro.nullLogger(), usethread=True,
                                   ns=False, default_auth=False,
                                   transport='g2rpc-tcp',
                                   method_list=['echo'])
    assert server.transports == ['g2rpc-tcp']


def test_no_transport_at_all():
    with pytest.raises(ro.remoteObjectError):
        ro.remoteObjectServer(svcname='x', obj=Service(), host=HOST,
                              logger=ro.nullLogger(), ns=False,
                              transport=[], method_list=['echo'])


def test_an_unknown_transport_fails_before_anything_binds():
    with pytest.raises(Exception):
        ro.remoteObjectServer(svcname='x', obj=Service(), host=HOST,
                              logger=ro.nullLogger(), ns=False,
                              transport=['xmlrpc', 'nonsense'],
                              method_list=['echo'])


# ----------------------------------------------------------- choosing --

def record(protocol='xmlrpc', port=8000, encoding='xml', alternates=()):
    return dict(name='multi', host=HOST, port=port, protocol=protocol,
                transport=protocol, encoding=encoding,
                alternates=[dict(protocol=p, port=n, encoding=e)
                            for p, n, e in alternates])


def test_a_current_client_takes_the_fastest_it_can_speak():
    rec = record(alternates=[('g2rpc-tcp', 8001, 'msgpack'),
                             ('g2rpc-zmq', 8002, 'msgpack')])
    assert ro.choose_endpoint(rec) == ('g2rpc-zmq', 8002, 'msgpack')


def test_a_client_may_pin_what_it_wants():
    rec = record(alternates=[('g2rpc-tcp', 8001, 'msgpack'),
                             ('g2rpc-zmq', 8002, 'msgpack')])
    assert ro.choose_endpoint(rec, prefer=['g2rpc-tcp'])[0] == 'g2rpc-tcp'
    assert ro.choose_endpoint(rec, prefer=['xmlrpc'])[0] == 'xmlrpc'


def test_the_primary_is_the_fallback_not_the_default():
    """It is chosen to be what an un-upgraded caller can speak, which makes
    it by definition the oldest thing on offer -- so a caller that knows
    better should get something better, and one that asks for nothing on
    offer still gets a working call."""
    rec = record(alternates=[('g2rpc-zmq', 8002, 'msgpack')])
    assert ro.choose_endpoint(rec, prefer=['nothing-like-it']) == (
        'xmlrpc', 8000, 'xml')
    assert ro.choose_endpoint(rec, prefer=[])[0] == 'xmlrpc'


def test_a_protocol_this_end_cannot_load_is_skipped(monkeypatch):
    """0mq needs pyzmq, which is not everywhere.  Choosing it where it is
    missing would turn a working call into an ImportError."""
    spec = ro_transport.get('g2rpc-zmq')
    monkeypatch.setattr(type(spec), 'available', classmethod(lambda cls: False))

    rec = record(alternates=[('g2rpc-tcp', 8001, 'msgpack'),
                             ('g2rpc-zmq', 8002, 'msgpack')])
    assert ro.choose_endpoint(rec)[0] == 'g2rpc-tcp'


def test_a_protocol_newer_than_this_client_is_skipped():
    rec = record(alternates=[('g2rpc-quantum', 8009, None)])
    assert ro.choose_endpoint(rec)[0] == 'xmlrpc'


def test_a_registration_with_no_alternates_is_unchanged():
    assert ro.choose_endpoint(record()) == ('xmlrpc', 8000, 'xml')


def test_endpoints_in_lists_the_primary_first():
    rec = record(alternates=[('g2rpc-zmq', 8002, 'msgpack')])
    assert ro.endpoints_in(rec) == [('xmlrpc', 8000, 'xml'),
                                    ('g2rpc-zmq', 8002, 'msgpack')]


# ------------------------------------------------------------ the proxy --

def test_a_proxy_takes_the_faster_way_in(service, nameservice):
    server = service()
    proxy = ro.remoteObjectProxy('multi', ns=nameservice, timeout=10)

    assert proxy.echo('hi') == 'hi'
    chosen = proxy.endpoints.clients()[0]
    assert chosen.transport == 'g2rpc-zmq'
    assert chosen.port != server.port, "not the compatible primary"


def test_a_proxy_can_be_pinned_to_the_compatible_one(service, nameservice):
    server = service()
    proxy = ro.remoteObjectProxy('multi', ns=nameservice, timeout=10,
                                 prefer=['xmlrpc'])

    assert proxy.echo('hi') == 'hi'
    assert proxy.endpoints.clients()[0].port == server.port


def test_the_ways_in_are_not_treated_as_providers(service, nameservice):
    """Three listeners, one provider: a proxy must not think it has three
    machines to fail over between."""
    service()
    proxy = ro.remoteObjectProxy('multi', ns=nameservice, timeout=10)
    assert len(proxy.endpoints.clients()) == 1


# --------------------------------------------- what an old client is told --

def test_only_a_wire_compatible_protocol_reports_a_legacy_name():
    """An un-upgraded client acts on this field.  Telling it 'socket' for
    g2rpc-tcp would send it to a module that speaks a different format --
    worse than a name it does not know, which it refuses legibly."""
    assert ns_mod.legacy_transport_for('xmlrpc') == 'xmlrpc'

    for protocol in ('g2rpc', 'g2rpc-tcp', 'g2rpc-zmq', 'jsonrpc'):
        assert ns_mod.legacy_transport_for(protocol) == protocol


def test_reading_an_old_registration_is_the_other_direction():
    """And still maps the old names, because that is what now listens
    there."""
    assert ro_transport.resolve_legacy_transport('socket') == 'g2rpc-tcp'
    assert ro_transport.resolve_legacy_transport('zmqrpc') == 'g2rpc-zmq'


# ---------------------------------------------- a name service's tolerance --

def test_a_malformed_alternate_is_dropped_not_refused(nameservice):
    """Losing one way in beats losing the service."""
    opts = ns_mod.normalize_options(
        dict(protocol='xmlrpc',
             alternates=[dict(protocol='g2rpc-zmq', port=8002),
                         dict(protocol='g2rpc-tcp'),      # no port
                         'not even a dict',
                         dict(port=8003)]),               # no protocol
        ro.nullLogger())

    assert [a['protocol'] for a in opts['alternates']] == ['g2rpc-zmq']


def test_alternates_that_are_not_a_list_at_all(nameservice):
    opts = ns_mod.normalize_options(dict(protocol='xmlrpc', alternates='oops'),
                                    ro.nullLogger())
    assert opts['alternates'] == []


def test_a_registration_without_alternates_still_normalizes():
    opts = ns_mod.normalize_options(dict(protocol='xmlrpc'), ro.nullLogger())
    assert opts['alternates'] == []


# --------------------------------------------------- pinning a protocol --

def test_a_proxy_with_no_transport_follows_the_registration():
    """Which is the only thing that knows what a service speaks."""
    rec = record(alternates=[('g2rpc-zmq', 8002, 'msgpack')])
    assert ro.choose_endpoint(rec, pin=None)[0] == 'g2rpc-zmq'


def test_a_pinned_transport_is_used_and_no_other():
    """It used to be accepted and then disregarded whenever the record named
    a protocol -- a parameter that takes a value and ignores it."""
    rec = record(alternates=[('g2rpc-tcp', 8001, 'msgpack'),
                             ('g2rpc-zmq', 8002, 'msgpack')])

    assert ro.choose_endpoint(rec, pin='xmlrpc') == ('xmlrpc', 8000, 'xml')
    assert ro.choose_endpoint(rec, pin='g2rpc-tcp')[1] == 8001


def test_a_pin_beats_a_preference():
    rec = record(alternates=[('g2rpc-zmq', 8002, 'msgpack')])
    assert ro.choose_endpoint(rec, prefer=['g2rpc-zmq'],
                              pin='xmlrpc')[0] == 'xmlrpc'


def test_a_pin_the_service_cannot_honour_says_so():
    """Rather than dialling and waiting: a call in a protocol the far end
    does not speak is not answered, and a timeout says nothing about why."""
    rec = record()          # xmlrpc only, as an un-upgraded service is
    with pytest.raises(ro.remoteObjectError) as excinfo:
        ro.choose_endpoint(rec, pin='g2rpc-zmq')

    assert 'does not offer' in str(excinfo.value)
    assert 'xmlrpc' in str(excinfo.value), "it should say what is on offer"


def test_pinning_end_to_end(service, nameservice):
    server = service()
    plain = ro.remoteObjectProxy('multi', ns=nameservice, timeout=10)
    pinned = ro.remoteObjectProxy('multi', ns=nameservice, timeout=10,
                                  transport='xmlrpc')

    assert plain.echo('hi') == 'hi'
    assert pinned.echo('hi') == 'hi'
    assert plain.endpoints.clients()[0].transport == 'g2rpc-zmq'
    assert pinned.endpoints.clients()[0].port == server.port, \
        'the pinned proxy should be on the primary'


# ------------------------------------------- what un-upgraded code does --

def test_an_xmlrpc_only_service_is_reached_either_way(nameservice):
    """Both readings of a proxy that predates any of this: one that passes
    no transport, and one that passes 'xmlrpc' because that was the
    default."""
    server = ro.remoteObjectServer(
        svcname='plain', obj=Service(), host=HOST, logger=ro.nullLogger(),
        usethread=True, ns=nameservice, default_auth=False,
        method_list=['echo'])
    server.ro_start(wait=True, timeout=15)
    try:
        for kwargs in ({}, {'transport': 'xmlrpc'}):
            proxy = ro.remoteObjectProxy('plain', ns=nameservice, timeout=10,
                                         **kwargs)
            assert proxy.echo('hi') == 'hi'
            assert proxy.endpoints.clients()[0].transport == 'xmlrpc'
    finally:
        server.ro_stop(wait=True, timeout=15)


def test_an_explicit_host_list_still_gets_a_concrete_protocol(nameservice):
    """There is no registration to follow, so None cannot mean "ask" --
    it falls back to the module default, as it always did."""
    server = ro.remoteObjectServer(
        svcname='plain2', obj=Service(), host=HOST, logger=ro.nullLogger(),
        usethread=True, ns=nameservice, default_auth=False,
        method_list=['echo'])
    server.ro_start(wait=True, timeout=15)
    try:
        proxy = ro.remoteObjectProxy('plain2',
                                     hostports=[(HOST, server.port)],
                                     timeout=10)
        assert proxy.echo('hi') == 'hi'
        assert proxy.endpoints.clients()[0].transport == ro.default_transport
    finally:
        server.ro_stop(wait=True, timeout=15)


# ------------------------------------------- both ways to declare one --

def test_a_subclassed_service_can_offer_several(nameservice):
    """Subclassing is how Gen2 services are written today, and delegation is
    where they are going.  Neither should be the one that cannot do this."""
    class Subclassed(ro.remoteObjectServer):
        def __init__(self):
            ro.remoteObjectServer.__init__(
                self, svcname='sub', host=HOST, logger=ro.nullLogger(),
                usethread=True, ns=nameservice, default_auth=False,
                transport=['xmlrpc', 'g2rpc-zmq'])

        def echo(self, value):
            return value

    server = Subclassed()
    server.ro_start(wait=True, timeout=15)
    try:
        assert server.transports == ['xmlrpc', 'g2rpc-zmq']
        proxy = ro.remoteObjectProxy('sub', ns=nameservice, timeout=10)
        assert proxy.echo('hi') == 'hi'
        assert proxy.endpoints.clients()[0].transport == 'g2rpc-zmq'
        assert proxy.ro_echo('still there') == 'still there', \
            "the introspection methods must survive too"
    finally:
        server.ro_stop(wait=True, timeout=15)


def test_a_property_on_a_subclass_does_not_break_the_method_scan():
    """The scan calls getattr on everything the served object has, and a
    subclass *is* the served object -- so a property is evaluated before
    __init__ has finished.  It is not a method, so skipping it loses
    nothing; raising loses the whole service."""
    class WithProperty(ro.remoteObjectServer):
        @property
        def not_ready_yet(self):
            return self._never_set

        def __init__(self):
            ro.remoteObjectServer.__init__(
                self, svcname='prop', host=HOST, logger=ro.nullLogger(),
                usethread=True, ns=False, default_auth=False)

        def echo(self, value):
            return value

    server = WithProperty()
    assert 'echo' in server.method_list
    assert 'not_ready_yet' not in server.method_list


def test_every_listener_shares_one_pool_of_workers(nameservice):
    """One serve loop per listener, so the pool has to be sized for them --
    otherwise the last one finds no worker and the service accepts a
    request and hangs."""
    server = ro.remoteObjectServer(
        svcname='pool', obj=Service(), host=HOST, logger=ro.nullLogger(),
        usethread=True, ns=False, default_auth=False, numthreads=4,
        threaded_server=True, transport=WAYS, method_list=['echo'])

    assert server.executor._max_workers >= len(WAYS) + 4
