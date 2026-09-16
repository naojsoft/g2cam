#
# test_name_service.py -- registration records, old and new
#
"""The name service's record grew a ``protocol`` field.

The field it replaces, ``transport``, was named for the transport but held
protocol names -- which is why 'xmlrpc' sat in it alongside 'socket'.  Both
directions of the migration have to keep working while services and clients
are upgraded one at a time:

* a service that has not been upgraded registers with ``transport`` and no
  ``protocol``, and must still be understood; and
* a client that has not been upgraded reads ``transport`` from a lookup, so
  that field must go on being reported.

The second is the one that is easy to miss, and the one that would break
every un-upgraded client the moment the name service was upgraded.
"""

import pytest

from g2base.remoteObjects import remoteObjectNameSvc as ns_mod
from g2base.remoteObjects import remoteObjects as ro

HOST = 'testhost'


@pytest.fixture
def nameservice():
    return ns_mod.remoteObjectNameService('names', ro.nullLogger(), HOST)


def only_record(nameservice, name):
    info = nameservice.getInfo(name)
    assert len(info) == 1
    return info[0]


# --------------------------------------------------- registering, old shape --

def test_an_old_registration_is_understood(nameservice):
    """What an un-upgraded service sends: no protocol, and an encoding that
    was the old module default and never meant anything for XML-RPC."""
    nameservice.register('svc', HOST, 8000,
                         dict(transport='xmlrpc', encoding='pickle',
                              secure=False))

    rec = only_record(nameservice, 'svc')
    assert rec['protocol'] == 'xmlrpc'
    assert rec['encoding'] == 'xml', "recorded as what it really speaks"


@pytest.mark.parametrize('legacy,protocol', [
    ('xmlrpc', 'xmlrpc'),
    ('socket', 'g2rpc-tcp'),
    ('zmqrpc', 'g2rpc-zmq'),
])
def test_every_old_transport_value_translates(nameservice, legacy, protocol):
    """The old field could only ever hold these three, so the translation
    is complete."""
    nameservice.register('svc', HOST, 8000, dict(transport=legacy))
    assert only_record(nameservice, 'svc')['protocol'] == protocol


def test_the_oldest_api_still_works(nameservice):
    """Older still: options was a bare bool meaning 'secure'.

    Read as XML-RPC rather than as whatever the module default has become:
    a registration in this shape came from a caller old enough to send it,
    and that is what such a caller speaks.  Reading it as the current
    default would hand out an endpoint speaking something else.
    """
    nameservice.register('svc', HOST, 8000, True)

    rec = only_record(nameservice, 'svc')
    assert rec['secure'] is True
    assert rec['protocol'] == 'xmlrpc'


# --------------------------------------------------- registering, new shape --

def test_a_new_registration_is_recorded_as_given(nameservice):
    nameservice.register('svc', HOST, 8000,
                         dict(protocol='jsonrpc', encoding='json'))

    rec = only_record(nameservice, 'svc')
    assert rec['protocol'] == 'jsonrpc'
    assert rec['encoding'] == 'json'


def test_a_protocol_newer_than_us_is_recorded_not_refused(nameservice):
    """A name service should not stop a service registering something it has
    simply never heard of."""
    nameservice.register('svc', HOST, 8000,
                         dict(protocol='g2rpc-quantum', encoding='qubits'))

    rec = only_record(nameservice, 'svc')
    assert rec['protocol'] == 'g2rpc-quantum'
    assert rec['encoding'] == 'qubits'


# -------------------------------------------------------------- reporting --

def test_lookups_report_both_fields(nameservice):
    """An upgraded client reads 'protocol'; one that has not been upgraded
    reads 'transport'.  Both have to be there."""
    nameservice.register('svc', HOST, 8000, dict(protocol='xmlrpc'))

    rec = only_record(nameservice, 'svc')
    assert rec['protocol'] == 'xmlrpc'
    assert rec['transport'] == 'xmlrpc'


def test_a_protocol_with_no_old_name_reports_its_own(nameservice):
    """An un-upgraded client cannot speak JSON-RPC under any label, so it is
    told the real name and refuses it, rather than being handed 'xmlrpc' and
    talking XML at a service that does not speak it."""
    nameservice.register('svc', HOST, 8000, dict(protocol='jsonrpc'))

    rec = only_record(nameservice, 'svc')
    assert rec['protocol'] == 'jsonrpc'
    assert rec['transport'] == 'jsonrpc'


def test_an_unknown_protocol_reports_itself(nameservice):
    nameservice.register('svc', HOST, 8000, dict(protocol='g2rpc-quantum'))
    assert only_record(nameservice, 'svc')['transport'] == 'g2rpc-quantum'


def test_a_lookup_still_has_everything_an_old_client_reads(nameservice):
    """servicePack.syncFrom() read exactly these keys."""
    nameservice.register('svc', HOST, 8000, dict(transport='xmlrpc'))

    rec = only_record(nameservice, 'svc')
    for key in ('name', 'host', 'port', 'secure', 'transport', 'encoding',
                'pingtime', 'registrar', 'keep'):
        assert key in rec, key


# ------------------------------------------------------ peer name servers --

def share(sender, receiver):
    """One name server's own registrations, merged by another.

    Exactly what a round of the exchange does: the receiver asks for
    getInfoMine() and merges what comes back.
    """
    return receiver.merge_registrations(
        dict(registrar=sender.myhost, registrar_id=sender.node_id,
             names=sender.getInfoMine()))


def test_registrations_shared_between_name_servers_carry_the_protocol():
    """Name servers converge by publishing their own registrations to each
    other, so the new field has to travel."""
    a = ns_mod.remoteObjectNameService('names', ro.nullLogger(), 'hostA')
    a.register('svc', 'hostA', 8000, dict(protocol='jsonrpc'))

    assert a.getInfoMine()[0]['protocol'] == 'jsonrpc'

    # ... and arrives intact at a peer.
    b = ns_mod.remoteObjectNameService('names', ro.nullLogger(), 'hostB')
    share(a, b)

    rec = only_record(b, 'svc')
    assert rec['protocol'] == 'jsonrpc'
    assert rec['registrar'] == 'hostA'


def test_a_shared_registration_keeps_its_other_ways_in():
    """The primary is deliberately the oldest protocol a service offers, so
    a peer that drops the alternates does not fail -- it silently resolves
    every such service to XML-RPC, which is exactly the thing multi-protocol
    registration exists to stop."""
    a = ns_mod.remoteObjectNameService('names', ro.nullLogger(), 'hostA')
    a.register('svc', 'hostA', 8000,
               dict(protocol='xmlrpc',
                    alternates=[dict(protocol='g2rpc-tcp', port=8001,
                                     encoding='msgpack')]))

    b = ns_mod.remoteObjectNameService('names', ro.nullLogger(), 'hostB')
    share(a, b)

    rec = only_record(b, 'svc')
    assert rec['alternates'] == [dict(protocol='g2rpc-tcp', port=8001,
                                      encoding='msgpack')]

    # What the whole thing is for: a client asking the peer still finds the
    # faster way in, rather than falling back to the primary.
    assert ro.choose_endpoint(rec) == ('g2rpc-tcp', 8001, 'msgpack')


def test_a_shared_registration_with_no_alternates_is_unchanged():
    a = ns_mod.remoteObjectNameService('names', ro.nullLogger(), 'hostA')
    a.register('svc', 'hostA', 8000, dict(protocol='g2rpc-tcp'))

    b = ns_mod.remoteObjectNameService('names', ro.nullLogger(), 'hostB')
    share(a, b)

    assert only_record(b, 'svc')['alternates'] == []


def test_a_peer_revalidates_what_it_is_sent():
    """A peer's registrations arrive over the wire like any other, so they
    are normalized on the way in rather than trusted -- one bad alternate
    costs that way in, not the service."""
    b = ns_mod.remoteObjectNameService('names', ro.nullLogger(), 'hostB')

    b.merge_registrations(dict(
        registrar='hostA',
        names=[dict(name='svc', host='hostA', port=8000, pingtime=1.0,
                    registrar='hostA', protocol='xmlrpc',
                    alternates=[dict(protocol='g2rpc-tcp', port=8001),
                                dict(protocol='g2rpc-tcp'),   # no port
                                'not even a dict'])]))

    rec = only_record(b, 'svc')
    assert rec['alternates'] == [dict(protocol='g2rpc-tcp', port=8001,
                                      encoding='msgpack')]


def test_every_shared_field_is_one_normalize_options_reads():
    """The two lists have to agree.  They did not, which is how alternates
    came to be registered and not shared."""
    known = ns_mod.normalize_options(dict(protocol='xmlrpc'),
                                     ro.nullLogger())

    for field in ns_mod.SHARED_FIELDS:
        assert field in known or field == 'transport', \
            "'%s' is shared but normalize_options does not read it" % (field,)


# -------------------------------------------------------- normalization --

def test_normalize_options_rejects_what_it_cannot_read():
    with pytest.raises(ns_mod.nameServiceError):
        ns_mod.normalize_options(['not', 'a', 'dict'], ro.nullLogger())


# ------------------------------------------------------------ end to end --

class EchoService:
    def echo(self, value):
        return value


def test_a_server_registers_and_a_proxy_finds_it(nameservice):
    """The whole loop: a server publishes what it speaks, the name service
    records it, and a proxy builds the right client from the record."""
    svc = ro.remoteObjectServer(
        svcname='echosvc', name='echosvc', obj=EchoService(),
        host='127.0.0.1', logger=ro.nullLogger(), usethread=True,
        ns=nameservice, default_auth=False, method_list=['echo'])
    svc.ro_start(wait=True, timeout=10.0)
    try:
        rec = only_record(nameservice, 'echosvc')
        assert rec['protocol'] == 'g2rpc-tcp', "the current default"
        assert rec['encoding'] == 'msgpack'
        # The legacy field carries the protocol's own name where it has no
        # older equivalent, so a client too old to read 'protocol' finds a
        # name it does not know and refuses, rather than one it misreads.
        assert rec['transport'] == 'g2rpc-tcp'
        assert rec['port'] == svc.port

        proxy = ro.remoteObjectProxy('echosvc', ns=nameservice,
                                     default_auth=False, timeout=10.0)
        assert proxy.echo('hi') == 'hi'
    finally:
        svc.ro_stop(wait=True, timeout=10.0)


def test_a_proxy_can_read_a_record_written_by_an_old_service(nameservice):
    """A service that has not been upgraded registers the old way; a current
    client must still be able to build a working proxy from that record."""
    svc = ro.remoteObjectServer(
        svcname=None, name='echosvc', obj=EchoService(),
        host='127.0.0.1', logger=ro.nullLogger(), usethread=True,
        # An un-upgraded service speaks XML-RPC, whatever the default here
        # has since become -- which is the whole premise of this test.
        transport='xmlrpc',
        ns=False, default_auth=False, method_list=['echo'])
    svc.ro_start(wait=True, timeout=10.0)
    try:
        # Registered by hand, in the shape an un-upgraded service sends.
        nameservice.register('echosvc', '127.0.0.1', svc.port,
                             dict(transport='xmlrpc', encoding='pickle',
                                  secure=False))

        proxy = ro.remoteObjectProxy('echosvc', ns=nameservice,
                                     default_auth=False, timeout=10.0)
        assert proxy.echo('hi') == 'hi'
    finally:
        svc.ro_stop(wait=True, timeout=10.0)


# ----------------------------------------------------------- two ways in --
#
# The name service cannot be looked up -- it is what lookups go through --
# so a second protocol cannot be advertised in a registration the way every
# other service advertises one.  It lives on a second agreed port instead,
# and a client finds it by trying.

def test_the_faster_way_in_comes_first(monkeypatch):
    monkeypatch.setattr(ro, 'ns_rpc_transport', 'g2rpc-tcp')
    monkeypatch.setattr(ro, 'ns_rpc_encoding', None)

    ways = ro.ns_endpoints()

    assert ways[0] == (ro.nameServiceRpcPort, 'g2rpc-tcp', None)
    assert ways[-1] == (ro.nameServicePort, ro.ns_transport, ro.ns_encoding)


def test_turning_it_off_leaves_only_the_old_way(monkeypatch):
    """ns_rpc_transport = None is how a site stops offering the second port
    -- and, on the client side, stops paying to try it."""
    monkeypatch.setattr(ro, 'ns_rpc_transport', None)

    assert ro.ns_endpoints() == [(ro.nameServicePort, ro.ns_transport,
                                  ro.ns_encoding)]


def test_the_faster_way_is_used_when_it_answers(monkeypatch):
    monkeypatch.setattr(ro, 'ns_rpc_transport', 'g2rpc-tcp')
    monkeypatch.setattr(ro, 'ns_rpc_encoding', None)
    tried = []

    class Handle:
        def __init__(self, port):
            self.port = port

        def ro_echo(self, value):
            tried.append(self.port)
            return value

    handle = ro._first_working(lambda port, protocol, encoding: Handle(port))

    assert handle.port == ro.nameServiceRpcPort
    assert tried == [ro.nameServiceRpcPort], 'the old port was not needed'


def test_it_falls_back_when_the_faster_port_refuses(monkeypatch):
    """A port with nothing behind it refuses at once, which is what makes
    trying cheaper than asking."""
    monkeypatch.setattr(ro, 'ns_rpc_transport', 'g2rpc-tcp')
    monkeypatch.setattr(ro, 'ns_rpc_encoding', None)
    tried = []

    class Handle:
        def __init__(self, port):
            self.port = port

        def ro_echo(self, value):
            tried.append(self.port)
            raise ro.remoteObjectError('connection refused')

    handle = ro._first_working(lambda port, protocol, encoding: Handle(port))

    assert handle.port == ro.nameServicePort
    assert tried == [ro.nameServiceRpcPort], \
        'the last way in is handed back unproven'


def test_the_last_way_in_is_not_probed(monkeypatch):
    """There is nothing left to fall back to, so proving it costs a round
    trip and buys nothing: a caller that cannot reach the name service at
    all should hear about it from its own call, in its own terms."""
    monkeypatch.setattr(ro, 'ns_rpc_transport', None)
    tried = []

    class Handle:
        def ro_echo(self, value):
            tried.append(1)
            return value

    ro._first_working(lambda port, protocol, encoding: Handle())

    assert tried == []


def test_the_name_service_records_both_ways_in(monkeypatch):
    """Nothing needs to read this to find it -- a client that could read it
    has found it already -- but a record describing half the service would
    be a lie to anything listing what is running."""
    monkeypatch.setattr(ro, 'ns_rpc_transport', 'g2rpc-tcp')
    monkeypatch.setattr(ro, 'ns_rpc_encoding', None)
    nssvc = ns_mod.remoteObjectNameService('names', ro.nullLogger(), HOST)

    nssvc.register_self()

    rec = only_record(nssvc, 'names')
    assert rec['protocol'] == ro.ns_transport
    assert rec['port'] == ro.nameServicePort
    # Recorded with a concrete encoding rather than the None it was given:
    # the record is what a reader builds a client from, and 'whatever the
    # default is' is not something a reader can act on.
    assert rec['alternates'] == [dict(protocol='g2rpc-tcp',
                                      port=ro.nameServiceRpcPort,
                                      encoding='msgpack')]


def test_it_records_one_way_in_when_that_is_all_there_is(monkeypatch):
    monkeypatch.setattr(ro, 'ns_rpc_transport', None)
    nssvc = ns_mod.remoteObjectNameService('names', ro.nullLogger(), HOST)

    nssvc.register_self()

    assert only_record(nssvc, 'names')['alternates'] == []


def test_both_ports_answer_the_same_service():
    """End to end, on two real sockets: the same name service, reached the
    way an upgraded client reaches it and the way an un-upgraded one does."""
    nsobj = ns_mod.remoteObjectNameService('names', ro.nullLogger(), '127.0.0.1')
    nssvc = ro.remoteObjectServer(
        name='names', obj=nsobj, svcname=None, host='127.0.0.1',
        transport=[ro.ns_transport, 'g2rpc-tcp'], encoding=ro.ns_encoding,
        port=[0, 0], logger=ro.nullLogger(), usethread=True,
        ns=False, default_auth=False)
    nssvc.ro_start(wait=True, timeout=10.0)
    try:
        old_port, new_port = nssvc.ports
        assert old_port != new_port

        old = ro.remoteObjectClient(host='127.0.0.1', port=old_port,
                                    transport=ro.ns_transport,
                                    encoding=ro.ns_encoding, auth=None)
        new = ro.remoteObjectClient(host='127.0.0.1', port=new_port,
                                    transport='g2rpc-tcp', auth=None)

        assert old.ro_echo(1) == 1
        assert new.ro_echo(1) == 1

        # And the state behind them is one service, not two.
        old.register('svc', '127.0.0.1', 9999,
                     dict(protocol='g2rpc-tcp', encoding='msgpack'))
        assert new.getInfo('svc')[0]['port'] == 9999
    finally:
        nssvc.ro_stop(wait=True, timeout=10.0)


# -------------------------------------------------- mesh peer name servers --
#
# The same exchange the pubsub does, straight between name servers: each
# asks every other for its own registrations.  Everything here turns on the
# merge being idempotent and one-hop.

def make_ns(host, peers=()):
    return ns_mod.remoteObjectNameService('names', ro.nullLogger(), host,
                                          peer_hosts=list(peers))


def test_a_seed_is_a_peer_before_it_has_answered():
    """Which is the whole point of seeding: a dozen name servers started at
    the same instant have not heard of each other, so a peer set discovered
    only from the white pages would start empty on every node at once."""
    a = make_ns('hostA', peers=['hostB'])

    assert a.peers() == ['hostB']


def test_we_are_never_our_own_peer():
    a = make_ns('hostA', peers=['hostA', 'hostB'])
    a.register_self()

    assert 'hostA' not in a.peers()


def test_peers_grow_from_the_white_pages():
    """Every name server registers itself under the name they all share, so
    the directory of services is also the directory of directories."""
    a = make_ns('hostA', peers=['hostB'])
    a.register('names', 'hostC', ro.nameServicePort, dict(keep=True))

    assert a.peers() == ['hostB', 'hostC']


def test_a_peer_that_goes_away_is_still_asked():
    """peers() must not shrink back to the seeds: a name server that has
    stopped answering is exactly the one whose absence we need to keep
    noticing, and a seed may be the host that died."""
    a = make_ns('hostA', peers=[])
    a.register('names', 'hostB', ro.nameServicePort, dict(keep=True))
    a.forget_peer('hostB')

    assert a.peers() == ['hostB']


def test_a_pull_merges_what_the_peer_holds(monkeypatch):
    a = make_ns('hostA', peers=['hostB'])

    class Peer:
        def getInfoMine(self):
            return [dict(name='svc', host='hostB', port=8000, pingtime=1.0,
                         registrar='hostB', protocol='g2rpc-tcp',
                         encoding='msgpack')]

    monkeypatch.setattr(a, '_peer_client', lambda host: Peer())

    assert a.pull_from_peer('hostB') == 1
    assert only_record(a, 'svc')['registrar'] == 'hostB'


def test_a_peer_that_cannot_be_reached_is_not_an_error(monkeypatch):
    """A name server going away is an ordinary event.  Its registrations age
    out; the round must not stop on the way to the peers behind it."""
    a = make_ns('hostA', peers=['down', 'up'])
    reached = []

    class Peer:
        def getInfoMine(self):
            return []

        def getPeers(self):
            return ['up']

    def client(host):
        if host == 'down':
            raise OSError('connection refused')
        reached.append(host)
        return Peer()

    monkeypatch.setattr(a, '_peer_client', client)
    monkeypatch.setattr(a, 'announce_to_seeds', lambda: 0)

    a.exchange_with_peers()

    assert 'up' in reached, 'the dead peer stopped the round'
    assert 'down' not in reached


def test_an_unreachable_peer_is_dialled_again_next_round(monkeypatch):
    """It may have come back on a different protocol -- it may have been
    upgraded, which is why there is more than one way in."""
    a = make_ns('hostA', peers=['hostB'])

    class Peer:
        def getInfoMine(self):
            raise OSError('connection reset')

    a._peer_clients['hostB'] = Peer()
    a.pull_from_peer('hostB')

    assert 'hostB' not in a._peer_clients


def test_we_do_not_take_our_own_registrations_back(monkeypatch):
    """A record of ours that reaches a peer and comes back must not
    overwrite ours: we heard it first-hand and they did not."""
    a = make_ns('hostA', peers=['hostB'])
    a.register('svc', 'hostA', 8000, dict(protocol='g2rpc-tcp'))
    ours = only_record(a, 'svc')['pingtime']

    a.merge_registrations(dict(registrar='hostB', names=[
        dict(name='svc', host='hostA', port=8000, pingtime=ours - 100,
             registrar='hostA', protocol='xmlrpc')]))

    rec = only_record(a, 'svc')
    assert rec['protocol'] == 'g2rpc-tcp'
    assert rec['pingtime'] == ours


def test_merging_the_same_round_twice_changes_nothing():
    """Idempotence is what lets this run beside the pubsub during a
    migration, and what lets a missed round repair itself on the next."""
    a = make_ns('hostA')
    env = dict(registrar='hostB', names=[
        dict(name='svc', host='hostB', port=8000, pingtime=1.0,
             registrar='hostB', protocol='g2rpc-tcp')])

    a.merge_registrations(env)
    first = dict(only_record(a, 'svc'))
    a.merge_registrations(env)

    assert only_record(a, 'svc') == first


def test_one_bad_registration_does_not_cost_the_rest():
    a = make_ns('hostA')

    merged = a.merge_registrations(dict(registrar='hostB', names=[
        dict(name='bad', host='hostB'),                     # no port, no time
        dict(name='good', host='hostB', port=8000, pingtime=1.0,
             registrar='hostB', protocol='g2rpc-tcp')]))

    assert merged == 1
    assert a.getNames() == ['good']


def test_only_first_hand_registrations_are_shared():
    """The one-hop rule.  Nothing forwards what it was told, so a record
    stays one hop from its registrar -- which is what makes 'registrar'
    mean anything and makes loops impossible without detecting them."""
    a = make_ns('hostA')
    a.register('mine', 'hostA', 8000, dict(protocol='g2rpc-tcp'))
    a.merge_registrations(dict(registrar='hostB', names=[
        dict(name='theirs', host='hostB', port=8001, pingtime=1.0,
             registrar='hostB', protocol='g2rpc-tcp')]))

    shared = [rec['name'] for rec in a.getInfoMine()]

    assert shared == ['mine'], 'a peer\'s record was passed on as our own'


def test_two_name_servers_converge_over_real_sockets():
    """End to end: B is told only about A, and the two finish holding the
    same services -- each still recorded against the node that heard it
    first-hand."""
    servers = []

    def serve(nsobj):
        svc = ro.remoteObjectServer(
            name='names', obj=nsobj, svcname=None, host='127.0.0.1',
            transport=ro.ns_transport, encoding=ro.ns_encoding, port=0,
            logger=ro.nullLogger(), usethread=True, ns=False,
            default_auth=False)
        svc.ro_start(wait=True, timeout=10.0)
        servers.append(svc)
        return svc

    # Distinct names, because a name server identifies itself by host: two
    # sharing one would each take the other's registrations for their own.
    a, b = make_ns('hostA'), make_ns('hostB')
    a.register('from_a', 'hostA', 8000, dict(protocol='g2rpc-tcp'))
    b.register('from_b', 'hostB', 8001, dict(protocol='xmlrpc'))

    try:
        a_svc = serve(a)
        # Reach A on the port it actually got, the way a seeded peer would.
        b._peer_clients['hostA'] = ro.remoteObjectClient(
            host='127.0.0.1', port=a_svc.port, transport=ro.ns_transport,
            encoding=ro.ns_encoding, auth=None, timeout=10.0)
        b.seed_hosts = ['hostA']

        assert b.pull_from_peer('hostA') == 1

        assert sorted(b.getNames()) == ['from_a', 'from_b']
        assert only_record(b, 'from_a')['registrar'] == 'hostA'
        assert only_record(b, 'from_a')['protocol'] == 'g2rpc-tcp'
        # ... and B still knows which of them is its own to share on.
        assert [r['name'] for r in b.getInfoMine()] == ['from_b']
    finally:
        for svc in servers:
            svc.ro_stop(wait=True, timeout=10.0)


# ----------------------------------------------------- membership, not data --
#
# Registrations travel one hop and membership travels any number, because
# the two are believed differently: a relayed service record is a claim you
# must trust, a relayed name server address proves itself when you try it.

def test_an_announcement_is_recorded_against_the_announcer():
    """Not against us.  A name server that claimed an announcement as its
    own would pass it on as first-hand, which is the one thing the exchange
    must never do."""
    seed = make_ns('seed')

    seed.announce_peer('hostB', ro.nameServicePort, dict(keep=True))

    assert only_record(seed, 'names')['registrar'] == 'hostB'
    assert seed.getInfoMine() == [], 'the seed passed it on as its own'


def test_membership_reaches_a_node_the_seed_cannot_share_with():
    """The case that breaks if membership rides on registrations: the seed
    knows everyone first-hand, so under the one-hop rule it has nothing to
    pass on, and a third node never hears of the second."""
    seed, b = make_ns('seed'), make_ns('hostB', peers=['seed'])
    seed.announce_peer('hostB', ro.nameServicePort, dict(keep=True))
    seed.announce_peer('hostC', ro.nameServicePort, dict(keep=True))

    class Peer:
        def getPeers(self):
            return seed.getPeers()

    b._peer_clients['seed'] = Peer()
    b.learn_peers_from('seed')

    assert 'hostC' in b.peers(), 'membership stopped at the seed'


def test_getPeers_includes_us():
    """Otherwise a node nobody has announced to would be invisible to the
    peers that ask it who else is out there."""
    a = make_ns('hostA')

    assert a.getPeers() == ['hostA']


def test_a_peer_learned_second_hand_is_not_a_registration():
    """It is a place to go and ask, and asking is what verifies it.  Until
    then it has no business being reported to a client as a service."""
    a = make_ns('hostA', peers=['hostB'])

    assert a.peers() == ['hostB']
    assert a.getNames() == []


def test_learning_the_same_peers_twice_is_quiet(monkeypatch):
    a = make_ns('hostA', peers=['hostB'])

    class Peer:
        def getPeers(self):
            return ['hostB', 'hostC']

    monkeypatch.setattr(a, '_peer_client', lambda host: Peer())

    assert a.learn_peers_from('hostB') == 1
    assert a.learn_peers_from('hostB') == 0


def test_a_peer_that_cannot_answer_who_it_knows_is_redialled(monkeypatch):
    a = make_ns('hostA', peers=['hostB'])

    class Peer:
        def getPeers(self):
            raise OSError('connection reset')

    a._peer_clients['hostB'] = Peer()
    a.learn_peers_from('hostB')

    assert 'hostB' not in a._peer_clients


def test_a_peer_learned_second_hand_is_passed_on(monkeypatch):
    """Membership has to be transitive, or a chain of introductions stops at
    the second link: whoever hears of a node has to be able to mention it,
    whether they met it or were told about it."""
    a = make_ns('hostA', peers=['hostB'])

    class Peer:
        def getPeers(self):
            return ['hostC']

    monkeypatch.setattr(a, '_peer_client', lambda host: Peer())
    a.learn_peers_from('hostB')

    assert 'hostC' in a.getPeers(), 'what we were told, we cannot repeat'
    assert a.getNames() == [], 'and it is still not a registration'


# ------------------------------------------------- who a name server is --
#
# Identity used to be the host name, which answers the wrong question twice.

def test_two_name_servers_on_one_host_are_not_the_same_one():
    """The case that made a cluster impossible to rehearse on one machine:
    both would take the other's registrations for their own and share
    nothing, because 'is this mine' was 'is this my host'."""
    a = ns_mod.remoteObjectNameService('names', ro.nullLogger(), 'host',
                                       port=7075)
    b = ns_mod.remoteObjectNameService('names', ro.nullLogger(), 'host',
                                       port=7085)

    assert a.node_id != b.node_id

    a.register('svc', 'host', 8000, dict(protocol='g2rpc-tcp'))
    assert share(a, b) == 1
    assert only_record(b, 'svc')['name'] == 'svc'


def test_an_id_outlives_the_process():
    """It has to.  A name server that came back with a new id would not know
    its own registrations in a peer's copy, and would take them back as
    somebody else's -- resurrecting what it had just forgotten."""
    before = ns_mod.derive_node_id('hostA', 7075)
    after = ns_mod.derive_node_id('hostA', 7075)

    assert before == after


def test_a_restarted_name_server_does_not_resurrect_its_own_names():
    """Which is what the stable id is for.  The peer still holds what we
    registered before we restarted; we must not take it back."""
    a = ns_mod.remoteObjectNameService('names', ro.nullLogger(), 'hostA')
    a.register('gone', 'hostA', 8000, dict(protocol='g2rpc-tcp'))
    b = ns_mod.remoteObjectNameService('names', ro.nullLogger(), 'hostB')
    share(a, b)

    # 'a' restarts: same host, same port, so the same id, and empty.
    a = ns_mod.remoteObjectNameService('names', ro.nullLogger(), 'hostA')
    share(b, a)

    assert a.getNames() == [], 'took back a registration it had forgotten'


def test_an_explicit_id_survives_a_rename():
    """The one thing deriving from the host name cannot do."""
    before = ns_mod.derive_node_id('old-name', 7075, configured='ns-summit-1')
    after = ns_mod.derive_node_id('new-name', 7075, configured='ns-summit-1')

    assert before == after


def test_a_record_says_who_heard_it_first_hand():
    a = ns_mod.remoteObjectNameService('names', ro.nullLogger(), 'hostA')
    a.register('svc', 'hostA', 8000, dict(protocol='g2rpc-tcp'))

    rec = only_record(a, 'svc')
    assert rec['registrar_id'] == a.node_id
    assert rec['registrar'] == 'hostA', 'the readable form stays too'


def test_a_record_from_before_ids_is_still_ours_by_host():
    """A registration written by a name server that has not been upgraded
    carries no id, so the host name has to go on answering for it."""
    a = ns_mod.remoteObjectNameService('names', ro.nullLogger(), 'hostA')

    assert a.is_ours(dict(registrar='hostA'))
    assert not a.is_ours(dict(registrar='hostB'))


def test_an_id_beats_the_host_name_when_both_are_there():
    """Two name servers on one host have the same host name and different
    ids, so the id has to be the one that decides."""
    a = ns_mod.remoteObjectNameService('names', ro.nullLogger(), 'host',
                                       port=7075)

    assert not a.is_ours(dict(registrar='host', registrar_id='someone-else'))
    assert a.is_ours(dict(registrar='elsewhere', registrar_id=a.node_id))


def test_a_name_server_reports_its_own_id():
    a = ns_mod.remoteObjectNameService('names', ro.nullLogger(), 'hostA')

    assert a.getNodeId() == a.node_id


def test_a_host_that_turns_out_to_be_us_stops_being_a_peer(monkeypatch):
    """DNS gives one name server as many names as it likes, so a node can be
    told to peer with itself.  Without an id there is no way to notice: the
    records come back looking like a stranger's and get merged as such."""
    a = ns_mod.remoteObjectNameService('names', ro.nullLogger(), 'hostA')

    class Mirror:
        def getNodeId(self):
            return a.node_id

    monkeypatch.setattr(ro, 'remoteObjectClient',
                        lambda **kwargs: Mirror())
    a._known_peers.add('hostA.long.form')

    with pytest.raises(ns_mod.nameServiceError):
        a._peer_client('hostA.long.form')

    assert 'hostA.long.form' not in a.peers()


# ------------------------------------------------------ one host, alone --
#
# Gen2 is run on a single host for testing, where there is one name server
# and nothing to exchange with.  That has to be an unremarkable state, and
# --peer=localhost has to mean "there is nobody else" rather than "peer with
# whoever is reading this".

def test_seeding_with_localhost_leaves_no_peers():
    a = ns_mod.remoteObjectNameService('names', ro.nullLogger(), 'hostA',
                                       peer_hosts=['localhost'])

    assert a.peers() == []
    assert a.seed_hosts == []


def test_a_loopback_name_is_never_passed_to_another_name_server():
    """It names the asker, so it means this machine here and a different
    one everywhere it might be repeated.  Left in, every name server in the
    cluster would eventually try it and find itself."""
    a = ns_mod.remoteObjectNameService('names', ro.nullLogger(), 'hostA',
                                       peer_hosts=['localhost', 'hostB'])

    assert a.getPeers() == ['hostA', 'hostB']


def test_a_loopback_name_heard_from_a_peer_is_ignored(monkeypatch):
    """A name server running on one host may well have been told to peer
    with localhost, and may say so when asked who it knows."""
    a = ns_mod.remoteObjectNameService('names', ro.nullLogger(), 'hostA',
                                       peer_hosts=['hostB'])

    class Peer:
        def getPeers(self):
            return ['hostB', 'localhost', '127.0.0.1', 'hostC']

    monkeypatch.setattr(a, '_peer_client', lambda host: Peer())
    a.learn_peers_from('hostB')

    assert a.peers() == ['hostB', 'hostC']


def test_a_lone_name_server_still_serves_its_own_registrations():
    """Nothing about having no peers may change what a single host sees."""
    a = ns_mod.remoteObjectNameService('names', ro.nullLogger(), 'hostA',
                                       peer_hosts=['localhost'])
    a.register_self()
    a.register('svc', 'hostA', 8000, dict(protocol='g2rpc-tcp'))

    assert sorted(a.getNames()) == ['names', 'svc']
    assert a.exchange_with_peers() == 0, 'nothing to exchange, and no error'


def test_an_alias_for_ourselves_costs_one_connection_not_one_a_round():
    """--peer may name this host by a form that is not the one it calls
    itself: its short name where it uses the FQDN, or its address.  That
    cannot be known without asking, but it only has to be asked once."""
    a = ns_mod.remoteObjectNameService('names', ro.nullLogger(), 'hostA.long',
                                       peer_hosts=['hostA'])
    dialled = []

    class Mirror:
        def getNodeId(self):
            return a.node_id

    def client(**kwargs):
        dialled.append(kwargs.get('host'))
        return Mirror()

    import unittest.mock as mock
    with mock.patch.object(ro, 'remoteObjectClient', client):
        for _ in range(4):
            a.announce_to_seeds()

    assert dialled == ['hostA'], 'redialled itself every round: %r' % (dialled,)
    assert a.peers() == []
    assert 'hostA' not in a.getPeers()
