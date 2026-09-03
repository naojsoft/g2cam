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


class FakePubSub:
    """Only what remoteObjectNameService touches."""

    def __init__(self):
        self.published = []

    def subscribe(self, channel):
        pass

    def add_callback(self, channel, fn):
        pass

    def publish(self, channel, env, pack_info):
        self.published.append(env)


@pytest.fixture
def nameservice():
    return ns_mod.remoteObjectNameService('names', FakePubSub(),
                                          ro.nullLogger(), HOST)


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
    """Older still: options was a bare bool meaning 'secure'."""
    nameservice.register('svc', HOST, 8000, True)

    rec = only_record(nameservice, 'svc')
    assert rec['secure'] is True
    assert rec['protocol'] == ro.default_transport


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

def test_registrations_shared_between_name_servers_carry_the_protocol():
    """Name servers converge by publishing their own registrations to each
    other, so the new field has to travel."""
    pubsub = FakePubSub()
    a = ns_mod.remoteObjectNameService('names', pubsub, ro.nullLogger(),
                                       'hostA')
    a.register('svc', 'hostA', 8000, dict(protocol='jsonrpc'))
    a.share_our_registrations()

    env = pubsub.published[-1]
    assert env['names'][0]['protocol'] == 'jsonrpc'

    # ... and arrives intact at a peer.
    b = ns_mod.remoteObjectNameService('names', FakePubSub(),
                                       ro.nullLogger(), 'hostB')
    b.update_offsite_registrations(None, 'names', env)

    rec = only_record(b, 'svc')
    assert rec['protocol'] == 'jsonrpc'
    assert rec['registrar'] == 'hostA'


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
        assert rec['protocol'] == 'xmlrpc'
        assert rec['encoding'] == 'xml'
        assert rec['transport'] == 'xmlrpc', "still there for old clients"
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
