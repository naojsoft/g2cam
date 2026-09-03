#
# test_ro_transport.py -- the transport registry
#
"""What the name service's ``transport`` and ``encoding`` fields resolve to.

The registry is the seam that replaced the old ``transports`` dict of
modules, so the things worth pinning are that existing registrations still
resolve, that the Gen2 XML-RPC variant and standard XML-RPC can both exist
at once, and that an unknown name fails with something a reader can act on.
"""

import subprocess
import sys
import textwrap

import pytest

from g2base.remoteObjects import ro_config, ro_transport


def test_the_shipped_transports_are_registered():
    assert ro_transport.names() == ['jsonrpc', 'msgpackrpc', 'xmlrpc',
                                    'xmlrpc-std']


def test_the_default_transport_resolves():
    """ro_config.default_transport must name something real, or every
    service that does not ask for a transport fails at construction."""
    spec = ro_transport.get(ro_config.default_transport)
    assert spec.name == 'xmlrpc'


def test_the_name_service_transport_resolves():
    ro_transport.get(ro_config.ns_transport)


def test_existing_registrations_still_resolve():
    """Services already registered say encoding='pickle', because that was
    the module-wide default_encoding and the XML-RPC path ignored it.  Those
    registrations have to keep working across the upgrade."""
    spec = ro_transport.get('xmlrpc', encoding='pickle')
    assert spec.name == 'xmlrpc'

    # ... as do ones that say nothing at all.
    assert ro_transport.get('xmlrpc', encoding=None).name == 'xmlrpc'


def test_unknown_transport_says_what_is_available():
    with pytest.raises(ro_transport.UnknownTransport) as excinfo:
        ro_transport.get('carrier-pigeon')
    assert 'carrier-pigeon' in str(excinfo.value)
    assert 'xmlrpc' in str(excinfo.value)


def test_a_fixed_encoding_is_not_a_constraint():
    """A standardised protocol's encoding is not a choice, so naming one is
    ignored rather than rejected.

    Treating it as a constraint was the bug: ro_config's default_encoding was
    'pickle', so merely switching default_transport to 'jsonrpc' -- which the
    registry advertises as available -- raised on every construction.
    """
    for name in ro_transport.names():
        spec = ro_transport.get(name)
        assert not spec.encoding_is_selectable
        for encoding in ('pickle', 'json', 'msgpack', 'xml', None):
            assert ro_transport.get(name, encoding=encoding) is spec


def test_a_selectable_encoding_is_checked():
    """When a protocol really can be packed several ways -- which g2rpc will
    be -- the field selects, and a value outside the set is an error."""
    seen = {}

    spec = ro_transport.TransportSpec(
        'probe', lambda encoding: seen.setdefault('encoding', encoding),
        content_type='application/octet-stream',
        encoding='msgpack', encodings=('msgpack', 'json'))

    assert spec.encoding_is_selectable
    assert spec.check_encoding(None) == 'msgpack', "defaults to its own"
    assert spec.check_encoding('json') == 'json'
    with pytest.raises(ro_transport.UnknownTransport) as excinfo:
        spec.check_encoding('xml')
    assert 'offers msgpack, json' in str(excinfo.value)

    spec.make_protocol('json')
    assert seen['encoding'] == 'json', "the choice reaches the protocol"


def test_legacy_transport_values_translate():
    """The old field was named for the transport but held protocol names,
    and could only ever hold one of three values."""
    assert ro_transport.resolve_legacy_transport('xmlrpc') == 'xmlrpc'
    assert ro_transport.resolve_legacy_transport('socket') == 'g2rpc-tcp'
    assert ro_transport.resolve_legacy_transport('zmqrpc') == 'g2rpc-zmq'
    # An unknown value passes through, to be reported by get().
    assert ro_transport.resolve_legacy_transport('jsonrpc') == 'jsonrpc'


def test_each_spec_records_what_it_puts_on_the_wire():
    """The name service should describe reality, not the module default."""
    assert ro_transport.get('xmlrpc').encoding == 'xml'
    assert ro_transport.get('jsonrpc').encoding == 'json'
    assert ro_transport.get('msgpackrpc').encoding == 'msgpack'


def test_the_backward_compatible_spec_declares_its_old_name():
    assert ro_transport.get('xmlrpc').legacy_transport == 'xmlrpc'
    assert ro_transport.get('jsonrpc').legacy_transport is None


def test_each_spec_builds_a_protocol_and_a_client_transport():
    for name in ro_transport.names():
        spec = ro_transport.get(name)
        assert spec.make_protocol() is not None
        transport = spec.make_client_transport('localhost', 8000,
                                               auth=('u', 'p'), timeout=1.0)
        assert transport.endpoint == 'http://localhost:8000/'


def test_secure_gives_an_https_url():
    spec = ro_transport.get('xmlrpc')
    assert spec.url('h', 9, secure=True) == 'https://h:9/'
    assert spec.url('h', 9, secure=False) == 'http://h:9/'


def test_a_secure_server_needs_a_certificate():
    with pytest.raises(ValueError) as excinfo:
        ro_transport.make_ssl_context(None)
    assert 'openssl' in str(excinfo.value)


def test_protocols_are_not_shared_between_callers():
    """Protocols hold per-conversation state -- the ids of requests waiting
    for replies -- so a client and a server must not be handed the same one.
    """
    spec = ro_transport.get('jsonrpc')
    assert spec.make_protocol() is not spec.make_protocol()


#: Run in a subprocess, so that nothing else in the test session can have
#: patched xmlrpc.client first and made the check vacuous.
_ISOLATION_PROBE = textwrap.dedent("""
    import xmlrpc.client
    from g2base.remoteObjects import ro_transport

    # The stdlib marshaller must be untouched for this to prove anything.
    try:
        xmlrpc.client.dumps((2 ** 40,))
        print('PATCHED')
        raise SystemExit(0)
    except OverflowError:
        pass

    big = ro_transport.get('xmlrpc').make_protocol()
    std = ro_transport.get('xmlrpc-std').make_protocol()

    big.create_request('f', (2 ** 40,), None).serialize()
    try:
        std.create_request('f', (2 ** 40,), None).serialize()
        print('STD_ACCEPTED')
        raise SystemExit(0)
    except OverflowError:
        pass

    # And the stdlib is still untouched afterwards.
    try:
        xmlrpc.client.dumps((2 ** 40,))
        print('LEAKED')
    except OverflowError:
        print('OK')
""")


def test_large_int_support_does_not_leak_into_the_process():
    """The Gen2 XML-RPC variant must not be a global monkeypatch.

    The deleted ro_XMLRPC got its oversized ints by assigning to
    ``xmlrpc.client.Marshaller.dispatch`` at import, which changed XML-RPC
    for everything else in the process and made it impossible to speak both
    the variant and the standard.  The replacement is a protocol option, so
    this checks that enabling it leaves the stdlib -- and the standard spec
    -- alone.

    Run in a subprocess: the compatibility harness applies that same patch to
    simulate an old client, so in the main test process the check would be
    vacuous.
    """
    result = subprocess.run([sys.executable, '-c', _ISOLATION_PROBE],
                            capture_output=True, text=True, timeout=60)
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == 'OK', (result.stdout, result.stderr)


#: Also run in a subprocess, for the same reason.
_IMPORT_PROBE = textwrap.dedent("""
    import xmlrpc.client
    from g2base.remoteObjects import remoteObjects as ro

    try:
        xmlrpc.client.dumps((2 ** 40,))
        print('PATCHED')
    except OverflowError:
        print('OK')
""")


def test_importing_remoteObjects_does_not_patch_the_stdlib():
    """Importing the package used to change XML-RPC for the whole process.

    remoteObjects imported ro_XMLRPC to fill its transports dict, and
    ro_XMLRPC assigned to xmlrpc.client.Marshaller.dispatch at import time.
    So merely importing remoteObjects -- which anything touching Gen2 does --
    silently changed how every other XML-RPC user in that process marshalled
    integers.  Both are gone; this makes sure neither comes back by another
    route.
    """
    result = subprocess.run([sys.executable, '-c', _IMPORT_PROBE],
                            capture_output=True, text=True, timeout=60)
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == 'OK', (result.stdout, result.stderr)
