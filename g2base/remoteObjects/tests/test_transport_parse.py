#
# test_transport_parse.py -- taking a transport string apart
#
"""A transport string is what somebody types: a command line, a config file.

It is deliberately *not* a wire format.  A name service registration keeps
the protocol, encoding and transport in separate fields, and everything that
reads a registration goes on reading those -- so this grammar exists to turn
what a person wrote into them, and nothing here should ever be handed to a
peer as a single string.

The rule that makes it compatible is that a plain name is a registry name,
looked up exactly as it always was.  Everything else is additive.
"""

import pytest

from g2base.remoteObjects import ro_transport
from g2base.remoteObjects.ro_transport import UnknownTransport, parse


# --------------------------------------------------- a plain name is a name --

@pytest.mark.parametrize('name', sorted(ro_transport.registry))
def test_every_registered_name_parses_to_itself(name):
    """Which is what keeps every existing config working: the grammar adds
    spellings, it does not replace the ones in use."""
    parsed = parse(name)

    assert parsed.name == name
    assert parsed.encoding is None
    assert parsed.envelope_auth is None
    assert parsed.secure is False
    assert parsed.spec is ro_transport.registry[name]


def test_a_hyphen_is_part_of_a_name_not_a_separator():
    """'xmlrpc-std' and 'tcp-persistent' both use one, meaning different
    things, and neither is structural."""
    assert parse('xmlrpc-std').name == 'xmlrpc-std'
    assert parse('g2rpc/tcp-persistent').name == 'g2rpc-tcp-persistent'


def test_an_old_transport_value_still_resolves():
    """A registration written before any of this says 'socket' or 'xmlrpc',
    and resolve_legacy_transport is still what answers for it."""
    for legacy in ro_transport.legacy_transport_names:
        assert parse(legacy).name == ro_transport.legacy_transport_names[legacy]


# ------------------------------------------------------------- composition --

@pytest.mark.parametrize('text,name', [
    ('g2rpc/http', 'g2rpc'),                       # the bare name's own carrier
    ('g2rpc/tcp', 'g2rpc-tcp'),
    ('g2rpc/tcp-persistent', 'g2rpc-tcp-persistent'),
    ('g2rpc/tcp-asyncio', 'g2rpc-tcp-asyncio'),
    ('g2rpc/zmq', 'g2rpc-zmq'),
    ('jsonrpc/http', 'jsonrpc'),
])
def test_a_carrier_after_the_slash_names_the_spec(text, name):
    assert parse(text).name == name


def test_an_encoding_after_the_colon():
    parsed = parse('g2rpc:json/tcp')

    assert (parsed.name, parsed.encoding) == ('g2rpc-tcp', 'json')


@pytest.mark.parametrize('text,mechanism', [
    ('g2rpc+auth=hmac/tcp', 'signature'),
    ('g2rpc+auth=plain/tcp', 'credentials'),
])
def test_a_layer_chooses_how_the_envelope_authenticates(text, mechanism):
    assert parse(text).envelope_auth == mechanism


def test_the_two_kinds_of_authentication_cannot_both_be_asked_for():
    """Not by a check, but because one layer holds one value.  '+auth=hmac'
    and '+auth=plain' are the same slot, so naming both is naming a layer
    twice rather than a combination to be rejected on its meaning."""
    with pytest.raises(UnknownTransport, match='names a layer twice'):
        parse('g2rpc+auth=hmac+auth=plain/tcp')


def test_authentication_needs_its_kind_said_out_loud():
    """The two differ in what they guarantee -- one proves, one claims -- so
    defaulting to either would be the wrong kindness."""
    with pytest.raises(UnknownTransport, match='needs a value'):
        parse('g2rpc+auth/tcp')


def test_a_kind_of_authentication_that_does_not_exist():
    with pytest.raises(UnknownTransport, match='not a kind'):
        parse('g2rpc+auth=rot13/tcp')


@pytest.mark.parametrize('text', ['g2rpc+sign/tcp', 'g2rpc+creds/tcp'])
def test_the_layer_is_not_called_sign(text):
    """'sign=plain' would describe carried credentials as a kind of signing.
    They are not: a signature is computed over the message and proves the
    sender holds the secret, credentials are carried and only claim it.  That
    is the whole difference between them."""
    with pytest.raises(UnknownTransport, match='not a layer'):
        parse(text)


def test_a_layer_that_takes_no_value_is_not_given_one():
    with pytest.raises(UnknownTransport, match='takes no value'):
        parse('g2rpc/http+tls=yes')


def test_tls_belongs_to_the_carrier():
    parsed = parse('g2rpc/http+tls')

    assert parsed.secure is True
    assert parsed.envelope_auth is None


def test_everything_at_once():
    parsed = parse('g2rpc:msgpack+auth=plain/http+tls')

    assert parsed == ro_transport.Transport(
        name='g2rpc', encoding='msgpack', envelope_auth='credentials',
        secure=True, layers=())


# ------------------------------------------ what it refuses, and how loudly --

@pytest.mark.parametrize('text', ['', '   ', None])
def test_nothing_is_not_a_transport(text):
    with pytest.raises(UnknownTransport):
        parse(text)


def test_an_unknown_protocol_lists_the_known_ones():
    with pytest.raises(UnknownTransport, match='known protocols are'):
        parse('nonsense')


def test_a_carrier_a_protocol_does_not_have_lists_what_it_does():
    with pytest.raises(UnknownTransport, match='g2rpc-tcp'):
        parse('g2rpc/carrierpigeon')


def test_a_protocol_with_no_tcp_spec_says_so():
    """msgpackrpc is registered over HTTP only.  The grammar can ask for it
    over TCP, and the answer is that nobody registered one -- which is the
    honest answer and names what exists."""
    with pytest.raises(UnknownTransport, match='no .msgpackrpc. over .tcp.'):
        parse('msgpackrpc/tcp')


def test_an_encoding_on_a_protocol_that_has_no_choice():
    with pytest.raises(UnknownTransport, match='fixes its own encoding'):
        parse('jsonrpc:msgpack')


def test_an_encoding_the_protocol_cannot_do():
    with pytest.raises(UnknownTransport, match='cannot encode'):
        parse('g2rpc:pickle/tcp')


def test_a_layer_on_a_protocol_with_no_envelope():
    with pytest.raises(UnknownTransport, match='no envelope'):
        parse('jsonrpc+auth=plain')


def test_tls_on_a_carrier_that_cannot():
    with pytest.raises(UnknownTransport, match='cannot encrypt'):
        parse('g2rpc/tcp+tls')


def test_tls_on_the_wrong_side_of_the_slash():
    with pytest.raises(UnknownTransport, match='not a layer this protocol'):
        parse('g2rpc+tls/tcp')


def test_an_encoding_on_the_wrong_side_of_the_slash():
    with pytest.raises(UnknownTransport, match='belongs on the protocol'):
        parse('g2rpc/tcp:json')


@pytest.mark.parametrize('text', ['g2rpc/', 'g2rpc:/tcp', 'g2rpc++auth=hmac/tcp',
                                  'g2rpc+/tcp', 'g2rpc+=hmac/tcp',
                                  'g2rpc+auth=/tcp'])
def test_a_malformed_string_is_refused_rather_than_guessed_at(text):
    with pytest.raises(UnknownTransport):
        parse(text)


def test_each_layer_says_whether_it_takes_a_value():
    """'auth' must have one, because its two kinds differ in what they
    guarantee; the schemes are optional because there is one of each today
    and naming it is how a second arrives without changing what the strings
    written now mean; 'tls' takes none."""
    from g2base.remoteObjects.ro_transport import LAYER_VALUES

    assert LAYER_VALUES['auth'][0] == 'required'
    assert LAYER_VALUES['compress'][0] == 'optional'
    assert LAYER_VALUES['encrypt'][0] == 'optional'
    assert LAYER_VALUES['tls'][0] == 'none'


# ------------------------------------------- accepted, and not yet built --
#
# A string written today should mean what it will mean once the layer is
# built, rather than being quietly ignored until then.  So these parse, and
# their values are checked, and then they refuse -- as NotImplementedError,
# which a caller can tell apart from having got the string wrong.

@pytest.mark.parametrize('text', [
    'g2rpc+encrypt/tcp',
    'g2rpc+encrypt=secretbox/tcp',
    'g2rpc:json+auth=hmac+encrypt/tcp',
])
def test_an_accepted_layer_that_is_not_built_yet_says_so(text):
    with pytest.raises(NotImplementedError, match='not yet wired up'):
        parse(text)


@pytest.mark.parametrize('text,offered', [
    ('g2rpc+compress=gzip/tcp', 'deflate'),
    ('g2rpc+encrypt=rot13/tcp', 'secretbox'),
])
def test_a_wrong_scheme_is_a_typo_rather_than_a_promise(text, offered):
    """Checked before the layer is refused as unbuilt, so a misspelling is
    reported as one however far off the implementation is."""
    with pytest.raises(UnknownTransport, match=offered):
        parse(text)


def test_compression_is_not_spelled_deflate():
    """deflate is the scheme, not the thing being asked for: '+compress' says
    what you want and '=deflate' says how -- and there are now three hows."""
    with pytest.raises(UnknownTransport, match='not a layer'):
        parse('g2rpc+deflate/tcp')


def test_gzip_is_not_among_the_schemes():
    """It is the same deflate algorithm inside a larger header, with a
    checksum this envelope does not need -- strictly worse than 'deflate' for
    no gain, so offering it would only invite the question."""
    with pytest.raises(UnknownTransport, match='not a kind'):
        parse('g2rpc+compress=gzip/tcp')


def test_encrypt_is_not_spelled_enc():
    """':encoding' already means the serializer, and '+enc' beside ':msgpack'
    would be two different things a syllable apart."""
    with pytest.raises(UnknownTransport, match='not a layer'):
        parse('g2rpc+enc=secretbox/tcp')


# ------------------------------------------------ and it reaches the objects --
#
# The grammar is only useful if a server and a client actually honour what a
# string says, and if what it says beats the arguments beside it -- the
# argument is what everything gets unless something said otherwise, and the
# string is something saying otherwise.

from g2base.remoteObjects import remoteObjects as ro    # noqa: E402

HOST = '127.0.0.1'


class Echo:
    def echo(self, value):
        return value


@pytest.fixture
def served():
    started = []

    def _make(transport, **kwargs):
        server = ro.remoteObjectServer(
            svcname='parsesvc', name='parsesvc', obj=Echo(), host=HOST,
            logger=ro.nullLogger(), usethread=True, ns=False,
            transport=transport, method_list=['echo'], **kwargs)
        server.ro_start(wait=True, timeout=10)
        started.append(server)
        return server

    yield _make
    for server in started:
        try:
            server.ro_stop(wait=True, timeout=10)
        except Exception:
            pass


def test_a_server_honours_what_the_string_says(served):
    server = served('g2rpc:json+auth=plain/tcp')

    assert server.spec.name == 'g2rpc-tcp'
    assert server.encoding == 'json'
    assert server.envelope_auth == 'credentials'


def test_the_string_beats_the_argument_beside_it(served):
    """The precedence that had to be chosen one way or the other."""
    server = served('g2rpc:json/tcp', encoding='msgpack')

    assert server.encoding == 'json'


def test_the_argument_still_applies_when_the_string_is_silent(served):
    server = served('g2rpc/tcp', encoding='json')

    assert server.encoding == 'json'


def test_a_plain_name_behaves_exactly_as_before(served):
    """The regression that matters: every existing caller passes one of
    these, and none of them should notice the grammar exists."""
    server = served('g2rpc-tcp')

    assert server.spec.name == 'g2rpc-tcp'
    assert server.encoding == 'msgpack'
    assert server.envelope_auth == ro.envelope_auth


def test_several_listeners_stay_a_list(served):
    """One string describes one way in, so there is no syntax for 'and also'
    and compat_transports is still a list."""
    server = served(['xmlrpc', 'g2rpc:json/tcp'])

    assert server.spec.name == 'xmlrpc'
    assert [a['protocol'] for a in server.alternates] == ['g2rpc-tcp']
    assert server.encoding == 'xml', 'the primary encodes as its own spec says'


def test_listeners_that_ask_for_different_encodings_are_refused(served):
    """A service listens several ways over one object, so an encoding is a
    property of the service; two strings disagreeing is a mistake rather
    than something to reconcile quietly."""
    with pytest.raises(ro.remoteObjectError, match='different encoding'):
        served(['g2rpc:json/tcp', 'g2rpc:msgpack/http'])


def test_a_client_honours_what_the_string_says():
    handle = ro.remoteObjectClient(HOST, 9999, name='parsesvc',
                                   transport='g2rpc:json+auth=plain/tcp')

    assert handle.spec.name == 'g2rpc-tcp'
    assert handle.encoding == 'json'
    assert handle.transport == 'g2rpc-tcp', 'normalised, not as typed'
    assert handle.proxy._credentials is not None
    assert handle.proxy._framing is None, 'carrying credentials, not signing'


def test_a_proxy_normalises_its_pin():
    """self.transport is choose_endpoint's `pin`, compared against the
    protocol names in a registration, so it cannot be the string as typed."""
    proxy = ro.remoteObjectProxy('parsesvc', hostports=[(HOST, 9999)],
                                 transport='g2rpc:json+auth=plain/tcp')

    assert proxy.transport == 'g2rpc-tcp'
    assert proxy.encoding == 'json'
    assert proxy.envelope_auth == 'credentials'


def test_a_call_goes_through_with_a_compound_string_at_both_ends(served):
    server = served('g2rpc:msgpack+auth=plain/tcp')
    handle = ro.remoteObjectClient(HOST, server.port, name='parsesvc',
                                   transport='g2rpc:msgpack+auth=plain/tcp',
                                   timeout=10)

    assert handle.echo('hi') == 'hi'


def test_the_thread_budget_understands_a_compound_name():
    """PubSub charges a worker for every listener whose serve loop runs on the
    pool, and the asyncio carrier is the one that does not.  Looked up rather
    than parsed, a compound name raised, was swallowed by a bare except, and
    was charged for anyway.

    Exercised through _affordable_transports rather than through parse, since
    what broke was the lookup inside it: a pool with room for exactly one
    charged listener keeps both of these when the free one is recognised, and
    refuses when it is not.
    """
    from g2base.remoteObjects import PubSub

    # reserved = outlimit(4) + subscription loop + start task + one spare = 7,
    # so a pool of 8 can afford exactly one listener that costs a worker.
    pubsub = PubSub.PubSub('budget', ro.nullLogger(), numthreads=8)
    listeners = ['g2rpc/tcp-asyncio', 'g2rpc/tcp']

    assert pubsub._affordable_transports(listeners, True, True) == listeners

    # and the plain spelling has always been understood, so it agrees
    plain = ['g2rpc-tcp-asyncio', 'g2rpc-tcp']
    assert pubsub._affordable_transports(plain, True, True) == plain


def test_a_pubsub_listens_the_way_a_compound_string_asks():
    """--transport on ro_ps_svc is split on commas and handed to
    start_server, which passes it to a remoteObjectServer -- so the grammar
    reaches a pubsub without ro_ps_svc knowing about it."""
    from g2base.remoteObjects import PubSub

    pubsub = PubSub.PubSub('parse-ps', ro.nullLogger(), numthreads=20)
    pubsub.start(wait=True)
    try:
        pubsub.start_server(port=0, transport=['g2rpc:json/tcp'], wait=True,
                            usethread=True)
        assert pubsub.server.spec.name == 'g2rpc-tcp'
        assert pubsub.server.encoding == 'json'
    finally:
        try:
            pubsub.stop_server(wait=True)
        except Exception:
            pass
        pubsub.stop(wait=True)


def test_the_default_transport_setting_stays_a_plain_name():
    """Not enforced, but asserted: the other things a compound string would
    say have settings of their own, so a compound default would be a second
    place saying one thing."""
    from g2base.remoteObjects import ro_transport as rt

    assert rt.parse(ro.default_transport).name == ro.default_transport
    assert rt.parse(ro.default_transport).encoding is None
    assert rt.parse(ro.default_transport).envelope_auth is None


# ---------------------------------------------------------- compressing --
#
# '+compress' used to parse and then refuse.  Now it builds a layer, at both
# ends, which must be configured alike: the header records that a body is
# compressed and not how.

def test_compress_is_offered_by_scheme():
    from g2base.remoteObjects import ro_transport as rt

    assert rt.parse('g2rpc+compress/tcp').layers == (('compress', None),)
    assert (rt.parse('g2rpc+compress=lzma/tcp').layers
            == (('compress', 'lzma'),))
    for scheme in ('deflate', 'bzip2', 'lzma'):
        assert scheme in rt.LAYER_VALUES['compress'][1]


def test_a_compressing_service_builds_the_layer_under_its_signature(served):
    """Under, not over: what is verified has to be what arrived, so the
    framing compresses first and signs the result -- whatever order the
    string named them in."""
    server = served('g2rpc+compress/tcp')
    layers = server.server.protocol.framing.layers

    assert [type(layer).__name__ for layer in layers] == ['Deflate',
                                                          'Signature']


def test_the_scheme_the_string_names_is_the_one_built(served):
    server = served('g2rpc+compress=lzma/tcp')
    layers = server.server.protocol.framing.layers

    assert [type(layer).__name__ for layer in layers] == ['Lzma', 'Signature']


def test_a_client_builds_it_too():
    handle = ro.remoteObjectClient(HOST, 9999, name='parsesvc',
                                   transport='g2rpc+compress=bzip2/tcp')

    assert [type(layer).__name__ for layer in handle.proxy._framing.layers] \
        == ['Bzip2', 'Signature']


def test_a_large_body_goes_and_comes_back(served):
    server = served('g2rpc+compress/tcp')
    handle = ro.remoteObjectClient(HOST, server.port, name='parsesvc',
                                   transport='g2rpc+compress/tcp', timeout=15)
    big = 'MEASURE TARGET NGC1234 EXPTIME=30 ' * 2000

    assert handle.echo(big) == big


def test_a_small_body_still_goes_through_a_compressing_service(served):
    """It is declined rather than compressed, so the flag is clear and the
    far end passes it through."""
    server = served('g2rpc+compress/tcp')
    handle = ro.remoteObjectClient(HOST, server.port, name='parsesvc',
                                   transport='g2rpc+compress/tcp', timeout=10)

    assert handle.echo('hi') == 'hi'


def test_it_actually_shrinks_the_body(served):
    """Otherwise every test above would pass with a layer that did nothing."""
    import msgpack

    plain = served('g2rpc/tcp')
    squeezed = served('g2rpc+compress/tcp')
    body = msgpack.packb([0, 1, 'echo', ['NGC1234 EXPTIME=30 ' * 3000]])

    assert (len(squeezed.server.protocol.framing.wrap(body))
            < len(plain.server.protocol.framing.wrap(body)) / 10)


def test_a_service_that_asks_for_nothing_builds_no_layer(served):
    """What the majority get: an empty pipeline, which wrap() skips."""
    server = served('g2rpc/tcp')
    layers = server.server.protocol.framing.layers

    assert [type(layer).__name__ for layer in layers] == ['Signature']


def test_the_decompression_bound_comes_from_the_configuration(served):
    """A small message costing arbitrary memory is the one hazard here, and
    the limit is a property of the host rather than of the conversation."""
    server = served('g2rpc+compress/tcp')
    deflate = server.server.protocol.framing.layers[0]

    assert deflate.max_size == ro.max_decompressed
    assert deflate.level == ro.compress_level
    assert deflate.threshold == ro.compress_threshold
