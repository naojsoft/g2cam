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
    'g2rpc+compress/tcp',
    'g2rpc+compress=deflate/tcp',
    'g2rpc+encrypt/tcp',
    'g2rpc+encrypt=secretbox/tcp',
    'g2rpc:json+auth=hmac+compress/tcp',
])
def test_an_accepted_layer_that_is_not_built_yet_says_so(text):
    with pytest.raises(NotImplementedError, match='not yet wired up'):
        parse(text)


@pytest.mark.parametrize('text,offered', [
    ('g2rpc+compress=lzma/tcp', 'deflate'),
    ('g2rpc+encrypt=rot13/tcp', 'secretbox'),
])
def test_a_wrong_scheme_is_a_typo_rather_than_a_promise(text, offered):
    """Checked before the layer is refused as unbuilt, so a misspelling is
    reported as one however far off the implementation is."""
    with pytest.raises(UnknownTransport, match=offered):
        parse(text)


def test_compression_is_not_spelled_deflate():
    """deflate is the scheme, not the thing being asked for: '+compress' says
    what you want and '=deflate' says how, which leaves room for a second
    how."""
    with pytest.raises(UnknownTransport, match='not a layer'):
        parse('g2rpc+deflate/tcp')


def test_encrypt_is_not_spelled_enc():
    """':encoding' already means the serializer, and '+enc' beside ':msgpack'
    would be two different things a syllable apart."""
    with pytest.raises(UnknownTransport, match='not a layer'):
        parse('g2rpc+enc=secretbox/tcp')
