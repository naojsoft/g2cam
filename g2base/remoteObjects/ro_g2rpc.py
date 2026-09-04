#
# ro_g2rpc.py -- how Gen2 configures tinyrpc's FlexRPC protocol
#
"""Gen2's own RPC protocol, which is now a configuration rather than an
implementation.

This module used to carry the whole protocol: an envelope of its own, a
packer chosen per message, error codes, request and response classes.  All of
that has moved into :py:mod:`tinyrpc.protocols.flexrpc`, where it belongs --
nothing in it was specific to Gen2, and keeping it here meant the framing,
the signing and the serialization each had to be invented twice.

What is left here is the part that *is* Gen2's: which encodings we offer
(including the XML packer, which tinyrpc has no reason to know about), what
we call things in the name service, and how ``authDict`` becomes signing
keys.

The one visible change is that the envelope is no longer nested.  It used to
be a JSON header naming a packer, wrapped around a dict carrying its own
version -- two envelopes, 51 bytes of header, and 2.7us to read.  Now there
is a single ten-byte binary header that carries the version, the encoding
and the security sections together.

The protocol name 'g2rpc' is unchanged: it appears in name-service
registrations, and those outlive any rearrangement of the code behind them.
"""

from tinyrpc import serializers
from tinyrpc.framing import FLAG_SIGNED, Framing
from tinyrpc.layers import Credentials, Signature, derive_key
from tinyrpc.protocols.flexrpc import (ERROR_APPLICATION,  # noqa: F401
                                       ERROR_INTERNAL, ERROR_INVALID_PARAMS,
                                       ERROR_INVALID_REQUEST,
                                       ERROR_METHOD_NOT_FOUND, ERROR_PARSE,
                                       ERROR_REFUSED, FlexRPCError,
                                       FlexRPCProtocol, require_principal)

from .packers import pack_xml

#: Gen2's XML packing, offered to tinyrpc under an id in the range reserved
#: for local use.  It is here rather than in tinyrpc because tinyrpc has no
#: reason to know about it, and here rather than in ``ro_packer`` because the
#: framing header now does what that module's JSON header did.
SERIALIZER_XML = serializers.SERIALIZER_LOCAL

_xml_packer = pack_xml.Packer()
serializers.register('xml', SERIALIZER_XML,
                     _xml_packer.pack, _xml_packer.unpack)

#: The default packing, when a caller expresses no preference.
DEFAULT_ENCODING = 'msgpack'

#: The encodings this protocol can be asked for.  ``pickle`` is deliberately
#: absent: unpickling runs whatever it is sent.
ENCODINGS = ('msgpack', 'json', 'xml')

#: An error reported by the far end of a g2rpc call.  The same class the
#: protocol itself raises, under the name this module has always used.
G2RPCError = FlexRPCError


class G2RPCProtocol(FlexRPCProtocol):
    """FlexRPC, named and defaulted the way Gen2 speaks it.

    :param encoding: One of :py:data:`ENCODINGS`.  ``encoding`` rather than
        ``serializer`` because that is the word the name service uses, and
        the registrations are what people read.
    :param framing: What protects the message; see
        :py:func:`signing_framing`.  The default protects nothing, which is
        what a trusted socket wants and what everything else does not.
    :param credentials: A username and password to attach to every call, for
        a service still checking an ``authDict`` the old way.
    """

    def __init__(self, encoding: str = DEFAULT_ENCODING,
                 framing: 'Framing' = None,
                 credentials: 'Credentials' = None,
                 id_generator=None) -> None:
        # tinyrpc has more encodings registered than Gen2 offers -- another
        # site's, or one registered for a single service -- so the check is
        # against what this protocol advertises, not what happens to exist.
        if encoding not in ENCODINGS:
            raise ValueError("unknown encoding '%s'; g2rpc offers %s"
                             % (encoding, ', '.join(ENCODINGS)))
        super().__init__(serializer=encoding, framing=framing,
                         credentials=credentials, id_generator=id_generator)

    @property
    def encoding(self) -> str:
        """What this protocol packs as, under the name Gen2 uses."""
        return self.serializer.name


# --------------------------------------------------------------- signing --

#: Deriving a key from a password is deliberately slow -- that is what makes
#: a weak one expensive to attack -- so it must happen once per key and not
#: once per call.  Two hundred thousand PBKDF2 rounds is about 60ms; doing
#: that per message would cost more than the call.
_derived: dict = {}


def key_for(password: str, service: str = '') -> bytes:
    """Turn a password from an ``authDict`` into a signing key.

    Salted with the service name where there is one, so the same password
    used for two services does not become the same key -- which would let
    either service's callers sign for the other.
    """
    salt = ('g2rpc:' + service).encode('utf-8')
    cached = _derived.get((password, salt))
    if cached is None:
        cached = derive_key(password, salt=salt)
        _derived[(password, salt)] = cached
    return cached


def signing_framing(authDict, service: str = '', sign_as: str = None,
                    require: bool = True) -> Framing:
    """Build the framing a service or its callers should sign with.

    Gen2 already has a table of who may call what: ``authDict`` maps a name
    to a shared secret.  This turns that same table into signing keys, so no
    new configuration is needed and nothing has to be distributed again.

    What changes is the mechanism, and it is worth being clear about which
    part improves.  The secret no longer travels: today every authenticated
    call puts the password on the wire, where anything between the two ends
    can read it and replay it forever.  A signature proves the caller holds
    the secret without sending it, and covers the message, so it cannot be
    edited on the way either.

    What does *not* change is what the secret means.  Gen2's convention is
    one secret per service, known to everyone allowed to call it, so a
    verified signature says "this caller knows what callers of this service
    are told" and not "this caller is Alice".  Naming an individual caller
    needs a key per caller, which is a configuration change rather than a
    code one; :py:class:`~tinyrpc.layers.Signature` is ready for it.

    :param authDict: ``{name: password}``, as the server already holds.
    :param service: The service being called.  It salts the keys and binds
        the signature, so a call meant for one service will not verify at
        another even between peers that talk to both.
    :param sign_as: Which name to sign as; the only one, when there is only
        one.
    :param require: Whether to refuse an unsigned message.  A server should;
        a client talking to a server that may not yet be upgraded should
        not.
    """
    keys = {name: key_for(password, service)
            for name, password in authDict.items()}
    return Framing(layers=[Signature(keys, sign_as=sign_as,
                                     audience=service or None)],
                   require=FLAG_SIGNED if require else 0)


__all__ = ['G2RPCProtocol', 'G2RPCError', 'ENCODINGS', 'DEFAULT_ENCODING',
           'SERIALIZER_XML', 'key_for', 'signing_framing',
           'require_principal', 'ERROR_APPLICATION', 'ERROR_INTERNAL',
           'ERROR_INVALID_PARAMS', 'ERROR_INVALID_REQUEST',
           'ERROR_METHOD_NOT_FOUND', 'ERROR_PARSE', 'ERROR_REFUSED']
