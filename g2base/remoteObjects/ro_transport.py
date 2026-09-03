#
# ro_transport.py -- what the name service's transport/encoding fields mean
#
"""The name service already records, for every registration, which
``transport`` and ``encoding`` a service speaks.  Nothing used to act on
that: the XML-RPC path ignored ``encoding`` entirely, and ``transport``
selected one of three modules that each implemented framing, serialization
and threading of their own.

This module turns those two fields into a tinyrpc **(protocol, client
transport, server transport)** triple.  Adding a way to speak is now
declaring a :py:class:`TransportSpec` here rather than writing another
transport module, and a service chooses one by registering under its name.

The default, ``xmlrpc``, is what Gen2 has always spoken: XML-RPC over HTTP,
one connection per call, with ``<nil/>`` and oversized ints allowed.  It has
to stay wire-compatible with services and clients that will not be upgraded.
``xmlrpc-std`` is the same thing without the two extensions, for talking to
XML-RPC implementations that are not Python's.
"""

import ssl

from tinyrpc.protocols.jsonrpc import JSONRPCProtocol
from tinyrpc.protocols.msgpackrpc import MSGPACKRPCProtocol
from tinyrpc.protocols.xmlrpc import XMLRPCProtocol
from tinyrpc.transports.http import HttpPostClientTransport
from tinyrpc.transports.http_server import HttpServerTransport

from . import ro_g2rpc


class UnknownTransport(KeyError):
    """No spec is registered under the requested name."""


class TransportSpec:
    """One way of speaking RPC: a protocol carried over a transport.

    :param name: The protocol name, as it appears in a name-service
        registration's ``protocol`` field.
    :param protocol_factory: Called to make a protocol instance.  Protocols
        are cheap and hold per-conversation state (outstanding request ids),
        so a client and a server each make their own rather than sharing one.
        It is called with an ``encoding`` keyword only when this spec has
        selectable encodings.
    :param content_type: The HTTP ``Content-Type`` for replies.
    :param encoding: What this spec actually puts on the wire.  Recorded with
        the registration so that the name service describes reality.
    :param encodings: The encodings that may be *chosen*, or empty when the
        encoding is fixed.

        This is the distinction the old configuration got wrong.  For a
        standardised protocol the encoding is not a separate axis at all --
        XML-RPC is XML, JSON-RPC is JSON, msgpack-RPC is msgpack -- so
        naming one is at best redundant and at worst a contradiction to
        reject.  Only a protocol built around an interchangeable packer,
        such as ``g2rpc``, genuinely has the choice, and only those declare
        it here.
    :param legacy_transport: The value the old ``transport`` field used for
        this, if any, so that registrations from un-upgraded services still
        resolve and un-upgraded clients still recognise it.
    """

    def __init__(self, name, protocol_factory, content_type,
                 encoding, encodings=(), legacy_transport=None,
                 description=''):
        self.name = name
        self.protocol_factory = protocol_factory
        self.content_type = content_type
        self.encoding = encoding
        self.encodings = tuple(encodings)
        self.legacy_transport = legacy_transport
        self.description = description

    @property
    def encoding_is_selectable(self):
        return bool(self.encodings)

    def check_encoding(self, encoding):
        """Return the encoding to use, or raise if it cannot be honoured.

        An encoding is only meaningful when this spec has a choice to make;
        otherwise it is ignored, which is what lets a registration written
        before any of this existed -- saying ``pickle``, the old module-wide
        default -- go on resolving.
        """
        if not self.encoding_is_selectable:
            return self.encoding
        if encoding is None:
            return self.encoding
        if encoding not in self.encodings:
            raise UnknownTransport(
                "protocol '%s' cannot encode as '%s'; it offers %s"
                % (self.name, encoding, ', '.join(self.encodings)))
        return encoding

    def make_protocol(self, encoding=None):
        if not self.encoding_is_selectable:
            return self.protocol_factory()
        return self.protocol_factory(encoding=self.check_encoding(encoding))

    def url(self, host, port, secure=False):
        return '%s://%s:%d/' % ('https' if secure else 'http', host, port)

    def make_server_transport(self, bindhost, port, logger=None,
                              ssl_context=None, poll_timeout=0.5, **kwargs):
        """Bind a server transport, or raise OSError if the port is taken."""
        return HttpServerTransport((bindhost, port),
                                   content_type=self.content_type,
                                   logger=logger,
                                   ssl_context=ssl_context,
                                   poll_timeout=poll_timeout,
                                   **kwargs)

    def make_client_transport(self, host, port, auth=None, secure=False,
                              timeout=None, verify=True):
        """Build a client transport.

        A fresh connection per call, which is what makes the system tolerate
        services being restarted underneath their clients: there is no
        connection to go stale between calls.
        """
        kwargs = {}
        if auth is not None:
            kwargs['auth'] = tuple(auth)
        if timeout is not None:
            kwargs['timeout'] = timeout
        if secure:
            kwargs['verify'] = verify
        return HttpPostClientTransport(self.url(host, port, secure), **kwargs)

    def __repr__(self):
        return '<TransportSpec %s>' % (self.name,)


#: Every way of speaking RPC that a service may register under.
registry = {}


def register(spec, replace=False):
    """Add a :py:class:`TransportSpec` to the registry."""
    if spec.name in registry and not replace:
        raise ValueError("a transport named '%s' is already registered"
                         % (spec.name,))
    registry[spec.name] = spec
    return spec


#: What the old ``transport`` field's values mean now.
#:
#: That field was named for the transport but held protocol names, which is
#: why 'xmlrpc' sat alongside 'socket'.  Its three possible values were the
#: three modules that existed, so the translation is complete.
legacy_transport_names = {
    'xmlrpc': 'xmlrpc',
    'socket': 'g2rpc-tcp',
    'zmqrpc': 'g2rpc-zmq',
}


def resolve_legacy_transport(transport):
    """Map an old ``transport`` value onto a protocol name."""
    return legacy_transport_names.get(transport, transport)


def get(protocol, encoding=None):
    """Look up the spec for a name-service registration.

    :param protocol: The registration's ``protocol`` field, or its old
        ``transport`` field, which is translated.
    :param encoding: The registration's ``encoding`` field.  Honoured only
        when the protocol has a choice to make; see
        :py:meth:`TransportSpec.check_encoding`.
    :raises UnknownTransport: when nothing is registered under that name, or
        the encoding cannot be honoured.
    """
    name = resolve_legacy_transport(protocol)
    try:
        spec = registry[name]
    except KeyError:
        raise UnknownTransport(
            "no protocol named '%s'; known protocols are %s"
            % (protocol, ', '.join(sorted(registry)))) from None

    spec.check_encoding(encoding)
    return spec


def names():
    """The registered transport names, sorted."""
    return sorted(registry)


def make_ssl_context(cert_file):
    """Build a server SSL context from a combined key+certificate file.

    This is the ``server.pem`` that the remoteObjects documentation has
    always described how to generate, and which nothing has ever used: the
    ``secure`` flag was plumbed through registrations, constructors and
    command lines while ``get_serverClass()`` returned the plain server
    whatever it was set to.
    """
    if not cert_file:
        raise ValueError(
            "a certificate file is required to run a secure server; "
            "generate one with: openssl req -new -x509 -keyout server.pem "
            "-out server.pem -days 365 -nodes")
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(cert_file)
    return context


# ---------------------------------------------------------------------------
# The transports Gen2 ships with.
# ---------------------------------------------------------------------------

# Note that none of these declare selectable encodings: each is a
# standardised protocol whose encoding its specification fixes.  g2rpc, whose
# envelope is ours and therefore can be packed several ways, will.

register(TransportSpec(
    'xmlrpc',
    lambda: XMLRPCProtocol(allow_none=True, allow_large_ints=True),
    content_type='text/xml', encoding='xml',
    legacy_transport='xmlrpc',
    description="XML-RPC over HTTP, as Gen2 has always spoken it: <nil/> and "
                "oversized ints allowed.  Backward compatible."))

register(TransportSpec(
    'xmlrpc-std',
    lambda: XMLRPCProtocol(allow_none=False),
    content_type='text/xml', encoding='xml',
    description="Standard XML-RPC over HTTP, without the two Gen2 "
                "extensions, for talking to non-Python implementations."))

register(TransportSpec(
    'jsonrpc',
    JSONRPCProtocol,
    content_type='application/json', encoding='json',
    description="JSON-RPC 2.0 over HTTP.  Carries keyword arguments, which "
                "XML-RPC cannot."))

register(TransportSpec(
    'msgpackrpc',
    MSGPACKRPCProtocol,
    content_type='application/msgpack', encoding='msgpack',
    description="msgpack-RPC over HTTP.  Compact and fast; carries keyword "
                "arguments."))

register(TransportSpec(
    'g2rpc',
    ro_g2rpc.G2RPCProtocol,
    # The packed envelope names its own packer in its header, so the
    # Content-Type has nothing to add.
    content_type='application/octet-stream',
    encoding=ro_g2rpc.DEFAULT_ENCODING,
    encodings=ro_g2rpc.ENCODINGS,
    description="Gen2's own protocol over HTTP, packed as msgpack, json or "
                "xml.  The only one here whose encoding is a choice.  Not a "
                "standard: only Gen2 speaks it."))
