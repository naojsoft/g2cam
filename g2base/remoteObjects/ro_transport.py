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


class UnknownTransport(KeyError):
    """No spec is registered under the requested name."""


class TransportSpec:
    """One way of speaking RPC: a protocol carried over a transport.

    :param name: The value that appears in a name-service registration's
        ``transport`` field.
    :param protocol_factory: Called with no arguments to make a protocol
        instance.  Protocols are cheap and hold per-conversation state
        (outstanding request ids), so a client and a server each make their
        own rather than sharing one.
    :param content_type: The HTTP ``Content-Type`` for replies.
    :param encodings: The ``encoding`` values this spec answers to, for the
        name service's benefit.  XML-RPC ignores the field -- its encoding is
        part of the protocol -- so registrations made before this existed,
        which say ``pickle`` because that was the module default, still
        resolve.
    """

    def __init__(self, name, protocol_factory, content_type,
                 encodings=(), description=''):
        self.name = name
        self.protocol_factory = protocol_factory
        self.content_type = content_type
        self.encodings = tuple(encodings)
        self.description = description

    def make_protocol(self):
        return self.protocol_factory()

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


def get(transport, encoding=None):
    """Look up the spec for a name-service registration.

    :param transport: The registration's ``transport`` field.
    :param encoding: The registration's ``encoding`` field.  Advisory: it is
        checked against the spec when the spec declares any encodings, and
        ignored otherwise, so that older registrations still resolve.
    :raises UnknownTransport: when nothing is registered under that name.
    """
    try:
        spec = registry[transport]
    except KeyError:
        raise UnknownTransport(
            "no transport named '%s'; known transports are %s"
            % (transport, ', '.join(sorted(registry)))) from None

    if encoding and spec.encodings and encoding not in spec.encodings:
        raise UnknownTransport(
            "transport '%s' does not speak encoding '%s' (only %s)"
            % (transport, encoding, ', '.join(spec.encodings)))

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

register(TransportSpec(
    'xmlrpc',
    lambda: XMLRPCProtocol(allow_none=True, allow_large_ints=True),
    content_type='text/xml',
    # XML-RPC carries its own encoding, so the field means nothing here.
    # Registrations predating this module say 'pickle' -- the old module-wide
    # default_encoding -- and must keep resolving, so nothing is declared.
    encodings=(),
    description="XML-RPC over HTTP, as Gen2 has always spoken it: <nil/> and "
                "oversized ints allowed.  Backward compatible."))

register(TransportSpec(
    'xmlrpc-std',
    lambda: XMLRPCProtocol(allow_none=False),
    content_type='text/xml',
    encodings=(),
    description="Standard XML-RPC over HTTP, without the two Gen2 "
                "extensions, for talking to non-Python implementations."))

register(TransportSpec(
    'jsonrpc',
    JSONRPCProtocol,
    content_type='application/json',
    encodings=('json',),
    description="JSON-RPC 2.0 over HTTP.  Carries keyword arguments, which "
                "XML-RPC cannot."))

register(TransportSpec(
    'msgpackrpc',
    MSGPACKRPCProtocol,
    content_type='application/msgpack',
    encodings=('msgpack',),
    description="msgpack-RPC over HTTP.  Compact and fast; carries keyword "
                "arguments."))
