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
from tinyrpc.transports.tcp import (ConnectionlessTcpClientTransport,
                                    ConnectionlessTcpServerTransport)

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

    #: Whether the carrier can convey a caller's credentials.  HTTP has a
    #: header for it; a bare socket has nowhere to put them, so a service
    #: asking for authentication over one would refuse every call.  Better to
    #: say so when the service is built than to look like a network fault.
    carries_credentials = True

    #: Whether the carrier can be encrypted.
    supports_tls = True

    #: What a failed bind raises, so the port search knows to try the next
    #: one.  0mq raises its own error, which is not an OSError.
    bind_errors = (OSError,)

    def make_server_transport(self, bindhost, port, **kwargs):
        raise NotImplementedError

    def make_client_transport(self, host, port, **kwargs):
        raise NotImplementedError

    def server_port(self, transport):
        """The port a bound server transport actually listens on."""
        return transport.endpoint[1]

    def __repr__(self):
        return '<%s %s>' % (type(self).__name__, self.name)


class HttpTransportSpec(TransportSpec):
    """A protocol carried over HTTP, one connection per call.

    Nothing is held between calls, so there is no connection to go stale:
    a client and a service can be restarted in any order, which is the
    property the whole system is built on.
    """

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
        kwargs = {}
        if auth is not None:
            kwargs['auth'] = tuple(auth)
        if timeout is not None:
            kwargs['timeout'] = timeout
        if secure:
            kwargs['verify'] = verify
        return HttpPostClientTransport(self.url(host, port, secure), **kwargs)


class TcpTransportSpec(TransportSpec):
    """A protocol carried over a bare TCP socket, one connection per call.

    The same bargain as HTTP, without the HTTP: a call dials, sends, reads
    its reply and hangs up.  That costs a connection setup per call and saves
    having to notice when a held connection has died.

    There is nowhere in a bare socket to put credentials, so a service
    needing authentication wants the HTTP carrier -- or the credentials would
    have to go in the envelope, as the old ro_socket transport put them.
    """

    carries_credentials = False
    supports_tls = False

    def make_server_transport(self, bindhost, port, logger=None,
                              ssl_context=None, poll_timeout=0.5, **kwargs):
        if ssl_context is not None:
            raise ValueError(
                "the '%s' transport cannot be encrypted; use an HTTP-carried "
                "protocol for a secure service" % (self.name,))
        return ConnectionlessTcpServerTransport.create(
            (bindhost or '', port), logger=logger,
            poll_timeout=poll_timeout, **kwargs)

    def make_client_transport(self, host, port, auth=None, secure=False,
                              timeout=None, verify=True):
        if secure:
            raise ValueError(
                "the '%s' transport cannot be encrypted" % (self.name,))
        return ConnectionlessTcpClientTransport((host, port), timeout=timeout)


class ZmqTransportSpec(TransportSpec):
    """A protocol carried over 0mq, request/reply.

    The server is a ROUTER and each call is a REQ socket, so 0mq queues a
    request until the connection is established rather than dropping it --
    the slow-joiner problem that afflicts PUB/SUB does not arise here.

    Like the TCP carrier this has nowhere to put credentials, and it costs a
    socket per call; what it buys is 0mq's queueing and its reach to peers
    that already speak it.
    """

    carries_credentials = False
    supports_tls = False

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._context = None

    @property
    def context(self):
        """One 0mq context for the process, made when first needed."""
        import zmq
        if self._context is None:
            self._context = zmq.Context.instance()
        return self._context

    @property
    def bind_errors(self):
        import zmq
        return (OSError, zmq.ZMQError)

    def url(self, host, port):
        return 'tcp://%s:%d' % (host or '127.0.0.1', port)

    def server_port(self, transport):
        return int(transport.endpoint.rsplit(':', 1)[1])

    def make_server_transport(self, bindhost, port, logger=None,
                              ssl_context=None, poll_timeout=0.5, **kwargs):
        if ssl_context is not None:
            raise ValueError(
                "the '%s' transport cannot be encrypted" % (self.name,))
        from tinyrpc.transports.zmq import ZmqServerTransport
        return ZmqServerTransport.create(self.context,
                                         self.url(bindhost, port),
                                         poll_timeout=poll_timeout)

    def make_client_transport(self, host, port, auth=None, secure=False,
                              timeout=None, verify=True):
        if secure:
            raise ValueError(
                "the '%s' transport cannot be encrypted" % (self.name,))
        from tinyrpc.transports.zmq import ZmqClientTransport
        return ZmqClientTransport.create(self.context, self.url(host, port),
                                         timeout=timeout)


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

register(HttpTransportSpec(
    'xmlrpc',
    lambda: XMLRPCProtocol(allow_none=True, allow_large_ints=True),
    content_type='text/xml', encoding='xml',
    legacy_transport='xmlrpc',
    description="XML-RPC over HTTP, as Gen2 has always spoken it: <nil/> and "
                "oversized ints allowed.  Backward compatible."))

register(HttpTransportSpec(
    'xmlrpc-std',
    lambda: XMLRPCProtocol(allow_none=False),
    content_type='text/xml', encoding='xml',
    description="Standard XML-RPC over HTTP, without the two Gen2 "
                "extensions, for talking to non-Python implementations."))

register(HttpTransportSpec(
    'jsonrpc',
    JSONRPCProtocol,
    content_type='application/json', encoding='json',
    description="JSON-RPC 2.0 over HTTP.  Carries keyword arguments, which "
                "XML-RPC cannot."))

register(HttpTransportSpec(
    'msgpackrpc',
    MSGPACKRPCProtocol,
    content_type='application/msgpack', encoding='msgpack',
    description="msgpack-RPC over HTTP.  Compact and fast; carries keyword "
                "arguments."))

register(HttpTransportSpec(
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

register(TcpTransportSpec(
    'g2rpc-tcp',
    ro_g2rpc.G2RPCProtocol,
    content_type='application/octet-stream',
    encoding=ro_g2rpc.DEFAULT_ENCODING,
    encodings=ro_g2rpc.ENCODINGS,
    legacy_transport='socket',
    description="Gen2's own protocol straight over TCP, with no HTTP "
                "framing.  Cheaper per call than the HTTP carrier, and "
                "cannot carry credentials or be encrypted."))

register(ZmqTransportSpec(
    'g2rpc-zmq',
    ro_g2rpc.G2RPCProtocol,
    content_type='application/octet-stream',
    encoding=ro_g2rpc.DEFAULT_ENCODING,
    encodings=ro_g2rpc.ENCODINGS,
    legacy_transport='zmqrpc',
    description="Gen2's own protocol over 0mq request/reply.  Like the TCP "
                "carrier it cannot carry credentials or be encrypted."))
