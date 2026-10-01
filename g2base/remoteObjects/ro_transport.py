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

import socket
import ssl
from typing import NamedTuple, Optional, Tuple

from tinyrpc.layers import COMPRESSORS
from tinyrpc.protocols.jsonrpc import JSONRPCProtocol
from tinyrpc.protocols.msgpackrpc import MSGPACKRPCProtocol
from tinyrpc.protocols.xmlrpc import XMLRPCProtocol
from tinyrpc.server.executor import RPCServerExecutor
from tinyrpc.transports.http_client import HttpClientTransport
from tinyrpc.transports.http_server import HttpServerTransport
from tinyrpc.transports.tcp import (ConnectionlessTcpClientTransport,
                                    ConnectionlessTcpServerTransport,
                                    NonBlockingTcpClientTransport,
                                    TcpServerTransport)

from . import ro_asyncio
from . import ro_g2rpc


class UnknownTransport(KeyError):
    """No spec is registered under the requested name."""


class TransportSpec:
    #: What this carrier is called in a transport string.  Subclasses that
    #: are a carrier say; the base does not know.
    carrier = None

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
    :param legacy_transport: The name an un-upgraded *client* can use to
        speak this correctly, if there is one.

        Only a protocol that is wire-compatible with what such a client
        already speaks has one, which in practice means XML-RPC.  Reading an
        old *registration* is the other direction and a separate map
        (:py:data:`legacy_transport_names`): an old service registering
        'socket' meant the old socket module, and that name resolves to
        g2rpc-tcp because that is what now listens there -- but telling an
        old client 'socket' would send it to a module speaking a format
        g2rpc-tcp does not, which is worse than telling it a name it does
        not know.
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

    def make_protocol(self, encoding=None, framing=None, credentials=None):
        """Build a protocol instance for one end of one conversation.

        ``framing`` and ``credentials`` are only meaningful to a protocol
        that has an envelope of its own to put them in, which is exactly the
        set that signs, so they are passed only to those.
        """
        kwargs = {}
        if self.encoding_is_selectable:
            kwargs['encoding'] = self.check_encoding(encoding)
        if self.auth_mechanism == 'signature':
            if framing is not None:
                kwargs['framing'] = framing
            if credentials is not None:
                kwargs['credentials'] = credentials
        return self.protocol_factory(**kwargs)

    #: How a caller proves itself over this spec.
    #:
    #: ``'basic'``
    #:     HTTP Basic: the transport lifts the credentials off the request
    #:     and the service compares them against its ``authDict``.  The
    #:     password crosses the wire on every call, so it is only as private
    #:     as the connection -- which is what TLS is for, and why a bare
    #:     socket cannot offer this at all.
    #: ``'signature'``
    #:     The caller signs the message with a key derived from the same
    #:     ``authDict`` entry.  The secret does not travel, the signature
    #:     covers the message, and stale ones expire -- and because it lives
    #:     in the protocol's envelope rather than an HTTP header, it works
    #:     over any carrier, including the ones that could not authenticate
    #:     before.
    #: ``None``
    #:     No way to, so a service asking for authentication over it would
    #:     refuse every call.  Better to say so when the service is built
    #:     than to look like a network fault.
    auth_mechanism = 'basic'

    @property
    def carries_credentials(self):
        """Whether a caller can prove itself at all over this spec."""
        return self.auth_mechanism is not None

    #: What must be importable for this spec to work, or ``None``.
    requires_module = None

    @classmethod
    def available(cls):
        """Whether this end can actually speak it.

        A spec can be registered and still be unusable here: 0mq needs
        pyzmq, which is not everywhere.  Being able to ask matters once a
        client chooses among the protocols a service offers -- picking one
        it cannot load would turn a working call into an ImportError.
        """
        if cls.requires_module is None:
            return True
        try:
            __import__(cls.requires_module)
        except ImportError:
            return False
        return True

    #: Whether the carrier can be encrypted.
    supports_tls = True

    #: What a failed bind raises, so the port search knows to try the next
    #: one.  0mq raises its own error, which is not an OSError.
    bind_errors = (OSError,)

    #: Whether a client may keep several calls in flight on one connection.
    #: That needs a connection that persists, and a protocol whose replies
    #: carry a correlation id.
    supports_multiplexing = False

    #: Whether one client transport should be kept and reused for every call
    #: rather than built per call.
    #:
    #: The connectionless carriers do not want this: building one costs
    #: nothing, and holding nothing between calls is what lets a client and a
    #: service restart in any order.  0mq does want it, because it expects
    #: its peers to last and starts losing replies if they do not.
    reuse_client_transport = False

    #: Whether a reused client transport needs to be one *per calling
    #: thread* rather than one shared between them.
    #:
    #: A held TCP connection carries no way to tell replies apart, so a
    #: thread waiting on one takes whichever arrives first -- possibly
    #: another thread's.  Sorting that out is what a correlation id and
    #: :py:class:`~tinyrpc.client_multiplexing.MultiplexingRPCClient` are
    #: for; an ordinary client avoids the question by not sharing.  0mq
    #: keeps a socket per thread inside the transport already, so its one
    #: object is safe to share.
    client_transport_per_thread = False

    def make_server_transport(self, bindhost, port, **kwargs):
        raise NotImplementedError

    def make_client_transport(self, host, port, **kwargs):
        raise NotImplementedError

    #: Whether this carrier's serve loop occupies a worker from the
    #: service's thread pool for as long as the service runs.
    #:
    #: True for every carrier that submits its loop to the executor, which
    #: is what makes a listener cost a worker and what the pubsub's thread
    #: budget counts.  A carrier that runs its own event loop thread costs
    #: the pool nothing and says so.
    server_holds_pool_worker = True

    def make_rpc_server(self, rpc_transport, protocol, dispatcher, executor,
                        ev_quit=None, logger=None):
        """The server that drives this listener.

        A seam rather than a fixed class, because the shape of the server is
        a property of the carrier: most run a serve loop on a worker and give
        each connection a thread, while an asyncio carrier runs an event loop
        and hands the work to the pool.  Both take the same executor, so a
        service's handlers run where they always did.
        """
        return RPCServerExecutor(rpc_transport, protocol, dispatcher,
                                 executor, ev_quit=ev_quit)

    def server_port(self, transport):
        """The port a bound server transport actually listens on."""
        return transport.endpoint[1]

    def __repr__(self):
        return '<%s %s>' % (type(self).__name__, self.name)


class HttpTransportSpec(TransportSpec):
    #: What this carrier is called in a transport string, after the '/'.
    carrier = 'http'

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
        return HttpClientTransport(self.url(host, port, secure),
                                   auth=auth, timeout=timeout, verify=verify,
                                   content_type=self.content_type)


class TcpTransportSpec(TransportSpec):
    carrier = 'tcp'

    """A protocol carried over a bare TCP socket.

    Two shapes, chosen by ``persistent``:

    * **one connection per call** (the default).  The same bargain as HTTP,
      without the HTTP: a call dials, sends, reads its reply and hangs up.
      That costs a connection setup per call and saves ever having to notice
      that a held connection has died.
    * **one connection, many calls** (``persistent=True``).  Cheaper per
      call and the only shape that can multiplex, since several replies have
      to be told apart on one connection.  In exchange the connection can
      die, so the client dials again when it does.

    A bare socket has nowhere to put HTTP credentials, so whether a service
    can authenticate over one is now a question about the protocol rather
    than the carrier: FlexRPC signs inside its own envelope and works here,
    while anything relying on an HTTP header does not.
    """

    auth_mechanism = None
    supports_tls = False

    def __init__(self, *args, persistent=False, **kwargs):
        super().__init__(*args, **kwargs)
        self.persistent = persistent

    @property
    def supports_multiplexing(self):
        return self.persistent

    @property
    def reuse_client_transport(self):
        """Hold the connection when there is one to hold.

        Building this per call was strictly worse than the connectionless
        carrier: it paid for a connection *and* a reader thread, then threw
        both away -- so the transport named "persistent" was the slowest of
        the three.
        """
        return self.persistent

    @property
    def client_transport_per_thread(self):
        return self.persistent

    def make_server_transport(self, bindhost, port, logger=None,
                              ssl_context=None, poll_timeout=0.5, **kwargs):
        if ssl_context is not None:
            raise ValueError(
                "the '%s' transport cannot be encrypted; use an HTTP-carried "
                "protocol for a secure service" % (self.name,))
        server = (TcpServerTransport if self.persistent
                  else ConnectionlessTcpServerTransport)
        return server.create((bindhost or '', port), logger=logger,
                             poll_timeout=poll_timeout, **kwargs)

    def make_client_transport(self, host, port, auth=None, secure=False,
                              timeout=None, verify=True):
        if secure:
            raise ValueError(
                "the '%s' transport cannot be encrypted" % (self.name,))
        if self.persistent:
            return NonBlockingTcpClientTransport((host, port))
        return ConnectionlessTcpClientTransport((host, port), timeout=timeout)


class ZmqTransportSpec(TransportSpec):
    carrier = 'zmq'

    """A protocol carried over 0mq, request/reply.

    The server is a ROUTER and each call is a REQ socket, so 0mq queues a
    request until the connection is established rather than dropping it --
    the slow-joiner problem that afflicts PUB/SUB does not arise here.

    Like the TCP carrier it has nowhere to put HTTP credentials, and it costs
    a socket per call; what it buys is 0mq's queueing and its reach to peers
    that already speak it.
    """

    auth_mechanism = None
    supports_tls = False
    requires_module = 'zmq'

    # 0mq expects peers to last: a client that made a socket per call began
    # losing replies after a few hundred of them.  One transport is kept and
    # reused, and it keeps a socket per calling thread.
    reuse_client_transport = True

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
        """Where a client connects to."""
        return 'tcp://%s:%d' % (host or '127.0.0.1', port)

    def bind_url(self, bindhost, port):
        """Where a server listens, which is not the same question.

        An empty bindhost means every interface, as it does to
        :py:meth:`socket.socket.bind`.  0mq spells that ``*``, and spells
        nothing at all as an error -- so this used to fall back on the
        client's default of 127.0.0.1 and quietly bind loopback only.  A
        service that did that registered the address its clients should use
        and then answered on none of them, which looks like the service
        being down rather than like a bind that went somewhere else.
        """
        return 'tcp://%s:%d' % (bindhost or '*', port)

    def server_port(self, transport):
        return int(transport.endpoint.rsplit(':', 1)[1])

    def make_server_transport(self, bindhost, port, logger=None,
                              ssl_context=None, poll_timeout=0.5, **kwargs):
        if ssl_context is not None:
            raise ValueError(
                "the '%s' transport cannot be encrypted" % (self.name,))
        from tinyrpc.transports.zmq import ZmqServerTransport
        return ZmqServerTransport.create(self.context,
                                         self.bind_url(bindhost, port),
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


#: What the old ``transport`` field's values mean now, when *reading* a
#: registration written by an un-upgraded service.
#:
#: That field was named for the transport but held protocol names, which is
#: why 'xmlrpc' sat alongside 'socket'.  Its three possible values were the
#: three modules that existed, so the translation is complete.
#:
#: This is one-way.  Writing the field is
#: :py:attr:`TransportSpec.legacy_transport`, and the two disagree on
#: purpose: 'socket' resolves to g2rpc-tcp because that is what listens
#: there now, but g2rpc-tcp does not report 'socket', because an old client
#: told that would use a module speaking a different format.
legacy_transport_names = {
    'xmlrpc': 'xmlrpc',
    'socket': 'g2rpc-tcp',
    'zmqrpc': 'g2rpc-zmq',
}


def resolve_legacy_transport(transport):
    """Map an old ``transport`` value onto a protocol name."""
    return legacy_transport_names.get(transport, transport)


#: What a ``+layer`` may ask for, and where it belongs.
#:
#: The envelope layers are FlexRPC's, so they mean nothing to a protocol
#: without one.  'tls' is the carrier's, and is the only one that is not.
#:
#: A layer may take one value, ``+layer=value``, naming a *variant*.  Tuning
#: -- a compression level, a signature's maximum age -- stays in
#: configuration: it has to match at both ends, and nobody types it twice.
ENVELOPE_LAYERS = ('auth', 'compress', 'encrypt')
CARRIER_LAYERS = ('tls',)

#: How the envelope authenticates.  Not called 'sign': only one of these
#: signs.  A signature is computed over the message and proves the sender
#: holds the secret; credentials are carried and only claim it, which is the
#: whole difference between them and not something a name should blur.
AUTH_VARIANTS = {'hmac': 'signature', 'plain': 'credentials'}

#: Which layers take a value, and whether they must have one.  'auth' must:
#: the two variants differ in what they guarantee, so defaulting one of them
#: silently is exactly the wrong kindness.  'compress' and 'encrypt' take an
#: optional scheme -- there is one of each today, so naming it is how a
#: second one arrives without the strings written now becoming ambiguous.
LAYER_VALUES = {'auth': ('required', tuple(AUTH_VARIANTS)),
                'compress': ('optional', tuple(sorted(COMPRESSORS))),
                'encrypt': ('optional', ('secretbox',)),
                'tls': ('none', None)}

#: Layers the grammar accepts and nothing yet applies.  Parsed, checked and
#: then refused, so a string written today means what it will mean when the
#: layer is built rather than being silently ignored until then.
UNBUILT_LAYERS = {
    'encrypt': "tinyrpc's Encrypt exists but g2cam never builds one, and its "
               "key would need deriving the way signing keys are, which both "
               "ends must then agree on",
}


class Transport(NamedTuple):
    """What a transport string asked for, taken apart.

    A surface syntax for people -- command lines, configuration files -- and
    not a wire format.  A registration keeps its protocol, encoding and
    transport in separate fields, and anything that reads one should go on
    reading those; this is for turning what somebody typed into them.
    """

    #: The registry name, ready for :py:func:`get`.
    name: str
    #: The encoding asked for, or None to take the spec's own.
    encoding: Optional[str] = None
    #: 'signature' or 'credentials' for a protocol with an envelope, else None.
    envelope_auth: Optional[str] = None
    #: Whether the carrier was asked to encrypt.
    secure: bool = False
    #: Envelope layers other than authentication, as ``(name, value)`` pairs
    #: in the order given.  The framing applies its own order regardless;
    #: see tinyrpc.framing.
    layers: Tuple[Tuple[str, Optional[str]], ...] = ()

    @property
    def spec(self):
        """The spec this names."""
        return get(self.name, self.encoding)


def parse(spec_string):
    """Take apart a transport string.

        protocol[:encoding][+layer...][/carrier[+layer...]]

    A plain name is a registry name and is looked up as one, which is what it
    has always been -- so 'jsonrpc' and 'g2rpc' go on meaning what they mean.
    Everything else is additive:

    * ``/carrier`` names the carrier: 'g2rpc/tcp' is the spec registered as
      'g2rpc-tcp', and 'g2rpc/http' is plain 'g2rpc', whose carrier that is.
    * ``:encoding`` picks the encoding, for the one protocol whose encoding
      is a choice.
    * ``+auth=hmac`` or ``+auth=plain`` picks how the envelope
      authenticates; ``+compress`` and ``+encrypt`` name the other envelope
      layers, and ``+tls`` asks the carrier to encrypt.  A layer may carry
      one value naming a variant: ``+compress=deflate``.

    ``-`` is never structural: it is part of a name, as in 'xmlrpc-std' and
    'tcp-persistent'.

    :raises UnknownTransport: for a name, carrier, encoding or layer that
        does not exist, or a combination the spec cannot honour.
    :raises NotImplementedError: for a layer this grammar accepts and
        nothing yet applies -- see :py:data:`UNBUILT_LAYERS`.  Separate from
        the above so that a caller can tell a typo from a promise.
    """
    text = (spec_string or '').strip()
    if not text:
        raise UnknownTransport('empty transport string')

    left, slash, right = text.partition('/')
    if slash and not right.strip():
        raise UnknownTransport(
            "'%s' ends in '/' but names no carrier" % (spec_string,))

    protocol, encoding, layers = _take_apart(left, spec_string)
    carrier, _enc, carrier_layers = _take_apart(right, spec_string)
    if _enc is not None:
        raise UnknownTransport(
            "'%s' puts an encoding on the carrier; it belongs on the "
            "protocol, before the '/'" % (spec_string,))

    name = _registry_name(protocol, carrier, spec_string)
    spec = get(name)                    # raises if the encoding is impossible

    for layer, value in layers:
        if layer not in ENVELOPE_LAYERS:
            raise UnknownTransport(
                "'%s' is not a layer this protocol can carry; it offers %s "
                "(and '%s' on the carrier)"
                % (layer, ', '.join(ENVELOPE_LAYERS),
                   "', '".join(CARRIER_LAYERS)))
        _check_value(layer, value, spec_string)
    for layer, value in carrier_layers:
        if layer not in CARRIER_LAYERS:
            raise UnknownTransport(
                "'%s' belongs on the protocol, before the '/', not on the "
                "carrier" % (layer,))
        _check_value(layer, value, spec_string)

    if encoding is not None and not spec.encoding_is_selectable:
        raise UnknownTransport(
            "'%s' fixes its own encoding, so ':%s' means nothing; only %s "
            "has a choice" % (protocol, encoding,
                              ', '.join(n for n in sorted(registry)
                                        if registry[n].encoding_is_selectable)))
    if encoding is not None:
        spec.check_encoding(encoding)

    asked = [layer for layer, _value in layers]
    if asked and spec.auth_mechanism != 'signature':
        raise UnknownTransport(
            "'%s' has no envelope to put '%s' in; only the g2rpc protocols do"
            % (protocol, '+'.join(asked)))
    if len(set(asked)) != len(asked):
        raise UnknownTransport("'%s' names a layer twice" % (spec_string,))

    secure = any(layer == 'tls' for layer, _value in carrier_layers)
    if secure and not spec.supports_tls:
        raise UnknownTransport(
            "the '%s' carrier cannot encrypt; use an HTTP-carried protocol"
            % (carrier or spec.carrier,))

    envelope_auth = None
    rest = []
    for layer, value in layers:
        if layer == 'auth':
            # One value, so a service cannot ask to authenticate two ways at
            # once: the exclusion is in the grammar rather than in a check.
            envelope_auth = AUTH_VARIANTS[value]
        else:
            rest.append((layer, value))
    rest = tuple(rest)

    for layer, _value in rest:
        if layer in UNBUILT_LAYERS:
            # NotImplementedError rather than UnknownTransport: the caller
            # did not get it wrong, and a caller that wants to tell those
            # apart -- to offer a better message, or to fall back -- can.
            raise NotImplementedError(
                "'+%s' is understood but not yet wired up: %s"
                % (layer, UNBUILT_LAYERS[layer]))

    return Transport(name=name, encoding=encoding, envelope_auth=envelope_auth,
                     secure=secure, layers=rest)


def _check_value(layer, value, spec_string):
    """Whether this layer takes a value, and whether this one is allowed."""
    need, allowed = LAYER_VALUES[layer]
    if need == 'none' and value is not None:
        raise UnknownTransport(
            "'+%s' takes no value, so '=%s' means nothing" % (layer, value))
    if need == 'required' and value is None:
        raise UnknownTransport(
            "'+%s' needs a value: one of %s" % (layer, ', '.join(allowed)))
    if allowed is not None and value is not None and value not in allowed:
        raise UnknownTransport(
            "'%s' is not a kind of '+%s'; it offers %s"
            % (value, layer, ', '.join(allowed)))


def _take_apart(text, spec_string):
    """One side of the '/': a name, an optional encoding, and layers."""
    text = text.strip()
    if not text:
        return None, None, ()

    head, plus, tail = text.partition('+')
    if plus and not tail.strip():
        raise UnknownTransport(
            "'%s' has a '+' with no layer after it" % (spec_string,))
    parts = tuple(part.strip() for part in tail.split('+')) if tail else ()
    if any(not part for part in parts):
        raise UnknownTransport(
            "'%s' has an empty '+' section" % (spec_string,))
    layers = []
    for part in parts:
        layer, equals, value = part.partition('=')
        layer, value = layer.strip(), value.strip()
        if not layer:
            raise UnknownTransport(
                "'%s' has a '=' with no layer before it" % (spec_string,))
        if equals and not value:
            raise UnknownTransport(
                "'%s' gives '%s' a '=' with no value after it"
                % (spec_string, layer))
        layers.append((layer, value if equals else None))
    layers = tuple(layers)

    name, colon, encoding = head.partition(':')
    name, encoding = name.strip(), encoding.strip()
    if colon and not encoding:
        raise UnknownTransport(
            "'%s' has a ':' with no encoding after it" % (spec_string,))
    if not name:
        raise UnknownTransport("'%s' names no protocol" % (spec_string,))

    return name, (encoding if colon else None), layers


def _registry_name(protocol, carrier, spec_string):
    """The registry key for a protocol and the carrier it was asked for."""
    protocol = resolve_legacy_transport(protocol)
    if carrier is None:
        return protocol

    compound = '%s-%s' % (protocol, carrier)
    if compound in registry:
        return compound
    # 'g2rpc/http' and 'jsonrpc/http': the bare name *is* that carrier's.
    if protocol in registry and registry[protocol].carrier == carrier:
        return protocol

    offers = sorted({name for name in registry
                     if name == protocol or name.startswith(protocol + '-')})
    raise UnknownTransport(
        "there is no '%s' over '%s'%s"
        % (protocol, carrier,
           '; registered: ' + ', '.join(offers) if offers
           else "; no protocol named '%s'" % (protocol,)))


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

class _SigningSpec:
    """Mixed into the specs whose protocol is FlexRPC.

    Those authenticate inside their own envelope, so they can do it over any
    carrier -- including 0mq and bare TCP, which had no way to before.
    """

    auth_mechanism = 'signature'


class G2RPCHttpSpec(_SigningSpec, HttpTransportSpec):
    pass


class G2RPCTcpSpec(_SigningSpec, TcpTransportSpec):
    pass


class G2RPCZmqSpec(_SigningSpec, ZmqTransportSpec):
    pass


# Note that none of the standardised protocols declare selectable encodings:
# each is a protocol whose encoding its specification fixes.  g2rpc, whose
# envelope is ours and therefore can be packed several ways, does.

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

register(G2RPCHttpSpec(
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

register(G2RPCTcpSpec(
    'g2rpc-tcp',
    ro_g2rpc.G2RPCProtocol,
    content_type='application/octet-stream',
    encoding=ro_g2rpc.DEFAULT_ENCODING,
    encodings=ro_g2rpc.ENCODINGS,
    description="Gen2's own protocol straight over TCP, with no HTTP "
                "framing.  Cheaper per call than the HTTP carrier, and "
                "cannot carry credentials or be encrypted."))

register(G2RPCTcpSpec(
    'g2rpc-tcp-persistent',
    ro_g2rpc.G2RPCProtocol,
    content_type='application/octet-stream',
    encoding=ro_g2rpc.DEFAULT_ENCODING,
    encodings=ro_g2rpc.ENCODINGS,
    persistent=True,
    description="Gen2's own protocol over a TCP connection that is held "
                "open, so a client can keep several calls in flight at once. "
                "The connection can die, so the client dials again."))

class G2RPCTcpAsyncioSpec(G2RPCTcpSpec):
    """g2rpc over TCP, served from an event loop rather than a thread each.

    Only the server differs from the threaded TCP specs, which is what makes
    them comparable -- and means a caller needs to know nothing about it.
    The client side is whichever ``persistent`` selects, exactly as it is
    there: g2rpc-tcp-asyncio dials per call, g2rpc-tcp-asyncio-persistent
    holds its connection.

    Holding one is what the loop makes cheap.  A held connection costs the
    service a coroutine instead of a thread, so the two ways of spending a
    thread per connection -- one per call, or one per connection for as long
    as it lasts -- both go away: 200 connections opened and left silent cost
    this server no threads at all, where either threaded carrier spends 200.
    """

    #: The loop runs on a thread of its own, so it takes nothing from the
    #: pool -- which is the point of it.
    server_holds_pool_worker = False

    def make_server_transport(self, bindhost, port, logger=None,
                              ssl_context=None, poll_timeout=0.5, **kwargs):
        if ssl_context is not None:
            raise ValueError(
                "the '%s' transport cannot be encrypted" % (self.name,))
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((bindhost or '', port))
        # asyncio manages the accept queue, but the backlog is ours to set
        # and a bursty service wants more than the default.
        sock.listen(kwargs.get('backlog', 512))
        sock.setblocking(False)
        return ro_asyncio.PreboundAsyncioTcpServerTransport(
            sock, poll_timeout=poll_timeout, logger=logger)

    def make_rpc_server(self, rpc_transport, protocol, dispatcher, executor,
                        ev_quit=None, logger=None):
        return ro_asyncio.AsyncioServerRunner(rpc_transport, protocol,
                                              dispatcher, executor,
                                              ev_quit=ev_quit, logger=logger)


register(G2RPCTcpAsyncioSpec(
    'g2rpc-tcp-asyncio',
    ro_g2rpc.G2RPCProtocol,
    content_type='application/octet-stream',
    encoding=ro_g2rpc.DEFAULT_ENCODING,
    encodings=ro_g2rpc.ENCODINGS,
    description="Gen2's own protocol over TCP, served from one event loop "
                "with the handlers on a thread pool.  A connection costs a "
                "coroutine rather than a thread, which is what a burst of "
                "them costs less of."))

register(G2RPCTcpAsyncioSpec(
    'g2rpc-tcp-asyncio-persistent',
    ro_g2rpc.G2RPCProtocol,
    content_type='application/octet-stream',
    encoding=ro_g2rpc.DEFAULT_ENCODING,
    encodings=ro_g2rpc.ENCODINGS,
    persistent=True,
    description="Gen2's own protocol over a TCP connection that is held "
                "open, served from one event loop.  The combination the "
                "other three each miss half of: the client pays no "
                "connection setup per call, and the service pays no thread "
                "per connection -- so a service reached by many callers at "
                "once costs coroutines rather than threads."))

register(G2RPCZmqSpec(
    'g2rpc-zmq',
    ro_g2rpc.G2RPCProtocol,
    content_type='application/octet-stream',
    encoding=ro_g2rpc.DEFAULT_ENCODING,
    encodings=ro_g2rpc.ENCODINGS,
    description="Gen2's own protocol over 0mq request/reply.  Like the TCP "
                "carrier it cannot carry credentials or be encrypted."))
