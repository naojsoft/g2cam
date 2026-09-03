#
# ro_g2rpc.py -- Gen2's own RPC protocol, with an interchangeable packer
#
"""An RPC protocol whose encoding is a choice rather than a given.

XML-RPC, JSON-RPC and msgpack-RPC each fix their encoding: the specification
*is* the encoding, which is why naming one alongside them meant nothing.  The
old ``ro_socket`` and ``ro_ZMQRPC`` transports were the exception -- they
shared one hand-rolled envelope and swapped the codec beneath it -- and that
is the capability this restores, on top of tinyrpc rather than beside it.

The envelope is a plain mapping, so anything :py:mod:`ro_packer` can pack
will carry it: msgpack (compact and fast, the default), json (readable, and
reachable from other languages), or xml.  The same protocol therefore runs
over any transport, and choosing HTTP, TCP or 0mq is a separate decision from
choosing how the bytes are encoded.

Deliberate properties, since we own the format rather than inheriting one:

* every request carries a correlation id and every reply echoes it, so
  several calls can be in flight on one connection and
  :py:class:`~tinyrpc.client_multiplexing.MultiplexingRPCClient` works over
  it -- which is most of the reason to want TCP or 0mq at all;
* keyword arguments are carried natively, which XML-RPC cannot do;
* errors are a distinct message type rather than a value that might be
  mistaken for a result, and carry the same numeric codes used elsewhere in
  Gen2 (the XML-RPC fault code interoperability table, which JSON-RPC 2.0
  also adopted), so one table explains every protocol here; and
* the envelope is versioned, so it can be changed without guessing.

Note this is *not* a standard: only Gen2 speaks it.  It is for traffic
between Gen2 components, and the XML-RPC protocol remains what keeps
un-upgraded peers working.
"""

from tinyrpc.exc import (InvalidParamsError, InvalidReplyError,
                         InvalidRequestError, MethodNotFoundError, RPCError,
                         ServerError)
from tinyrpc.protocols import (RPCErrorResponse, RPCProtocol, RPCRequest,
                               RPCResponse, default_id_generator)

from g2base import Bunch

from . import ro_packer

#: Bumped when the envelope's shape changes incompatibly.  A peer that sees a
#: version it does not know refuses the message rather than guessing.
ENVELOPE_VERSION = 1

#: The default packing, when a caller expresses no preference.
DEFAULT_ENCODING = 'msgpack'

#: The encodings this protocol can be asked for.  ``pickle`` is deliberately
#: absent: unpickling runs whatever it is sent.
ENCODINGS = ('msgpack', 'json', 'xml')

# Message types.
REQUEST = 'request'
RESPONSE = 'response'
ERROR = 'error'

#: Error codes, from the XML-RPC fault code interoperability table that
#: JSON-RPC 2.0 later adopted.  Sharing it means one explanation covers every
#: protocol Gen2 speaks.
ERROR_PARSE = -32700
ERROR_INVALID_REQUEST = -32600
ERROR_METHOD_NOT_FOUND = -32601
ERROR_INVALID_PARAMS = -32602
ERROR_INTERNAL = -32603
ERROR_APPLICATION = -32500


class G2RPCError(RPCError):
    """An error reported by the far end of a g2rpc call."""

    def __init__(self, message, code=ERROR_APPLICATION, data=None):
        super().__init__(message)
        self.message = message
        self.code = code
        self.data = data


def _code_and_message(error):
    """Classify an exception for the wire.

    The distinction that matters to a caller is whether the service refused
    the request or the method itself failed, so it is kept: everything the
    RPC machinery raises gets its own code, and anything else is an
    application error carrying the exception's own text.
    """
    if isinstance(error, str):
        return ERROR_APPLICATION, error, None
    if isinstance(error, G2RPCError):
        return error.code, error.message, error.data
    if isinstance(error, MethodNotFoundError):
        return ERROR_METHOD_NOT_FOUND, 'Method not found', None
    if isinstance(error, InvalidParamsError):
        return ERROR_INVALID_PARAMS, 'Invalid parameters', None
    if isinstance(error, InvalidRequestError):
        return ERROR_INVALID_REQUEST, 'Invalid request', None
    if isinstance(error, ServerError):
        return ERROR_INTERNAL, 'Internal error', None
    # An exception out of the called method.  Name the type as well as the
    # message: without a traceback it is often the only clue as to what
    # went wrong on the far side.
    return (ERROR_APPLICATION,
            '%s: %s' % (type(error).__name__, error), None)


class G2RPCRequest(RPCRequest):
    """One call, on its way out or just arrived."""

    def __init__(self, encoding=DEFAULT_ENCODING):
        super().__init__()
        self.one_way = False
        self.encoding = encoding

    def _pack(self, envelope):
        return ro_packer.pack(envelope, Bunch.Bunch(ptype=self.encoding))

    def serialize(self):
        return self._pack({
            'v': ENVELOPE_VERSION,
            'type': REQUEST,
            'id': self.unique_id,
            'method': self.method,
            'args': list(self.args or ()),
            'kwargs': dict(self.kwargs or {}),
        })

    def respond(self, result):
        """Build the reply to this request, or ``None`` if none is wanted."""
        if self.one_way or self.unique_id is None:
            return None

        response = G2RPCResponse(encoding=self.encoding)
        response.unique_id = self.unique_id
        response.result = result
        return response

    def error_respond(self, error):
        """Build an error reply to this request."""
        if self.one_way or self.unique_id is None:
            return None

        response = G2RPCErrorResponse(encoding=self.encoding)
        response.unique_id = self.unique_id
        response.code, response.error, response.data = _code_and_message(error)
        return response


class G2RPCResponse(RPCResponse):
    """A successful reply."""

    def __init__(self, encoding=DEFAULT_ENCODING):
        super().__init__()
        self.result = None
        self.encoding = encoding

    def serialize(self):
        return ro_packer.pack({
            'v': ENVELOPE_VERSION,
            'type': RESPONSE,
            'id': self.unique_id,
            'result': self.result,
        }, Bunch.Bunch(ptype=self.encoding))


class G2RPCErrorResponse(RPCErrorResponse):
    """A reply reporting that the call failed."""

    def __init__(self, encoding=DEFAULT_ENCODING):
        super().__init__()
        self.error = None
        self.code = ERROR_APPLICATION
        self.data = None
        self.encoding = encoding

    def serialize(self):
        error = {'code': self.code, 'message': self.error}
        if self.data is not None:
            error['data'] = self.data
        return ro_packer.pack({
            'v': ENVELOPE_VERSION,
            'type': ERROR,
            'id': self.unique_id,
            'error': error,
        }, Bunch.Bunch(ptype=self.encoding))


class G2RPCProtocol(RPCProtocol):
    """Gen2's RPC protocol, packed however you ask.

    :param encoding: One of :py:data:`ENCODINGS`.  It decides only how the
        envelope is serialized; the envelope itself, and therefore what a
        peer must understand, is the same either way.
    """

    #: Replies carry the id of the request they answer, so calls can be
    #: multiplexed over one connection.
    supports_reply_correlation = True
    supports_out_of_order = True

    def __init__(self, encoding=DEFAULT_ENCODING, id_generator=None):
        if encoding not in ENCODINGS:
            raise ValueError(
                "unknown encoding '%s'; g2rpc offers %s"
                % (encoding, ', '.join(ENCODINGS)))
        # Fail now rather than on the first call if the packer is missing.
        ro_packer.get_packer(encoding)

        self.encoding = encoding
        self._id_generator = id_generator or default_id_generator()

    def _get_unique_id(self):
        return next(self._id_generator)

    def request_factory(self):
        return G2RPCRequest(encoding=self.encoding)

    def create_request(self, method, args=None, kwargs=None, one_way=False):
        """Build a request.

        Unlike XML-RPC, positional and keyword arguments may both be given.
        """
        request = self.request_factory()
        request.method = method
        request.args = list(args or ())
        request.kwargs = dict(kwargs or {})
        request.one_way = one_way
        if not one_way:
            request.unique_id = self._get_unique_id()
        return request

    def _unpack(self, data):
        try:
            envelope = ro_packer.unpack(data)
        except Exception as e:
            raise G2RPCError('could not unpack the message: %s' % (e,),
                             code=ERROR_PARSE) from None

        if not isinstance(envelope, dict):
            raise G2RPCError('message is not an envelope',
                             code=ERROR_INVALID_REQUEST)

        version = envelope.get('v')
        if version != ENVELOPE_VERSION:
            raise G2RPCError(
                'unsupported envelope version %r (this is version %d)'
                % (version, ENVELOPE_VERSION), code=ERROR_INVALID_REQUEST)

        return envelope

    def parse_request(self, data):
        """Reconstruct a request from the wire."""
        try:
            envelope = self._unpack(data)
        except G2RPCError as e:
            raise _as_request_error(e) from None

        if envelope.get('type') != REQUEST:
            raise _as_request_error(G2RPCError(
                "expected a request, got %r" % (envelope.get('type'),),
                code=ERROR_INVALID_REQUEST))

        method = envelope.get('method')
        if not isinstance(method, str):
            raise _as_request_error(G2RPCError(
                'request names no method', code=ERROR_INVALID_REQUEST))

        request = self.request_factory()
        request.method = method
        request.args = list(envelope.get('args') or ())
        request.kwargs = dict(envelope.get('kwargs') or {})
        request.unique_id = envelope.get('id')
        request.one_way = request.unique_id is None

        # Answer in the encoding we were addressed in.  The packed format
        # names its own packer, so both ends can read anything; imposing our
        # own on the reply would only make a caller decode something it did
        # not ask for.
        try:
            request.encoding = ro_packer.peek_packer(data)
        except Exception:
            pass

        return request

    def parse_reply(self, data):
        """Reconstruct a reply from the wire.

        The reply carries the id of the request it answers, which is what
        lets a client keep several calls in flight at once.
        """
        try:
            envelope = self._unpack(data)
        except G2RPCError as e:
            raise InvalidReplyError(str(e)) from None

        kind = envelope.get('type')

        if kind == RESPONSE:
            response = G2RPCResponse(encoding=self.encoding)
            response.unique_id = envelope.get('id')
            response.result = envelope.get('result')
            return response

        if kind == ERROR:
            error = envelope.get('error') or {}
            response = G2RPCErrorResponse(encoding=self.encoding)
            response.unique_id = envelope.get('id')
            response.error = error.get('message', 'unspecified error')
            response.code = error.get('code', ERROR_APPLICATION)
            response.data = error.get('data')
            return response

        raise InvalidReplyError("expected a reply, got %r" % (kind,))

    def raise_error(self, error):
        """Turn an error reply back into an exception for the caller."""
        exception = G2RPCError(error.error, code=getattr(
            error, 'code', ERROR_APPLICATION),
            data=getattr(error, 'data', None))
        if self.raises_errors:
            raise exception
        return exception


def _as_request_error(error):
    """Wrap a parse failure as the exception a server expects.

    A server catches :py:class:`~tinyrpc.exc.RPCError` around
    ``parse_request`` and calls ``error_respond()`` on it, so a failure
    there has to arrive as one of those rather than as a bare exception.
    """
    class _ParseFailure(InvalidRequestError):
        def error_respond(self):
            response = G2RPCErrorResponse()
            response.unique_id = None
            response.error = str(error)
            response.code = error.code
            return response

    return _ParseFailure(str(error))
