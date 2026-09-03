#
# This is open-source software licensed under a BSD license.
# Please see the file LICENSE.txt for details.
#
"""Pack an envelope as XML, using the XML-RPC value encoding.

Two notes on what this used to do:

* ``unpack()`` returned ``loads(payload)[0]``, which is the *params tuple*
  rather than the value inside it, so nothing that packed here could be
  unpacked again.  Nothing noticed because nothing used it -- the pubsub
  layer and the name service both specify msgpack.
* It reached into ``xmlrpc.client.Marshaller.dispatch`` to allow oversized
  integers, which changed XML-RPC for everything else in the process.  That
  is now a protocol option in tinyrpc, so the patch is gone and this borrows
  the option instead.
"""

import xmlrpc.client

from tinyrpc.protocols.xmlrpc import dumps as xmlrpc_dumps


class Packer:

    def __init__(self):
        self.kind = 'xml'
        self.version = '1.0'

    def pack(self, data):
        # NOTE [1]: the xmlrpc.client module requires an outer tuple.
        # allow_none carries None values, and allow_large_ints carries
        # integers outside the signed 32 bits the standard permits, both of
        # which an arbitrary RPC payload will contain sooner or later.
        payload = xmlrpc_dumps((data,), allow_none=True,
                               allow_large_ints=True)
        return payload.encode('utf-8')

    def unpack(self, payload):
        params, _methodname = xmlrpc.client.loads(payload)
        # See NOTE [1] above: unwrap the outer tuple.
        return params[0]

    def __str__(self):
        return "%s/%s" % (self.kind, self.version)
