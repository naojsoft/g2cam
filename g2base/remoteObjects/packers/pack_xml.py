#
# This is open-source software licensed under a BSD license.
# Please see the file LICENSE.txt for details.
#
from g2base import six

def dump_int(_, v, w):
    w("<value><int>%d</int></value>" % (v))

if six.PY2:
    import xmlrpclib as xmlrpc_client
    from xmlrpclib import Marshaller
    Marshaller.dispatch[long] = dump_int

else:
    import xmlrpc.client as xmlrpc_client
    from xmlrpc.client import Marshaller

# monkey-patch to allow python xml-rpc not to complain about large
# integers
Marshaller.dispatch[int] = dump_int

class Packer(object):

    def __init__(self):
        self.kind = 'xml'
        self.version = '1.0'

    def pack(self, data):
        # NOTE [1]: python xmlrpc.client module requires an outer structure
        # of a tuple
        data = (data,)
        # TODO: is there a more efficient encoding to byte stream?
        payload = xmlrpc_client.dumps(data).encode('utf-8')
        return payload

    def unpack(self, payload):
        data = xmlrpc_client.loads(payload)
        # See NOTE [1] above
        data = data[0]
        return data

    def __str__(self):
        return "%s/%s" % (self.kind, self.version)
