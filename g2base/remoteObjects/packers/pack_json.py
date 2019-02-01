#
# This is open-source software licensed under a BSD license.
# Please see the file LICENSE.txt for details.
#
import json

class Packer(object):

    def __init__(self):
        self.kind = 'json'
        self.version = '1.0'

    def pack(self, data):
        # TODO: is there a more efficient encoding to byte stream?
        payload = json.dumps(data).encode('utf-8')
        return payload

    def unpack(self, payload):
        data = json.loads(payload)
        return data

    def __str__(self):
        return "%s/%s" % (self.kind, self.version)
