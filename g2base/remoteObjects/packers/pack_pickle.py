#
# This is open-source software licensed under a BSD license.
# Please see the file LICENSE.txt for details.
#
import pickle

class Packer(object):

    def __init__(self):
        self.kind = 'pickle'
        self.version = '1.0'

    def pack(self, data):
        payload = pickle.dumps(data)
        return payload

    def unpack(self, payload):
        data = pickle.loads(payload)
        return data

    def __str__(self):
        return "%s/%s" % (self.kind, self.version)
