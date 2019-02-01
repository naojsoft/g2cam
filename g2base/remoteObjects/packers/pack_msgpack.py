#
# This is open-source software licensed under a BSD license.
# Please see the file LICENSE.txt for details.
#

import msgpack

extcode_bigint = 42

def encode_special(obj):
    # msgpack does not handle ints bigger than 64 bits
    # we need 1 bit for the sign
    if isinstance(obj, int) and obj.bit_length() > 63:
        return msgpack.ExtType(extcode_bigint, str(obj).encode())
    return obj

def decode_special(code, data):
    if code == extcode_bigint:
        return int(data)
    return msgpack.ExtType(code, data)


class Packer(object):

    def __init__(self):
        self.kind = 'msgpack'
        self.version = '1.0'

    def pack(self, data):
        payload = msgpack.packb(data,  default=encode_special,
                                use_bin_type=True)
        return payload

    def unpack(self, payload):
        data = msgpack.loads(payload,  ext_hook=decode_special,
                             raw=False)
        return data

    def __str__(self):
        return "%s/%s" % (self.kind, self.version)
