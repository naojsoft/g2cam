#
# This is open-source software licensed under a BSD license.
# Please see the file LICENSE.txt for details.
#

from cryptography.fernet import Fernet

class Secure(object):

    @classmethod
    def generate_key(cls, seed):
        return Fernet.generate_key()

    def __init__(self, key=None):
        self.kind = 'fernet'
        self.version = '1.0'

        if key is None:
            key = self.generate_key(None)
        self.set_key(key)

    def set_key(self, key):
        self.key = key
        self.cipher = Fernet(key)

    def read_key(self, path):
        with open(path, 'rb') as in_f:
            key = in_f.read()
            self.set_key(key)

    def write_key(self, path):
        with open(path, 'wb') as out_f:
            out_f.write(self.key)

    def encrypt(self, buf_b):
        return self.cipher.encrypt(buf_b)

    def decrypt(self, buf_b):
        return self.cipher.decrypt(buf_b)

    def __str__(self):
        return "%s/%s" % (self.kind, self.version)
