#
# ro_codec.py -- encoding/decoding support for remoteObjects system
#
from . import ro_config

encoding = ro_config.default_encoding.lower()

codecs = {}

# cjson and cPickle were the fast Python 2 alternatives; on Python 3
# the stdlib json is the same code, and pickle picks up _pickle itself.
import json
codecs['json'] = (json.dumps, json.loads)

import pickle
codecs['pickle'] = (pickle.dumps, pickle.loads)

def get_codecs():
    return list(codecs.keys())

def get_encoder(encoding):
    return codecs[encoding][0]

def get_decoder(encoding):
    return codecs[encoding][1]


#END
