#
# ro_codec.py -- encoding/decoding support for remoteObjects system
#
# Eric Jeschke (eric@naoj.org)
#
from . import ro_config

encoding = ro_config.default_encoding.lower()

codecs = {}

try:
    # faster
    import cjson
    codecs['json'] = (cjson.encode, cjson.decode)
except ImportError:
    # slower
    import json
    codecs['json'] = (json.dumps, json.loads)

try:
    # faster
    import cPickle
    codecs['pickle'] = (cPickle.dumps, cPickle.loads)
except ImportError:
    # slower
    import pickle
    codecs['pickle'] = (pickle.dumps, pickle.loads)

def get_codecs():
    return codecs.keys()

def get_encoder(encoding):
    return codecs[encoding][0]

def get_decoder(encoding):
    return codecs[encoding][1]


#END

