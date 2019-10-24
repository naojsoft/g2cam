#
# ro_transport.py -- remote objects transports
#
# This is open-source software licensed under a BSD license.
# Please see the file LICENSE.txt for details.
#
"""
"""
# holds all the possible transports
transports = {}

def get_transport(name):
    return transports[name]


############################################################
# Collect the different transports we can use
############################################################

try:
    from .transports import ro_XMLRPC
    transports['xmlrpc'] = ro_XMLRPC

except ImportError as e:
    raise e

try:
    from .transports import ro_socket
    transports['socket'] = ro_socket

except ImportError as e:
    pass

try:
    from .transports import ro_redis
    transports['redis'] = ro_redis

except ImportError as e:
    pass
