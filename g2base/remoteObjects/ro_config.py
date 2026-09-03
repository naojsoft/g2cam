#
# Configuration file for remoteObjects
#

# Port for manager service to run on
managerServicePort = 7070

# Port for name service to run on
nameServicePort    = 7075

# Beginning of range of ports for remote object services
objectsBasePort    = 8000

# Default protocol.  Named "transport" for the constructor keyword and the
# name service field it fills, both of which predate the distinction; see
# ro_transport, which lists what is available.  'xmlrpc' is what un-upgraded
# clients and services speak, so leave it alone unless everything talking to
# the service in question has been upgraded.
default_transport  = 'xmlrpc'
#default_transport  = 'jsonrpc'
#default_transport  = 'msgpackrpc'

# Default encoding, or None for "whatever the protocol uses".
#
# For a standardised protocol the encoding is not a separate choice at all:
# XML-RPC is XML, JSON-RPC is JSON, msgpack-RPC is msgpack.  Setting it here
# only means something for a protocol built around an interchangeable packer,
# and is ignored otherwise.  It used to default to 'pickle', which no
# protocol here can produce -- and which nothing should accept over a socket,
# since unpickling runs whatever it is sent.
default_encoding  = None

# Name service protocol.  The name service is what clients use to find
# everything else, so it is the last thing that should stop speaking a
# protocol an un-upgraded client understands.
ns_transport  = 'xmlrpc'
ns_encoding  = None

# Do you want to default to SSL connections (slower)
# [only for transport=xmlrpc]
# NOTE: currently this should be set to False!
default_secure     = False

# Path of default cert file to use for encrypted servers
# [only for transport=xmlrpc]
# NOTE: careful, if you set this then anyone with access to this file
# will run a server with that cert
default_cert       = None

# If set to True, and no explicit authentication is supplied
# servers and clients will resort to using the service name.
# A good idea to leave True, to prevent accidental masquerading.
use_default_auth   = True

# Timeout value for certain known types of remoteObject calls (e.g. nameSvc)
timeout = 10.0

# Default seconds between pings to the remoteObjectsNameSvc
default_ns_ping_interval = 10.0

# Should remoteObject servers be multithreaded by default?
default_threaded_server = True

# Default number of threads to use for the server
default_num_threads = 5

# Oversized integers (outside the signed 32 bits the XML-RPC standard
# allows) are now a property of the protocol rather than a global setting:
# the 'xmlrpc' spec enables them and 'xmlrpc-std' does not.  See
# ro_transport.  The former allow_long flag is gone; nothing read it.


# ERROR CODES
OK             = 0
ERROR          = 1
ERROR_FATAL    = 2
ERROR_FAILOVER = 3

#END
