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
# ro_transport, which lists what is available.
#
# g2rpc-tcp is msgpack over a bare socket: about 2.6x XML-RPC's call rate on
# a small call, and it costs the service no held threads.  A caller that has
# not been upgraded cannot speak it and cannot be told to -- a registration
# whose primary is g2rpc-tcp names a protocol such a caller does not know, so
# it refuses rather than mistaking it for something else.  That is the
# intended behaviour: a service still called by old clients should say so,
# with transport=['xmlrpc', 'g2rpc-tcp'], which keeps XML-RPC as the primary
# they read while everything else takes the faster way.
default_transport  = 'g2rpc-tcp'
#default_transport  = 'xmlrpc'
#default_transport  = 'jsonrpc'
#default_transport  = 'msgpackrpc'

# What a service says when it is still spoken to by callers of both ages.
#
# XML-RPC first, so it stays the primary an un-upgraded caller reads, with
# the current default beside it for everything else.  Named rather than
# written out at each such service, so that there is one place to look for
# which services are still carrying XML-RPC for somebody -- and one line to
# change when the last of those callers is gone.
compat_transports = ['xmlrpc', default_transport]

# Default encoding, or None for "whatever the protocol uses".
#
# For a standardised protocol the encoding is not a separate choice at all:
# XML-RPC is XML, JSON-RPC is JSON, msgpack-RPC is msgpack.  Setting it here
# only means something for a protocol built around an interchangeable packer,
# and is ignored otherwise.  It used to default to 'pickle', which no
# protocol here can produce -- and which nothing should accept over a socket,
# since unpickling runs whatever it is sent.
default_encoding  = None

# The name service's protocol, for both ends.  It cannot be looked up -- it
# is what lookups go through -- so a client dials it by fixed port and fixed
# protocol rather than reading a registration, and there is no negotiating
# it.  That makes it the last thing that should stop speaking what an
# un-upgraded caller understands, so it stays on XML-RPC while the default
# for everything else moves on.
#
# Serving a second protocol here would need a well-known port for it as
# well, since there is no registration for a caller to learn one from.
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

# How hard to make guessing a service's password, in PBKDF2 rounds, when
# turning it into a signing key.
#
# What the rounds buy is narrow: they make *offline* guessing expensive for
# somebody who has captured traffic and wants the key behind it.  They do
# nothing against forgery, replay, or a caller who already knows the
# password -- the signature itself does that, and costs 6us a call whatever
# this is set to.
#
# The cost is paid once per key per process: around 30ms at 200000 rounds,
# 1.5ms at 10000.  A service that runs for weeks never notices either; a
# program invoked over and over pays it every time it starts, once for each
# service it talks to.
#
# 10000 suits what these passwords presently are.  use_default_auth below
# makes an unconfigured service's password its own name, which is public by
# construction -- no number of rounds protects a password that is already
# known, so paying 30ms for it buys nothing.  Raise this where the passwords
# are genuinely secret and worth attacking; both ends must agree on it,
# since it decides the key rather than merely how long it took to make.
kdf_rounds = 10000

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
