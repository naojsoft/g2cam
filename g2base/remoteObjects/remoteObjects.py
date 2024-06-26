#! /usr/bin/env python
#
# remoteObjects.py -- remote object server module.
#
# TODO:
# [ ] ? allow authentication on a method-by-method basis
#
"""
In order to use encrypted remoteObject servers, follow these steps:

1) Install the OpenSSL package in order to generate key and
certificate. Note: you probably already have this package installed if
you are under Linux, or *BSD.

2) Install the python-openssl package, which wraps the OpenSSL
library for use by python.

3) Generate a self-signed certificate compounded of a certificate and
a private key for your server with the following command:

$ openssl req -new -x509 -keyout server.pem -out server.pem -days 365 -nodes

This will output them both in the same file named server.pem
"""

import sys, os, time
import socket
import threading
from g2base import six
if six.PY2:
    import Queue
else:
    import queue as Queue
# binascii encoding/decoding is much faster than xmlrpclib's
# built-in Binary class
import binascii
import zlib
import warnings
import traceback
import inspect
import signal

from g2base import Bunch, Task, ssdlog


from .ro_config import *

# Collect the different transports we can use
transports = {}
try:
    from . import ro_XMLRPC
    transports['xmlrpc'] = ro_XMLRPC
except ImportError as e:
    raise e
    pass
try:
    from . import ro_socket
    transports['socket'] = ro_socket
except ImportError:
    pass
try:
    from . import ro_ZMQRPC
    transports['zmqrpc'] = ro_ZMQRPC
except ImportError:
    pass

version = '20130801.0'

# Format for log messages
STD_FORMAT = '%(asctime)s | %(levelname)1.1s | %(filename)s:%(lineno)d (%(funcName)s) | %(message)s'

# The default manager server
default_ms = None

# The default name server
default_ns = None

# For generating errors from this module
#
class remoteObjectError(Exception):
    pass

class NameServiceWarning(RuntimeWarning):
    pass

class ManagerServiceWarning(RuntimeWarning):
    pass

#------------------------------------------------------------------
# Remote object server implementation
#

class remoteObjectServer(object):

    '''This module implements the interface for the remote calling of
    object methods.  i.e. it implements the "server" side.

    Usual use is to subclass this and define your own remotely-callable
    methods.
    '''

    def __init__(self, svcname=None, obj=None, logger=None, ev_quit=None,
                 name='', host=None, port=None, usethread=True,
                 timeout=0.1, ping_interval=default_ns_ping_interval,
                 strict_registration=False, numthreads=default_num_threads,
                 threaded_server=default_threaded_server,
                 threadPool=None, transport=default_transport,
                 encoding=default_encoding,
                 authDict=None, default_auth=use_default_auth,
                 secure=default_secure, cert_file=default_cert,
                 ns=None, method_list=None, method_prefix=None):

        self.svcname = svcname
        self.name = name
        # Event that gets set when the server starts running
        self.ev_start = threading.Event()
        # Event that gets set when the server stops running
        self.ev_stop = threading.Event()
        if not ev_quit:
            self.ev_quit = threading.Event()
        else:
            self.ev_quit = ev_quit

        if obj == None:
            self.obj = self
        else:
            self.obj = obj

        if method_list:
            # if an allowed method list was provided, use it
            methodNames = list(method_list)

        else:
            # otherwise, look up all the callables in the object we are
            # serving and if they are not private, reveal them
            methodNames = []
            for attrName in dir(self.obj):
                if callable(getattr(self.obj, attrName)):
                    # if user specified a method prefix, then only
                    # register methods that begin with that prefix
                    if (method_prefix != None):
                        if attrName.startswith(method_prefix):
                            methodNames.append(attrName)
                    elif not attrName.startswith('_'):
                        methodNames.append(attrName)

        self.method_list = methodNames
        self.method_list.sort()

        # Logger for logging debug/error messages
        if not logger:
            self.logger = nullLogger()
        else:
            self.logger = logger

        # Port we listen on for remote control requests
        if host:
            self.host = host
            self.bindhost = host
        else:
            self.host = socket.getfqdn()
            # Default is to bind to all interfaces
            self.bindhost = ''

        self.transport = transport
        self.encoding = encoding
        self.port = port

        if authDict:
            self.authDict = authDict
        elif default_auth and self.svcname:
            self.authDict = {svcname: svcname}
        else:
            self.authDict = None

        self.secure = secure
        self.nsopts = { 'secure': secure, 'transport': transport,
                        'encoding': encoding,
                        }
        self.cert_file = cert_file

        self.usethread = usethread
        self.threadPool = threadPool
        self.numthreads = numthreads
        self.timeout = timeout
        self.lastpingtime = 0.0
        self.pinginterval = ping_interval
        self.strict_registration = strict_registration
        self.threaded_server = threaded_server
        if ns is None:
            # if no specific name server supplied, use the module default
            ns = default_ns
        elif ns is False:
            ns = None
            self.pinginterval = 1000000000
        self.ns = ns
        self.__pid = os.getpid()

        ro_transport = transports[transport]
        serv_klass = ro_transport.get_serverClass(secure=secure)

        # If a specific port was requested, then try to start a server there,
        # otherwise try to start a server with find_free_port
        if self.port:
            port = self.port
        else:
            start_port = objectsBasePort
            port = find_free_port(self.bindhost,
                                  start_port, start_port+15000)
            self.port = port

        self.server = serv_klass(self.bindhost, self.port,
                                 ev_quit=self.ev_quit,
                                 timeout=self.timeout,
                                 logger=self.logger,
                                 authDict=self.authDict,
                                 cert_file=self.cert_file,
                                 threaded=self.threaded_server,
                                 threadPool=self.threadPool,
                                 numthreads=self.numthreads)


    def ro_start(self, wait=False, timeout=None):
        '''Start/enable remote object server.'''

        if self.usethread:
            if self.threadPool:
                task = Task.FuncTask2(self.__cmd_loop)
                # How to initialize() task?
                self.threadPool.addTask(task)

            else:
                self.mythread = threading.Thread(target=self.__cmd_loop,
                                             name=self.name)
                self.mythread.start()

            if wait:
                self.ev_start.wait(timeout=timeout)
        else:
            self.__cmd_loop()


    def ro_stop(self, wait=False, timeout=None):
        '''Stop/disable remote object server.'''
        self.server.stop()
        self.ev_quit.set()

        if wait:
            if self.usethread:
                # This seems to cause some hangs
                #self.mythread.join()
                self.ev_stop.wait(timeout=timeout)
            else:
                self.ro_wait_stop()


    def ro_wait_start(self, timeout=None):
        '''Wait for remote object server to start.'''
        if not self.ev_start.isSet():
            self.ev_start.wait(timeout=timeout)

        if not self.ev_start.isSet():
            raise remoteObjectError("Timed out waiting for server to start")


    def ro_wait_stop(self, timeout=None):
        '''Wait for remote object server to terminate.'''
        if not self.ev_stop.isSet():
            self.ev_stop.wait(timeout=timeout)

        if not self.ev_stop.isSet():
            raise remoteObjectError("Timed out waiting for server to terminate")


    def ro_list(self):
        """Introspection function that returns a list of allowed methods
        that can be called for this remote object.
        """
        return self.method_list


    def ro_workerStatus(self):
        res = []
        if self.threadPool:
            return self.threadPool.workerStatus()
        else:
            raise remoteObjectError("Sorry, this RO server was not created with a threadPool.")


    ## def ro_workerReset(self):
    ##     res = []
    ##     if self.threadPool:
    ##         for worker in self.threadPool.workers:
    ##             worker.reset()
    ##     else:
    ##         raise remoteObjectError("Sorry, this RO server was not created with a threadPool.")


    def ro_help(self, methodName):
        """Introspection function to print the method, its parameters,
        and it's docstring, if any.
        """
        # Check that the requested method is in the allowed list
        if not methodName in self.method_list:
            return ''

        # get the callable
        func = getattr(self.obj, methodName)

        # introspect the argument list
        (args, varargs, varkw, defaults) = inspect.getargspec(func)

        # remove 'self' from argument list, if present
        if (len(args) > 0) and (args[0] == 'self'):
            args.pop(0)

        # get doc string for the function
        docstr = inspect.getdoc(func)

        return '%s(%s)\n%s' % (methodName, ', '.join(args), str(docstr))


    def ro_help_all(self):
        res = []
        for methodName in self.method_list:
            res.append(self.ro_help(methodName))

        return '\n===\n'.join(res)


    def ro_setLogLevel(self, level):
        # this allows numeric
        level = ssdlog.get_level(level)

        self.logger.setLevel(level)
        # Because levels are settable at each handler, we have to run
        # through the handlers to set them as well.
        # Ugh...no logging API for getting handlers!
        for hdlr in self.logger.handlers:
            hdlr.setLevel(level)

        self.logger.info("LOGGING LEVEL RESET TO %d" % level)

        return OK


    def ro_stacktrace(self, thread_id):
        code = []
        stack = sys._current_frames()[thread_id]
        t = time.localtime(time.time())
        code.append("# time: %s  ThreadID: %s" % (
            time.strftime("%Y-%m-%d %H:%M:%S", t), thread_id))
        for filename, lineno, fnname, srcline in traceback.extract_stack(stack):
            code.append('File: "%s", line %d, in %s' % (filename, lineno, fnname))
            if srcline:
                code.append("  %s" % (srcline.strip()))
        return "\n".join(code)

    def ro_thread_ids(self):
        return list(sys._current_frames().keys())

    def ro_stacktraces(self):
        code = {}
        for thread_id in self.ro_thread_ids():
            code[str(thread_id)] = self.ro_stacktrace(thread_id)
        return code

    def ro_stacktraces_file(self, path):
        self.logger.warn("Dumping stacktraces to '%s'" % (path))
        with open(path, 'a') as out_f:
            for trace in self.ro_stacktraces().values():
                out_f.write("\n********************************\n")
                out_f.write(trace)
                out_f.write("\n")
        return True

    def ro_stacktraces_dump(self):
        name = time.strftime("%Y%m%d-%H%M%S-stacktrace",
                             time.localtime())
        name += '-' + self.svcname
        tracefile = os.path.join('/tmp', name)
        self.ro_stacktraces_file(tracefile)

    def __signal_handler(self, signum, frame):
        self.logger.error('Received signal %d' % signum)
        self.ro_stacktraces_dump()

    def ro_register_stacktraces_dump(self):
        signal.signal(signal.SIGUSR2, self.__signal_handler)

    def ro_get_pid(self):
        return self.__pid

    def __ns_register(self):
        # If a nameserver is defined and we have a servicename, try to
        # register our service
        if self.svcname and self.ns:
            try:
                self.ns.register(self.svcname, self.host, self.port,
                                 self.nsopts)

            except remoteObjectError as e:
                if self.strict_registration:
                    raise(e)
                self.logger.warn("Failed to register to name service: %s" % (
                    str(e)))


    def __ns_ping(self):
        if self.svcname and self.ns and self.pinginterval:
            now = time.time()
            if (now - self.lastpingtime) > self.pinginterval:
                try:
                    self.ns.ping(self.svcname, self.host, self.port,
                                 self.nsopts, now)

                except remoteObjectError as e:
                    #if self.strict_registration:
                    #    raise(e)
                    self.logger.warn("Failed to ping name service: %s" % (
                        str(e)))

                self.lastpingtime = now


    def __ns_unregister(self):
        # Unregister our service
        if self.svcname and self.ns:
            try:
                self.ns.unregister(self.svcname, self.host, self.port)

            except remoteObjectError as e:
                # Just as in SOSSrpc module, sometimes unregistering fails.
                # It seems best to silently ignore these for now...
                #self.logger.warn("Failed to unregister to name service: %s" % (
                #    str(e)))
                pass


    def __cmd_loop(self):
        '''Loop until asked to quit, serving XML-RPC requests.
        '''

        self.logger.info("Starting remote object server on %s:%d." % \
                           (self.host, self.port))

        # These let XML-RPC know about what methods we have available
        #self.server.register_introspection_functions()
        for name in self.method_list:
            self.server.register_function(getattr(self.obj, name))

        # Register expected methods unless they have been overridden
        for name in ['ro_echo', 'ro_list', 'ro_help', 'ro_help_all',
                     'ro_thread_ids', 'ro_stacktrace', 'ro_stacktraces',
                     'ro_stacktraces_file', 'ro_stacktraces_dump', 'ro_get_pid',
                     'ro_setLogLevel', 'ro_workerStatus']:
            if not hasattr(self.obj, name):
                self.server.register_function(getattr(self, name))

        #self.server.register_instance(self)

        # Register our service
        self.__ns_register()

        # Server requests until asked to terminate
        try:
            self.ev_stop.clear()
            self.ev_start.set()

            self.server.start()

            while not self.ev_quit.isSet():
                # Ping the name server if we haven't in a while
                self.__ns_ping()

                self.ev_quit.wait(timeout=1.0)

        except Exception as e:
            self.logger.error("Error running server: %s" % str(e))

        finally:
            self.logger.debug("Terminating request loop...")
            self.server.stop()

        # Unregister our service
        try:
            self.__ns_unregister()
        except:
            pass

        self.logger.info("Stopping remote object server on %s:%d." % \
                           (self.host, self.port))
        self.ev_start.clear()
        self.ev_stop.set()


    #
    # Remote execution commands
    #
    # NOTE: these cannot return null (None)
    #
    # Subclass remoteObjectServer and add your own methods.
    #

    def ro_echo(self, arg):
##         self.logger.debug('ro_echo: %s: %s' % (self.svcname, str(arg)))
##         return (arg, self.svcname)
        self.logger.debug('ro_echo: %s' % (str(arg)))
        return arg


#------------------------------------------------------------------
# Remote object client implementation
#
def call_remote(client, attrname, args, kwdargs):

    #method = client.proxy.get_method(attrname)
    try:
        #print "Trying to invoke method '%s.%s' on %s:%d" % \
        #      (client.name, attrname, client.host, client.port)
        #res = method(*args, **kwdargs)
        res = client.proxy.call(attrname, args, kwdargs)
        return (OK, res)

##     except xmlrpclib.Fault as e:
##         errstr = "Method call %s.%s failed to %s:%d: %s" % \
##                  (client.name, attrname, client.host, client.port, e)
##         return (ERROR_FATAL, errstr)

    except NameError as e:
        errstr ="No such method %s.%s at %s:%d." % \
                 (client.name, attrname, client.host, client.port)
        return (ERROR_FATAL, errstr)

    except socket.error as e:
        errstr = "Method call %s.%s failed to %s:%d: %s" % (
            client.name, attrname, client.host, client.port, str(e))
        return (ERROR_FAILOVER, errstr)

#     except IOError as e:
#         errstr = "Method call %s.%s failed to %s:%d: %s" % \
#                  (client.name, attrname, client.host, client.port, str(e))
#         return (ERROR_FAILOVER, errstr)

##     except remoteObjectError as e:
##         errstr = "Method call %s.%s failed to %s:%d: %s" % \
##                  (client.name, attrname, client.host, client.port, str(e))
##         return (ERROR_FATAL, errstr)

    except Exception as e:
        try:
            (type, value, tb) = sys.exc_info()
            tb = ''.join(traceback.format_tb(tb))

        except Exception:
            tb = "Traceback information unavailable."

        errstr = "Method call %s.%s failed to %s:%d: %s\n%s" % (
            client.name, attrname, client.host, client.port,
            str(e), tb)
        return (ERROR_FATAL, errstr)


class remoteObjectClient(object):

    """This class implements the interface for the remote calling of
    object methods.  i.e. it implements the "client" side as a proxy object.

    """

    def __init__(self, host, port, name='<remote object>', auth=None,
                 default_auth=use_default_auth, secure=default_secure,
                 transport=default_transport, encoding=default_encoding,
                 timeout=None):
        try:
            self.host = host
            self.port = port
            self.name = name
            self.transport = transport
            self.encoding = encoding

            ro_transport = transports[transport]

            if (not auth) and default_auth:
                auth = (self.name, self.name)
            elif isinstance(auth, str):
                # user:pass
                auth = auth.split(':')

            #print "client: auth=", auth
            self.proxy = ro_transport.make_serviceProxy(host, port,
                                                        auth=auth,
                                                        secure=secure,
                                                        timeout=timeout)

        except Exception as e:
            raise remoteObjectError("Can't create proxy to service found on host '%s' at port %d: %s" % \
                             (host, port, str(e)))


    def __getattr__(self, attrname):

        def call(*args, **kwdargs):

            (flag, res) = call_remote(self, attrname, args, kwdargs)
            if flag == OK:
                return res

            raise remoteObjectError(res)

        return call


    def __str__(self):
        return ("remoteObjectClient(%s, %d)" % (self.host, self.port))


class remoteObjectSP(object):

    """Base class for 'SP' (Service Pack)-based remote objects.

    """

    def __init__(self, name, svcpack=None, hostports=None, auth=None,
                 logger=None, default_auth=use_default_auth,
                 secure=default_secure, transport=default_transport,
                 timeout=None):

        self.name = name
        if auth is None and default_auth:
            auth = (name, name)
        elif isinstance(auth, str):
            # user:pass
            auth = auth.split(':')
        else:
            raise ValueError("Authorization format not recognized: '%s'" % (
                str(auth)))
        self.auth = auth

        # Logger for logging debug/error messages
        if not logger:
            self.logger = nullLogger()
        else:
            self.logger = logger

        if not svcpack:
            self.sp = servicePack(auth=auth, secure=secure,
                                  transport=transport, timeout=timeout)
            if hostports:
                for tup in hostports:
                    if len(tup) == 2:
                        (host, port) = tup
                        authp = self.auth
                    elif len(tup) == 3:
                        (host, port, authp) = tup
                    else:
                        raise remoteObjectError("Malformed hostports!")

                    self.sp.addHost(host, port, name=name, auth=authp,
                                    secure=secure, transport=transport)
        else:
            self.sp = svcpack

    def __str__(self):
        return ("remoteObjectProxy(%s)" % (self.name))


    # Subclasses should provide a __getattr__
    #def __getattr__(self, attrname):


class remoteObjectSPAll(remoteObjectSP):

    """This class implements a 'call all' remote object.  It will attempt to call
    the method on all of the clients in the service pack and return a dictionary of
    the results, indexed by (host, port) tuples.

    """

    def __getattr__(self, attrname):

        def call(*args, **kwdargs):

            # Call all clients, and gather the results into a dictionary
            results = {}
            for client in self.sp.getClients():
                key = (client.host, client.port)

                results[key] = call_remote(client, attrname, args, kwdargs)

            return results

        return call


class remoteObjectSPFailover(remoteObjectSP):

    """This class implements an SP remote object, which tries to call a method
    and will fail over to other entries in the service pack if errors occur.

    """

    def __init__(self, name, **kwdargs):

        self.clientidx = 0

        remoteObjectSP.__init__(self, name, **kwdargs)


    def __getattr__(self, attrname):

        def call(*args, **kwdargs):

            num = self.sp.numClients()

            res = "No client available"

            # Try the method on each client in the service pack, until one
            # returns with an answer, or we exhaust all possible clients.
            for i in range(num):
                client = self.sp[self.clientidx]

                (flag, res) = call_remote(client, attrname, args, kwdargs)

                if flag == OK:
                    return res

                elif flag == ERROR_FAILOVER:
                    # FAILOVER
                    self.logger.warn("Error: %s\nTrying to fail over to another candidate..." % (res))
                    self.clientidx = (self.clientidx + 1) % num
                    continue

                else:
                    raise remoteObjectError(res)

            raise remoteObjectError(res)

        return call


class remoteObjectProxy(remoteObjectSP):

    """This class implements an SP remote object, which tries to call a method
    and will fail over to other entries in the service pack if errors occur.

    """

    def __init__(self, name, ns=None, **kwdargs):
        self.clientidx = -1
        if not ns:
            # if no specific name server supplied, use the module default
            ns = default_ns
        self.ns = ns

        remoteObjectSP.__init__(self, name, **kwdargs)


    def __reset(self):
        if not self.ns:
            raise remoteObjectError("[remoteObjectProxy] no name server configured")

        # Lookup the service providers for name via the local name server.
        hostinfo = self.ns.getInfo(self.name)
        if len(hostinfo) == 0:
            raise remoteObjectError("[remoteObjectProxy] no remote object server found for '%s'" % self.name)

        # Synchronize our service pack to that set
        self.sp.syncFrom(hostinfo)
        self.clientidx = 0


    def __getattr__(self, attrname):

        def call(*args, **kwdargs):

            if self.clientidx < 0:
                self.__reset()

            client = self.sp[self.clientidx]
            (flag, res) = call_remote(client, attrname, args, kwdargs)
            if flag == OK:
                return res

            elif flag == ERROR_FAILOVER:
                # Failover.  Reset our idea of the current set of service providers.
                self.__reset()

                num = self.sp.numClients()

                # Try the method on each client in the service pack, until one
                # returns with an answer, or we exhaust all possible clients.
                for i in range(num):
                    client = self.sp[self.clientidx]

                    (flag, res) = call_remote(client, attrname, args, kwdargs)
                    if flag == OK:
                        return res

                    elif flag == ERROR_FAILOVER:
                        # FAILOVER
                        self.logger.warn("Error: %s\nTrying to fail over to another candidate..." % (res))
                        self.clientidx = (self.clientidx + 1) % num
                        continue

                    else:
                        break

            raise remoteObjectError(res)

        return call


class remoteObjectSPFailoverRR(remoteObjectSP):

    """This class implements an SP remote object, which tries to call a method
    and will subsequently call it on each successive client in the service pack
    on subsequent invocations of the call.

    """

    def __init__(self, name, **kwdargs):

        self.clientidx = 0

        remoteObjectSP.__init__(self, name, **kwdargs)


    def __getattr__(self, attrname):

        def call(*args, **kwdargs):

            num = self.sp.numClients()
            res = "No remote object servers available"

            # Try the method on each client in the service pack, until one
            # returns with an answer, or we exhaust all possible clients.
            for i in range(num):
                client = self.sp[self.clientidx]

                (flag, res) = call_remote(client, attrname, args, kwdargs)

                self.clientidx = (self.clientidx + 1) % num

                if flag == OK:
                    return res

                elif flag == ERROR_FAILOVER:
                    # FAILOVER
                    self.logger.warn("Error: %s\nTrying to fail over to another candidate..." % (res))
                    continue

                else:
                    break

            raise remoteObjectError(res)

        return call


#------------------------------------------------------------------
# Service pack interface
#

class servicePack(object):

    def __init__(self, auth=None, secure=default_secure, timeout=None,
                 transport=default_transport):
        self.auth = auth
        self.secure = secure
        self.timeout = timeout
        self.transport = transport

        self.clients = {}
        self.pinginfo = {}
        # The currently selected proxy out of the set of clients
        self.proxy = None
        # Index for round-robin or failover purposes
        self.index = 0
        self.strategy = 'std'

    # Allow iteration over a servicePack
    def __getitem__(self, i):
        key = list(self.clients.keys())[i]
        return self.clients[key]

    def has_key(self, host, port):
        key = (host, port)
        return key in self.clients

    def add(self, client, replace=True):
        key = (client.host, client.port)
        if key in self.clients and not replace:
            return
        self.clients[key] = client
        self.pinginfo[key] = {}

    def _get_auth_secure(self, auth, secure, transport):
        # If no authorization passed, default to servicePack default
        # (note that None != False)
        if auth == None:
            auth = self.auth
        # If no secure flag passed, default to servicePack default
        # (note that None != False)
        if secure == None:
            secure = self.secure
        if transport == None:
            transport = self.transport

        return (auth, secure, transport)

    def addHost(self, host, port, name='', auth=None, secure=None,
                transport=None, timeout=None, replace=True):
        (auth, secure, transport) = self._get_auth_secure(auth, secure,
                                                          transport)

        key = (host, port)
        if key in self.clients and not replace:
            return
        if timeout == None:
            timeout = self.timeout
        client = remoteObjectClient(host, port, name=name, auth=auth,
                                    secure=secure, transport=transport,
                                    timeout=timeout)
        curtime = time.time()
        info = {
            'name': name,
            'host': host, 'port': port,
            'secure': secure, 'auth': auth,
            'transport': transport,
            'lastping': curtime, 'lastupdate': curtime,
            }
        self.clients[key] = client
        self.pinginfo[key] = info

    def recordPingFrom(self, host, port, name, nsinfo):
        key = (host, port)
        self.pinginfo[key].update(nsinfo)

    def clear(self):
        self.clients.clear()
        self.pinginfo.clear()

    def syncFrom(self, hostinfo, auth=None, secure=None, transport=None,
                 deleteOrphans=True):
        """Synchronize to a sequence of dicts containing info for providers
        of a particular service."""

        (auth, secure, transport) = self._get_auth_secure(auth, secure,
                                                          transport)

        hostports = []

        # If there is a new service provider in the hostinfo that is not in
        # our set, then add them.
        for d in hostinfo:
            self.addHost(d['host'], d['port'], replace=False, auth=auth,
                         secure=d.get('secure', secure),
                         transport=d.get('transport', transport))
            hostports.append((d['host'], d['port']))

        # If we have a service provider in our client set that is not in the
        # synclist, then delete them.
        if deleteOrphans:
            for key in list(self.clients.keys()):
                if not key in hostports:
                    del self.clients[key]

    def delHost(self, host, port):
        key = (host, port)
        # If no entry for this service, silently return.
        if key not in self.clients:
            return

        if self.proxy == self.clients[key]:
            self.proxy = None

        del self.clients[key]
        del self.pinginfo[key]

    def getClient(self, host, port):
        key = (host, port)
        return self.clients[key]

    def getClients(self):
        return self.clients.values()

    def numClients(self):
        return len(self.clients)

    def showAll(self):
        return list(self.clients.keys())

    def getInfo(self, host, port):
        return self.pinginfo[key]

    def getInfoAll(self):
        res = {}
        for key in list(self.clients.keys()):
            res[key] = self.pinginfo[key]

        return res

    def showChosen(self):
        if self.proxy:
            return (self.proxy.host, self.proxy.port)

        raise remoteObjectError("No valid clients")

    def chooseClient(self):

        hosts = list(self.clients.keys())
        for key in hosts:
            client = self.clients[key]
            try:
                if client.ro_echo(1):
##                     print 'chooseClientbyPing: found listener at %s:%d' % \
##                           (client.host, client.port)
                    self.proxy = client
                    return client

            except remoteObjectError as e:
##                 print 'chooseClientbyPing: client error: %s' % str(e)
                continue

        self.proxy = None
        raise remoteObjectError("No client responds to ping: %s" % str(hosts))

    def getLosers(self):

        hostports = list(self.clients.keys())
        echoval = 99
        results = []

        for key in hostports:
            client = self.clients[key]
            try:
                if not (echoval == client.ro_echo(echoval)):
                    results.append(key)

            except remoteObjectError as e:
                results.append(key)

        return results


    # Delete any services from this service pack that are not responding to
    # heartbeats.
    #
    def purge(self):

        goodclient = None
        hosts = list(self.clients.keys())

        for key in hosts:
            client = self.clients[key]
            try:
                if client.ro_echo(1):
                    goodclient = client
                    continue

            except remoteObjectError as e:
                pass

##             print "deleting unresponsive client: %s:%d" % \
##                   (client.host, client.port)
            self.delhost(client.host, client.port)

        if not self.proxy:
            self.proxy = goodclient


#------------------------------------------------------------------
# Misc helper functions and classes
#

# Null logger in case a logger is not passed to the remoteObjectServer
#
class nullLogger(object):
    def __init__(self, f_out=None):
        self.f_out = f_out

    def debug(self, msg):
        if self.f_out:
            self.f_out.write("%s\n" % msg)
            self.f_out.flush()

    def info(self, msg):
        if self.f_out:
            self.f_out.write("%s\n" % msg)
            self.f_out.flush()

    def warning(self, msg):
        if self.f_out:
            self.f_out.write("%s\n" % msg)
            self.f_out.flush()

    warn = warning

    def error(self, msg):
        if self.f_out:
            self.f_out.write("%s\n" % msg)
            self.f_out.flush()


# EXPORTED MODULE-LEVEL FUNCTIONS

# Use these to abstract transporting binary buffers.  binascii is much
# faster than the one used by xmlrpclib or base64 modules.

def binary_encode(buffer):
    return binascii.b2a_base64(buffer).decode('latin1')

def binary_decode(data):
    return binascii.a2b_base64(data)

def compress(data):
    return zlib.compress(data)

def uncompress(buffer):
    return zlib.decompress(buffer)

def cleanse_dict(d):
    new_d = {}
    for key, val in d.items():
        if isinstance(val, float):
            pass
        elif isinstance(val, int):
            pass
        elif isinstance(val, str):
            pass
        else:
            # convert everything else to a string
            val = str(val)

        new_d[key] = val
    return new_d

def populate_host(hostbnch, def_user=None, def_port=None):

    hostbnch.setdefault('user', def_user)
    port = hostbnch.setdefault('port', def_port)
    fqdn = socket.getfqdn(hostbnch.host)
    hostbnch.setdefault('fqdn', fqdn)
    if port:
        key=('%s:%d' % (fqdn, port))
    else:
        key = fqdn
    hostbnch.setdefault('key', key)

    return hostbnch


def split_host(elt, def_user=None, def_port=None):
    # elt is of the format user@host:port

    res = Bunch.Bunch()

    # Split into host, port (port is optional)
    info = elt.split(':')
    elt = info[0]
    if len(info) > 1:
        res.port = int(info[1])

    # Split host into user, host (user is optional)
    info = elt.split('@')
    if len(info) > 1:
        res.user = info[0]
        host = info[1]
    else:
        host = info[0]
    res.host = host

    # sets 'fqdn', 'key' & possibly 'user' and 'port'
    populate_host(res, def_user=def_user, port=def_port)

    return res


def unique_hosts(elts):
    """Return the unique set of hosts for a list of [user@host:port ...]
    """
    l = list(set([host.fqdn for host in elts]))
    l.sort()
    return l

def unique_host_ports(elts):
    """Return the unique set of (host, port) for a list of [user@host:port ...]
    """
    l = list(set([(host.fqdn, host.port) for host in elts]))
    l.sort()
    return l


def find_free_port(host, start_port, end_port):

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    found_port = False
    for port in range(start_port, start_port+15000):
        try:
            sock.bind((host, port))
            found_port = True
            break

        except socket.error:
            continue

    if not found_port:
        raise remoteObjectError('No free port found for remote object server')
    else:
        try:
            sock.close()
        except:
            pass
        return port


def get_myhost(short=False):
    try:
        myhost = socket.getfqdn()

    except Exception as e:
        raise remoteObjectError("Can't get my own host name: %s" % str(e))

    if not short:
        return myhost
    else:
        return myhost.split('.')[0]


def get_hosts(svcname, nshost=None, port=nameServicePort,
              auth=None, secure=default_secure):
    """Find out all hosts that are hosting a given service.
    """

    if not nshost:
        nshost = get_myhost()

    # Make a handle to the remote object name service on the local
    # machine.  Query it to see the list of hosts running svcname
    tmpns = remoteObjectClient(host=nshost, port=port,
                               transport=ns_transport,
                               auth=auth, secure=secure)

    hostports = tmpns.getHosts(svcname)

    ro_hosts = []
    for host, port in hostports:
        ro_hosts.append(host)

    return ro_hosts


# Get list of hosts in the remote object playground.  If RO_HOSTS is
# set then we use it, otherwise we query the local name server to find
# out who is playing.
#
def get_ro_hosts(nshost=None):
    """Query the name server for the list of all hosts in the remote objects
    play space.
    """

    if 'RO_HOSTS' in os.environ:
        ro_hosts = os.environ['RO_HOSTS'].strip().split(',')

    else:
        if not nshost:
            # Does user have a list of name servers defined?
            if 'RO_NAMES' in os.environ:
                ro_hosts = os.environ['RO_NAMES'].strip().split(',')
                nshost = ro_hosts[0]
            else:
                # Try looking at localhost and see if there is NS running
                nshost = get_myhost()
                ro_hosts = [nshost]

        try:
            ro_hosts = get_hosts('names', nshost=nshost)

        except remoteObjectError as e:
            #raise NameServiceWarning("Can't connect to name server; assuming remote hosts=%s" % (str(ro_hosts)))
            pass

    # Sort so that all hosts have the same view of the list
    ro_hosts.sort()

    return ro_hosts


def addns(host, auth=None, secure=default_secure):
    global default_ns
    default_ns = remoteObjectProxy('names', host=host, port=nameServicePort,
                                   transport=ns_transport,
                                   auth=auth, secure=secure)

def make_robunch(name, hostports=None, auth=None, secure=default_secure,
                 ns=None):
    """Creates a bunch with handles to all of the individual services running
    on each host, plus a remoteObjectSP handle to all hosts.  If the hostport
    list is not given then the hostport list is queried from the local name
    server.
    """
    # If no list of hostnames is given, then query it from the local name server.
    if (not hostports):
        if ns:
            hostports = ns.getHosts(name)
        elif default_ns:
            hostports = default_ns.getHosts(name)
        else:
            # TODO: raise an exception?
            hostports = []

    sp = servicePack()
    bunch = Bunch.Bunch()
    for (host, port) in hostports:
        host = socket.getfqdn(host)
        client = remoteObjectClient(host=host, port=port,
                                    name=('%s(%s)' % (name, host)),
                                    auth=auth, secure=secure)
        bunch['%s:%d' % (host, port)] = client
        sp.add(client)

    bunch['all'] = remoteObjectSPAll('%s(all)' % (name), svcpack=sp)
    return bunch

def make_mspack(hosts, auth=None, secure=default_secure):
    sp = servicePack(auth=auth, secure=secure)
    for host in hosts:
        client = remoteObjectClient(host=host, port=managerServicePort,
                                    name=('monsvc(%s)' % host),
                                    auth=auth, secure=secure)
        sp.add(client)

    return remoteObjectSPFailover('monsvc', svcpack=sp)

def getms(hosts=None, auth=None, secure=default_secure):
    if not hosts:
        hosts = get_ro_hosts()

    return make_mspack(hosts, auth=auth, secure=secure)

def make_nspack(hosts, auth=None, secure=default_secure):
    sp = servicePack(auth=auth, secure=secure)
    for host in hosts:
        client = remoteObjectClient(host=host, port=nameServicePort,
                                    transport=ns_transport,
                                    name=('names(%s)' % host),
                                    auth=auth, secure=secure)
        sp.add(client)

    return remoteObjectSPFailover('names', svcpack=sp)

def getns(hosts=None, auth=None, secure=default_secure):
    if not hosts:
        hosts = get_ro_hosts()

    return make_nspack(hosts, auth=auth, secure=secure)


def write_pid_file(filepath):
    with open(filepath, 'w') as out_f:
        out_f.write(str(os.getpid()))


def init(ro_hosts=None,
         allowNSfailure=True, allowMSfailure=True,
         auth=None, secure=default_secure):
    """Initialize the remoteObjects system.  Find out what hosts we know about
    and try to obtain handles to the manager service and the name service.
    """
    global default_ns, default_ms, default_secure

    try:
        if not ro_hosts:
            ro_hosts = get_ro_hosts()

        default_ms = getms(ro_hosts, auth=auth, secure=secure)
        #default_ms.ro_echo(1)
        #print "ms=%s" % str(ms)

    except remoteObjectError as e:
        # No manager service available!
        if not allowMSfailure:
            raise ManagerServiceWarning("Cannot contact manager service: %s" % (
                str(e)))

    try:
        default_ns = getns(ro_hosts, auth=auth, secure=secure)
        default_ns.ro_echo(1)
        #print "ns=%s" % str(ns)

    except remoteObjectError as e:
        # No name service available!
        if not allowNSfailure:
            raise NameServiceWarning("Cannot contact name service: %s" % (
                str(e)))

    # ro.init() sets the default module security
    default_secure = secure


def addlogopts(optprs):
    """Add special options used in remoteObjects applications."""
    if hasattr(optprs, 'add_option'):
        # older optparse
        add_argument = optprs.add_option
    else:
        # newer argparse
        add_argument = optprs.add_argument

    add_argument("--auth", dest="auth",
                 help="Use authorization; arg should be user:passwd")
    add_argument("--cert", dest="cert",
                      help="Path to key/certificate file")
    add_argument("--secure", dest="secure", action="store_true",
                 default=False,
                 help="Use SSL encryption")


# END
