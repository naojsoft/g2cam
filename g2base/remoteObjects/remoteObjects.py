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

import sys
import os
import time
import socket
import threading
# binascii encoding/decoding is much faster than xmlrpclib's
# built-in Binary class
import binascii
import zlib
import traceback
import inspect
import signal

from concurrent import futures

from g2base import Bunch, Task, ssdlog

import xmlrpc.client

from tinyrpc import exc as tinyrpc_exc
from tinyrpc.client import RPCClient
from tinyrpc.dispatch import RPCDispatcher
from tinyrpc.server.executor import RPCServerExecutor

from . import ro_endpoints, ro_executor, ro_transport
from .ro_config import *

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

#: Methods that start and stop the server itself.  These are how the process
#: hosting a service controls it, not operations the service offers, and they
#: are never exposed remotely: a service that subclasses remoteObjectServer
#: would otherwise let any client that can reach it shut it down.
local_only_methods = frozenset(['ro_start', 'ro_stop',
                                'ro_wait_start', 'ro_wait_stop'])

#: Introspection and debugging methods every remote object server offers,
#: unless the served object defines one of its own.
ro_methods = ['ro_echo', 'ro_list', 'ro_help', 'ro_help_all',
              'ro_thread_ids', 'ro_stacktrace', 'ro_stacktraces',
              'ro_stacktraces_file', 'ro_stacktraces_dump', 'ro_get_pid',
              'ro_setLogLevel', 'ro_workerStatus']


def make_authenticator(authDict, logger):
    """Build a server authenticator that checks HTTP Basic credentials.

    The transport has already taken the credentials off the request and put
    them on the context, so this only decides whether to accept them, and
    works the same whichever protocol and transport carried them.
    """
    def authenticate(context, request):
        auth = getattr(context, 'auth', None)
        if auth is None:
            logger.error("No authentication credentials passed")
            raise remoteObjectError(
                "Service requires authentication and no credentials passed")

        username, password = auth
        if username not in authDict:
            logger.error("No user matching '%s'" % (username,))
            raise remoteObjectError(
                "Service requires authentication; "
                "username or password mismatch")

        if authDict[username] != password:
            logger.error("Password incorrect for '%s'" % (username,))
            # As before: slow down brute force attempts.  Note that this
            # occupies a worker for the duration, so it also slows the
            # service down for everyone when credentials are merely stale.
            time.sleep(1.0)
            raise remoteObjectError(
                "Service requires authentication; "
                "username or password mismatch")

        logger.debug("Authorized client '%s'" % (username,))

    return authenticate


class remoteObjectServer:

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

        if obj is None:
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
                    if (method_prefix is not None):
                        if attrName.startswith(method_prefix):
                            methodNames.append(attrName)
                    elif not attrName.startswith('_'):
                        methodNames.append(attrName)

        # Logger for logging debug/error messages
        if not logger:
            self.logger = nullLogger()
        else:
            self.logger = logger

        # A server that subclasses remoteObjectServer picks up its own
        # lifecycle methods in the scan above, which would let any client
        # that can reach the service stop it.  Never expose those.
        withheld = local_only_methods.intersection(methodNames)
        if withheld:
            self.logger.warning(
                "not exposing %s: these control this server's own lifecycle "
                "and are only for the process hosting it"
                % (', '.join(sorted(withheld)),))

        self.method_list = sorted(set(methodNames) - local_only_methods)

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

        self.spec = ro_transport.get(transport, encoding=encoding)

        # What we tell the name service about ourselves.  'protocol' and
        # 'encoding' describe what we actually speak; 'transport' is the
        # field un-upgraded clients read, and carries the name they know this
        # by -- or, for a protocol that has no old equivalent, the new name,
        # which such a client cannot use under any label and will refuse
        # legibly rather than mistake for something else.
        self.nsopts = {'secure': secure,
                       'protocol': self.spec.name,
                       'encoding': self.spec.encoding,
                       'transport': (self.spec.legacy_transport or
                                     self.spec.name),
                       }

        ssl_context = None
        if self.secure:
            ssl_context = ro_transport.make_ssl_context(self.cert_file)

        # Bind and keep.  The old find_free_port() bound a port, closed it,
        # and returned the number for the server to bind again, which left a
        # window for somebody else to take it.  Binding the real listening
        # socket as we search closes that window.
        self.rpc_transport = self.__bind(ssl_context)
        self.port = self.rpc_transport.endpoint[1]

        # Everything the object exposes, plus the ro_* methods it did not
        # override, goes in the dispatcher.
        self.dispatcher = RPCDispatcher()
        for name in self.method_list:
            self.dispatcher.add_method(getattr(self.obj, name), name)
        for name in ro_methods:
            if not hasattr(self.obj, name):
                self.dispatcher.add_method(getattr(self, name), name)

        if self.threadPool is not None:
            # Run handlers on the pool the application gave us, as before.
            self.executor = ro_executor.ThreadPoolExecutor(self.threadPool)
            self.__own_executor = False
        else:
            # The serve loop occupies a worker for as long as the server
            # runs, so it needs one of its own on top of the handlers'.
            # With a single worker there would be nobody left to dispatch to
            # and the service would accept a request and then hang.
            max_workers = 1 + (self.numthreads if self.threaded_server else 1)
            self.executor = futures.ThreadPoolExecutor(
                max_workers=max_workers,
                thread_name_prefix='ro-%s' % (self.svcname or self.name or
                                              'server'))
            self.__own_executor = True

        self.server = RPCServerExecutor(self.rpc_transport,
                                        self.spec.make_protocol(),
                                        self.dispatcher,
                                        self.executor,
                                        ev_quit=self.ev_quit)
        if self.authDict:
            self.server.authenticator = make_authenticator(self.authDict,
                                                           self.logger)

    def __bind(self, ssl_context):
        """Bind the listening socket, searching the service port range when
        no specific port was asked for."""
        if self.port:
            candidates = [self.port]
        else:
            candidates = range(objectsBasePort, objectsBasePort + 15000)

        last_error = None
        for port in candidates:
            try:
                return self.spec.make_server_transport(
                    self.bindhost, port, logger=self.logger,
                    ssl_context=ssl_context, poll_timeout=self.timeout)
            except OSError as e:
                last_error = e
                continue

        if self.port:
            raise remoteObjectError(
                "Can't bind %s:%d for remote object server: %s"
                % (self.bindhost or '*', self.port, last_error))
        raise remoteObjectError(
            'No free port found for remote object server in %d-%d: %s'
            % (objectsBasePort, objectsBasePort + 15000, last_error))


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

        if self.__own_executor:
            # Only ours to shut down; a caller-supplied thread pool is
            # usually shared with the rest of the application.
            self.executor.shutdown(wait=False)

        if wait:
            if self.usethread:
                # This seems to cause some hangs
                #self.mythread.join()
                self.ev_stop.wait(timeout=timeout)
            else:
                self.ro_wait_stop()


    def ro_wait_start(self, timeout=None):
        '''Wait for remote object server to start.'''
        if not self.ev_start.is_set():
            self.ev_start.wait(timeout=timeout)

        if not self.ev_start.is_set():
            raise remoteObjectError("Timed out waiting for server to start")


    def ro_wait_stop(self, timeout=None):
        '''Wait for remote object server to terminate.'''
        if not self.ev_stop.is_set():
            self.ev_stop.wait(timeout=timeout)

        if not self.ev_stop.is_set():
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
        if methodName not in self.method_list:
            return ''

        # get the callable
        func = getattr(self.obj, methodName)

        # introspect the argument list (getargspec was removed in 3.11,
        # and signature() renders bound methods without 'self' anyway)
        try:
            sig = str(inspect.signature(func))
        except (TypeError, ValueError):
            sig = '(...)'

        # get doc string for the function
        docstr = inspect.getdoc(func)

        return '%s%s\n%s' % (methodName, sig, str(docstr))


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

        # The methods were put in the dispatcher when the server was built.

        # Register our service
        self.__ns_register()

        # Server requests until asked to terminate
        try:
            self.ev_stop.clear()
            self.ev_start.set()

            self.server.start()

            while not self.ev_quit.is_set():
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
        except Exception:
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

#: Exceptions that mean "this provider did not answer", as opposed to "this
#: call failed".  They are what makes failing over to another provider worth
#: trying, and they are the whole of the fault-tolerance policy.
failover_errors = (
    ConnectionError,            # covers requests.ConnectionError
    socket.timeout,
    OSError,                    # covers socket.error and requests' IOError base
    tinyrpc_exc.TimeoutError,
)

#: Exceptions that mean the call reached a service and that service said no.
#: Trying somewhere else would just produce the same answer.
fatal_errors = (
    xmlrpc.client.Fault,
    tinyrpc_exc.RPCError,
)


def normalize_auth(auth, name=None, default_auth=use_default_auth):
    """Put authentication credentials into one shape: ``(user, passwd)``.

    Accepts ``None``, a ``'user:passwd'`` string, or any two-element
    sequence.  The old version accepted only ``None`` and a string and raised
    ValueError on anything else -- including the ``(user, passwd)`` tuple that
    the rest of the module passes around, so ``ro_test.py --auth=bob:pw``,
    which splits into a list before calling, failed before it made a call.

    :param name: Used for the default ``(name, name)`` credentials.
    :param default_auth: Whether to supply those when none were given.
    """
    if auth is None:
        if default_auth and name:
            return (name, name)
        return None

    if isinstance(auth, str):
        user, sep, passwd = auth.partition(':')
        if not sep:
            raise ValueError(
                "authorization string should be 'user:passwd', not '%s'"
                % (auth,))
        return (user, passwd)

    if isinstance(auth, (tuple, list)) and len(auth) == 2:
        return (auth[0], auth[1])

    raise ValueError("Authorization format not recognized: '%s'" % (auth,))


def call_remote(client, attrname, args, kwdargs):
    """Make one call and classify the outcome.

    :return: ``(OK, result)``, ``(ERROR_FAILOVER, message)`` when the
        provider did not answer and another one is worth trying, or
        ``(ERROR_FATAL, message)`` when it did answer and refused.
    """
    where = "%s.%s at %s:%d" % (client.name, attrname, client.host,
                                client.port)
    try:
        return (OK, client.proxy.call(attrname, args, kwdargs))

    except fatal_errors as e:
        return (ERROR_FATAL, "Method call %s failed: %s" % (where, e))

    except failover_errors as e:
        return (ERROR_FAILOVER, "Method call %s failed: %s" % (where, e))

    except Exception as e:
        try:
            tb = ''.join(traceback.format_tb(sys.exc_info()[2]))
        except Exception:
            tb = "Traceback information unavailable."
        return (ERROR_FATAL,
                "Method call %s failed: %s\n%s" % (where, e, tb))


class remoteObjectClient:
    """A handle on one service, at one host and port.

    Attribute access returns a callable that makes the remote call, so
    ``client.foo(1, 2)`` calls ``foo(1, 2)`` on the service.
    """

    def __init__(self, host, port, name='<remote object>', auth=None,
                 default_auth=use_default_auth, secure=default_secure,
                 transport=default_transport, encoding=default_encoding,
                 timeout=None):
        self.host = host
        self.port = port
        self.name = name
        self.transport = transport
        self.encoding = encoding
        self.secure = secure
        self.timeout = timeout

        try:
            self.auth = normalize_auth(auth, name=name,
                                       default_auth=default_auth)
            self.spec = ro_transport.get(transport, encoding=encoding)
            self.proxy = _ServiceProxy(self.spec, host, port, auth=self.auth,
                                       secure=secure, timeout=timeout)

        except Exception as e:
            raise remoteObjectError(
                "Can't create proxy to service found on host '%s' at port "
                "%d: %s" % (host, port, e))

    def __getattr__(self, attrname):
        if attrname.startswith('__'):
            raise AttributeError(attrname)

        def call(*args, **kwdargs):
            (flag, res) = call_remote(self, attrname, args, kwdargs)
            if flag == OK:
                return res
            raise remoteObjectError(res)

        return call

    def __str__(self):
        return "remoteObjectClient(%s, %d)" % (self.host, self.port)


class _ServiceProxy:
    """Makes the actual call, over a fresh connection each time.

    A connection per call is what lets a client and a service be restarted in
    any order: there is nothing held between calls to go stale.
    """

    def __init__(self, spec, host, port, auth=None, secure=False,
                 timeout=None):
        self.spec = spec
        self.host = host
        self.port = port
        self.auth = auth
        self.secure = secure
        self.timeout = timeout

    def call(self, attrname, args, kwdargs):
        transport = self.spec.make_client_transport(
            self.host, self.port, auth=self.auth, secure=self.secure,
            timeout=self.timeout)
        client = RPCClient(self.spec.make_protocol(), transport)
        return client.call(attrname, tuple(args), dict(kwdargs) or None)


#------------------------------------------------------------------
# Calling strategies
#
# What to do with the providers an Endpoints gives us.  These are separate
# from where the providers come from, so either can be chosen without
# constraining the other.
#

def call_failover(endpoints, attrname, args, kwdargs, logger=None):
    """Call the first provider that answers.

    On a connection-level failure the endpoints are resolved again -- the set
    of providers may have changed, and the name service is the authority on
    that -- and the rest are tried in turn.
    """
    clients = endpoints.clients()
    if not clients:
        raise remoteObjectError("No provider available for '%s'"
                                % (endpoints.name,))

    (flag, res) = call_remote(clients[0], attrname, args, kwdargs)
    if flag == OK:
        return res
    if flag != ERROR_FAILOVER:
        raise remoteObjectError(res)

    # Did not answer.  Ask again who provides this service, then work
    # through them.
    if logger:
        logger.warning("%s; re-resolving '%s' and trying another provider"
                       % (res, endpoints.name))
    try:
        clients = endpoints.refresh()
    except LookupError as e:
        raise remoteObjectError(str(e))

    for client in clients:
        (flag, res) = call_remote(client, attrname, args, kwdargs)
        if flag == OK:
            return res
        if flag != ERROR_FAILOVER:
            break
        if logger:
            logger.warning("%s; trying another provider" % (res,))

    raise remoteObjectError(res)


def call_all(endpoints, attrname, args, kwdargs):
    """Call every provider, and report on each.

    :return: ``{(host, port): (flag, result)}``.  Nothing is raised for a
        provider that fails: the point is to see what each one said.
    """
    results = {}
    for client in endpoints.clients():
        results[(client.host, client.port)] = call_remote(
            client, attrname, args, kwdargs)
    return results


#------------------------------------------------------------------
# Proxies
#

class _ProxyBase:
    """Shared construction for the attribute-style proxies."""

    def __init__(self, name, hostports=None, ns=None, auth=None,
                 logger=None, default_auth=use_default_auth,
                 secure=default_secure, transport=default_transport,
                 encoding=default_encoding, timeout=None):
        self.name = name
        # Per-instance: a hostports entry may carry its own credentials, and
        # sharing that map between proxies would leak one service's
        # credentials into another's calls.
        self._auth_overrides = {}
        self.auth = normalize_auth(auth, name=name, default_auth=default_auth)
        self.logger = logger if logger else nullLogger()
        self.secure = secure
        self.transport = transport
        self.encoding = encoding
        self.timeout = timeout

        if hostports is not None:
            self.endpoints = ro_endpoints.StaticEndpoints(
                name, [self.__hostport(t) for t in hostports],
                self.__client_for_hostport, logger=self.logger)
        else:
            if ns is None:
                ns = default_ns
            self.endpoints = ro_endpoints.NameSvcEndpoints(
                name, ns, self.__client_for_record, logger=self.logger)

    def __hostport(self, tup):
        # (host, port) or (host, port, auth)
        if len(tup) == 2:
            return (tup[0], tup[1])
        if len(tup) == 3:
            self._auth_overrides[(tup[0], tup[1])] = normalize_auth(
                tup[2], name=self.name, default_auth=False)
            return (tup[0], tup[1])
        raise remoteObjectError("Malformed hostports entry: %s" % (tup,))

    def __client_for_hostport(self, host, port):
        auth = self._auth_overrides.get((host, port), self.auth)
        return remoteObjectClient(host, port, name=self.name, auth=auth,
                                  default_auth=False, secure=self.secure,
                                  transport=self.transport,
                                  encoding=self.encoding,
                                  timeout=self.timeout)

    def __client_for_record(self, rec):
        """Build a client from one name service registration.

        The registration says which transport and encoding that provider
        speaks, so a set of providers for one name may legitimately not all
        speak the same thing.
        """
        # 'protocol' is what a current name service reports; 'transport'
        # is the older field, still emitted, whose values ro_transport
        # translates.
        protocol = rec.get('protocol') or rec.get('transport') \
            or self.transport

        return remoteObjectClient(
            rec['host'], rec['port'], name=self.name, auth=self.auth,
            default_auth=False,
            secure=rec.get('secure', self.secure),
            transport=protocol,
            encoding=rec.get('encoding', self.encoding),
            timeout=self.timeout)

    def __str__(self):
        return "%s(%s)" % (type(self).__name__, self.name)


class remoteObjectProxy(_ProxyBase):
    """A handle on a service by name, wherever it is running.

    Providers are looked up in the name service on the first call and reused
    afterwards.  When one stops answering, the name service is asked again
    and the remaining providers are tried.  In practice there is usually just
    the one.
    """

    def __getattr__(self, attrname):
        if attrname.startswith('__'):
            raise AttributeError(attrname)

        def call(*args, **kwdargs):
            return call_failover(self.endpoints, attrname, args, kwdargs,
                                 logger=self.logger)

        return call


class remoteObjectProxyAll(_ProxyBase):
    """A handle that calls *every* provider of a service.

    Returns ``{(host, port): (flag, result)}``.  Give ``hostports`` to name
    the providers, or leave it out to call whoever the name service says is
    providing the service.
    """

    def __getattr__(self, attrname):
        if attrname.startswith('__'):
            raise AttributeError(attrname)

        def call(*args, **kwdargs):
            return call_all(self.endpoints, attrname, args, kwdargs)

        return call


#: Former name of :py:class:`remoteObjectProxyAll`.
#: TODO: remove once nothing refers to it.
remoteObjectSPAll = remoteObjectProxyAll

#------------------------------------------------------------------
# Misc helper functions and classes
#

# Null logger in case a logger is not passed to the remoteObjectServer
#
class nullLogger:
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
        except Exception:
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
    """Point the module's default name service at _host_."""
    global default_ns
    default_ns = make_nspack([host], auth=auth, secure=secure)

def make_robunch(name, hostports=None, auth=None, secure=default_secure,
                 ns=None):
    """A bunch of handles to each provider of a service, plus one for all.

    Individual providers are keyed ``'host:port'``; ``bunch['all']`` calls
    every one of them.  If no hostports are given they are queried from the
    name service.
    """
    if not hostports:
        if ns:
            hostports = ns.getHosts(name)
        elif default_ns:
            hostports = default_ns.getHosts(name)
        else:
            hostports = []

    hostports = [(socket.getfqdn(host), port) for host, port in hostports]

    bunch = Bunch.Bunch()
    for (host, port) in hostports:
        bunch['%s:%d' % (host, port)] = remoteObjectClient(
            host=host, port=port, name=name, auth=auth, secure=secure)

    bunch['all'] = remoteObjectProxyAll(name, hostports=hostports, auth=auth,
                                        secure=secure)
    return bunch


def make_mspack(hosts, auth=None, secure=default_secure):
    """A failover handle to the manager service on each of _hosts_."""
    return remoteObjectProxy('monsvc',
                             hostports=[(host, managerServicePort)
                                        for host in hosts],
                             auth=auth, secure=secure)


def getms(hosts=None, auth=None, secure=default_secure):
    if not hosts:
        hosts = get_ro_hosts()

    return make_mspack(hosts, auth=auth, secure=secure)


def make_nspack(hosts, auth=None, secure=default_secure):
    """A failover handle to the name service on each of _hosts_.

    Deliberately built from an explicit host list rather than by lookup:
    this is the handle used to *do* lookups, so it cannot rely on one.
    """
    return remoteObjectProxy('names',
                             hostports=[(host, nameServicePort)
                                        for host in hosts],
                             transport=ns_transport, encoding=ns_encoding,
                             auth=auth, secure=secure)


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
    add_argument = optprs.add_argument

    add_argument("--auth", dest="auth",
                 help="Use authorization; arg should be user:passwd")
    add_argument("--cert", dest="cert",
                      help="Path to key/certificate file")
    add_argument("--secure", dest="secure", action="store_true",
                 default=False,
                 help="Use SSL encryption")


# END
