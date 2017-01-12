#
# ro_XMLRPC.py -- enhanced XML-RPC services for remoteObjects system
#
# Eric Jeschke (eric@naoj.org)
#
import sys
import threading
import traceback
import base64
from g2base import six
if six.PY2:
    import Queue
    from SimpleXMLRPCServer import (SimpleXMLRPCServer,
                                    SimpleXMLRPCRequestHandler,
                                    SimpleXMLRPCDispatcher)
    from xmlrpclib import ServerProxy, Transport
else:
    import queue as Queue
    from xmlrpc.server import (SimpleXMLRPCServer, SimpleXMLRPCRequestHandler,
                               SimpleXMLRPCDispatcher)
    from xmlrpc.client import ServerProxy, Transport
try:
    import fcntl
except ImportError:
    fcntl = None

from g2base import Task

version = '20160912.0'

# Timeout value for XML-RPC sockets
socket_timeout = 0.25


class Error(Exception):
    """Class of errors raised in this module."""
    pass

class socketTimeout(Error):
    """Raised when a socket times out waiting for a client."""
    pass


#
# ------------------ CONVENIENCE FUNCTIONS ------------------
#
class ServiceProxy(object):

    def __init__(self, url):
        self.url = url
        # transport = MyTransport()
        #self.proxy = ServerProxy(self.url, transport=transport,
        #                         allow_none=True)
        self.proxy = ServerProxy(self.url, allow_none=True)
        #self.logger = logger

    def call(self, attrname, args, kwdargs):

        #transport = MyTransport()
        #proxy = ServerProxy(self.url, transport=transport,
        #                      allow_none=True)
        #proxy = ServerProxy(self.url, allow_none=True)
        method = eval('self.proxy.%s' % attrname)
        return method(*args, **kwdargs)

def make_serviceProxy(host, port, auth=None, secure=False, timeout=None):
    """
    Convenience function to make a XML-RPC service proxy.
    'auth' is None for no authentication, otherwise (user, passwd)
    'secure' should be True if you want to use SSL, otherwise vanilla http.
    """
    if auth is not None:
        user, passwd = auth

    try:
        if secure:
            if auth is not None:
                url = 'https://%s:%s@%s:%d/' % (user, passwd, host, port)
            else:
                url = 'https://%s:%d/' % (host, port)
        else:
            if auth is not None:
                url = 'http://%s:%s@%s:%d/' % (user, passwd, host, port)
            else:
                url = 'http://%s:%d/' % (host, port)

        return ServiceProxy(url)

    except Exception as e:
        raise Error("Can't create proxy to service found on '%s:%d': %s" % (
            host, port, str(e)))


#
# ------------------ CLIENT EXTENSIONS ------------------
#
class MyTransport(Transport):

    def request(self, host, handler, request_body, verbose=0):
        try:
            return self.single_request(host, handler, request_body, verbose)

        finally:
            try:
                self.close()
            except:
                pass

#
# ------------------ THREADING EXTENSIONS ------------------
#
class ProcessingMixin(object):
    """Mix-in class to handle each request in a new thread."""

    def __init__(self, daemon=False, threaded=False, threadPool=None):
        self.daemon_threads = daemon

    def verify_request(self, request, client_address):
        #possible host-based authentication protection
        ip, port = client_address
        self.logger.debug("caller ip: %s:%d" % (ip, port))
        #if not (ip in self.authorized_hosts):
        #    return False
        return True

    def process_request(self, request, client_address):
        """Optional multithreaded request processing."""

        if self.threaded:
            self.queue.put((request, client_address))

        # Default behavior is single-threaded sequential execution
        else:
            self.do_process_request(request, client_address)

    def do_process_request(self, request, client_address):

        self.finish_request(request, client_address)
        self.shutdown_request(request)

        request.close()

#
# ------------------ XML-RPC SERVERS ------------------
#

class XMLRPCRequestHandler(SimpleXMLRPCRequestHandler):
    """Subclass SimpleXMLRPCRequestHandler to add basic HTTP authentication
    check.
    """
    # def __init__(self, *args, **kwdargs):
    #     SimpleXMLRPCRequestHandler.__init__(*args, **kwdargs)

    def setup(self):
        SimpleXMLRPCRequestHandler.setup(self)

        # *** NOTE: NO KEEP-ALIVE!!! ***
        # Keep-alive does not play well with multithreaded xml-rpc
        self.server.close_connection = True
        self.protocol_version = "HTTP/1.0"

    def get_authorization_creds(self):
        auth = self.headers.get("authorization", None)
        logger = self.server.logger

        #logger.debug("Auth is %s" % (str(auth)))
        if auth is not None:
            try:
                method, auth = auth.split()
                if method.lower() == 'basic':
                    #logger.debug("decoding base64...")
                    auth = base64.decodestring(auth.encode()).decode()
                    #logger.debug("splitting...")
                    username, password = auth.split(':')
                    logger.debug("username: '%s', password: '%s'" % (
                        username, password))
                    auth = { 'method': 'basic',
                             'username': username,
                             'password': password,
                             }
                else:
                    logger.error("unsupported auth method: '%s'" % method)
                    auth = None
            except Exception as e:
                logger.error("unrecognized auth cred: '%s'" % auth)
                auth = None

        return auth

    def _dispatch(self, method, params):
        """
        Called to dispatch an XML-RPC request.
        """
        auth = self.get_authorization_creds()

        # Refer back to server to do the dispatch
        return self.server.do_dispatch(method, params, auth,
                                       self.client_address)

# Using the ProcessingMixin allows the XML-RPC server to handle more than
# one request at a time.
#
class XMLRPCServer(ProcessingMixin, SimpleXMLRPCServer):
    """
    Basic XML-RPC server.
    """

    # Note: cert_file param is just for constructor compatibility with
    # SecureXMLRPCServer--it is ignored
    def __init__(self, host, port, ev_quit=None, timeout=socket_timeout,
                 logger=None,
                 requestHandler=XMLRPCRequestHandler,
                 logRequests=False, allow_none=True, encoding=None,
                 threaded=False, threadPool=None, numthreads=5,
                 authDict=None, cert_file=None):

        SimpleXMLRPCServer.__init__(self, (host, port),
                                    requestHandler=requestHandler,
                                    logRequests=logRequests,
                                    allow_none=allow_none, encoding=encoding,
                                    bind_and_activate=True)

        ProcessingMixin.__init__(self, threaded=threaded, threadPool=threadPool)

        if logger:
            self.logger = logger
        else:
            self.logger = logging.Logger('null')

        # Our termination flag
        if not ev_quit:
            self.ev_quit = threading.Event()
        else:
            self.ev_quit = ev_quit

        # Defines how responsive to termination we are
        self.timeout = timeout

        self.authDict = authDict
        self.threaded = threaded
        self.threadPool = threadPool
        self._num_threads = numthreads

        # Make XML-RPC sockets not block indefinitely
        #self.socket.settimeout(timeout)
        # Override anemic limit of python's default SocketServer
        self.socket.listen(64)

        # [Bug #1222790] If possible, set close-on-exec flag; if a
        # method spawns a subprocess, the subprocess shouldn't have
        # the listening socket open.
        if fcntl is not None and hasattr(fcntl, 'FD_CLOEXEC'):
            flags = fcntl.fcntl(self.fileno(), fcntl.F_GETFD)
            flags |= fcntl.FD_CLOEXEC
            fcntl.fcntl(self.fileno(), fcntl.F_SETFD, flags)

    def start(self, use_thread=True):
        # Threaded server.  Start N workers either as new threads or using
        # the thread pool, if we have one.
        if self.threaded:
            # queue for communicating with workers
            self.queue = Queue.Queue()

            for i in range(self._num_threads):
                if self.threadPool == None:
                    thread = threading.Thread(target=self.worker,
                                              name="RPC-Worker-%d" % (i+1),
                                              args=[i, self.queue])
                    thread.daemon = False
                    thread.start()
                else:
                    task = Task.FuncTask2(self.worker, i, self.queue)
                    self.threadPool.addTask(task)

        if not use_thread:
            self.serve_forever()

        else:
            if self.threadPool == None:
                thread = threading.Thread(target=self.serve_forever)
                thread.daemon=True
                thread.start()

            else:
                task = Task.FuncTask2(self.serve_forever)
                self.threadPool.addTask(task)

    def stop(self):
        # use another thread to call shutdown because if we happen to
        # be in the same thread as server_forever() it will deadlock
        thread = threading.Thread(target=self.shutdown)
        thread.start()

        self.ev_quit.set()
        #self.server_close()

    def worker(self, i, queue):

        self.logger.info("worker %d spinning up..." % (i))

        while not self.ev_quit.isSet():
            try:
                # NOTE: if timeout is specified, then performance
                # drops *substantially*.
                tup = queue.get(block=True, timeout=None)
                assert len(tup) == 2, \
                       Error("Invalid queue contents: len(tup) != 2 (%d)" % (
                                    len(tup)))
                request, client_address = tup
                if request == None:
                    # Put termination sentinal back on the queue for other
                    # workers to discover and terminate
                    queue.put(tup)
                    break

            except Queue.Empty:
                continue

            try:
                self.do_process_request(request, client_address)

            except Exception as e:
                self.logger.error("Error processing request: %s" % (str(e)))

        self.logger.info("worker %d shutting down..." % (i))

    def authenticate_client(self, methodName, params, auth, client_address):
        # this is the caller's ip address and port
        ip, port = client_address

        if not auth:
            self.logger.error("No authentication credentials passed")
            raise Error("Service requires authentication and no credentials passed")

        # this is the client authentication, pulled from the HTTP header
        try:
            username = auth['username']
            password = auth['password']

        except KeyError:
            self.logger.error("Bad authentication credentials passed")
            raise Error("Service only handles 'basic' authentication type")

        if not username in self.authDict:
            self.logger.error("No user matching '%s'" % username)
            self.logger.info("authdict is '%s'" % str(self.authDict))
            # sleep thwarts brute force attacks
            # but also delays applications when there is a legitimate
            # authentication mismatch
            #time.sleep(1.0)
            raise Error("Service requires authentication; username or password mismatch")

        if self.authDict[username] != password:
            self.logger.error("Password incorrect '%s'" % password)
            # sleep thwarts brute force attacks
            time.sleep(1.0)
            raise Error("Service requires authentication; username or password mismatch")

        self.logger.debug("Authorized client '%s'" % (username))

    def do_dispatch(self, methodName, params, auth, client_addr):

        try:
            if self.authDict:
                self.authenticate_client(methodName, params, auth, client_addr)

            # log all method calls, but truncate params to a reasonable size
            # in case a huge parameter(s) was sent
            self.logger.debug("calling method %s(%s)" % (str(methodName),
                                                         str(params)[:500]))
            response = SimpleXMLRPCDispatcher._dispatch(self, methodName,
                                                        params)
            #response = self.my_dispatch(methodName, params, auth, client_addr)
            self.logger.debug("response is: %s" % str(response))
            return response

        except Exception as e:
            self.logger.error("Method %s raised exception: %s" % (str(methodName),
                                                                  str(e)))
            try:
                (type, value, tb) = sys.exc_info()
                tb_str = ("Traceback:\n%s" % '\n'.join(traceback.format_tb(tb)))
                self.logger.error(tb_str)
            except:
                self.logger.error("Traceback information unavailable")
            raise e


def get_serverClass(secure=False):
    return XMLRPCServer


#END
