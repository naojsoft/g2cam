#
# ro_socket.py -- socket-based RPC services for remoteObjects system
#
import sys, os, time
import threading
from uuid import uuid4
import inspect
import logging
import zlib
import socket
import select
import Queue

from g2base import Task
from . import ro_codec
import six

# Timeout value for RPC sockets
socket_timeout = 0.25

# RPC version we implement
rpc_version = '2.0'

# header size
rpc_hdr_len = 64

# read chunk size
read_chunk_size = 4*1024

# number of allowed queued connections before refusing any
default_num_connect = 5


class Error(Exception):
    """Class of errors raised in this module."""
    pass

class socketTimeout(Error):
    """Raised when a socket times out waiting for a client."""
    pass


worker_group = 0

def bump_worker_group():
    global worker_group
    worker_group += 1
    return worker_group


class RpcMessage(object):

    def __init__(self):
        super(RpcMessage, self).__init__()

        self.encoding = ro_codec.encoding
        self.compression = 'none'
        self.address = None
        self.rpc_version = rpc_version

    def encode(self):
        d = self.toDict()
        encode = ro_codec.get_encoder(self.encoding)
        msg = encode(d)
        if self.compression == 'zlib':
            msg = zlib.compress(msg)
        hdr = '%s,%s,%s,%d' % (
            self.rpc_version, self.encoding,
            self.compression, len(msg))
        # pad header to required size
        hdr += ' ' * (rpc_hdr_len - len(hdr))
        assert len(hdr) == rpc_hdr_len, \
               Error("RPC header len actual(%d) != expected(%d)" % (
                        len(hdr), rpc_hdr_len))
        return hdr, msg

    def decode(self, hdr, msg):
        hdr = hdr.strip()
        items = hdr.split(',')
        self.compression = items[2]
        if self.compression == 'zlib':
            msg = zlib.decompress(msg)
        self.encoding = items[1]
        decode = ro_codec.get_decoder(self.encoding)
        d = decode(msg)
        self.fromDict(d)

    def send(self, sock, flags=0):
        hdr, msg = self.encode()
        pkt = hdr + msg
        num_bytes = len(pkt)
        size = num_bytes
        while size > 0:
            #num_sent = sock.sendto(pkt, self.address)
            num_sent = sock.send(pkt)
            size -= num_sent
            pkt = pkt[num_sent:]
        #print "sent %d bytes" % (num_bytes)

    def recv(self, sock, flags=0):
        #print "reading hdr"
        #msg, address = sock.recvfrom(read_chunk_size)
        msg = sock.recv(read_chunk_size)
        if len(msg) < rpc_hdr_len:
            # we didn't receive all of the header--read the rest
            pieces = [ msg ]
            size_rem = rpc_hdr_len - len(msg)
            while size_rem > 0:
                chunk_size = min(size_rem, read_chunk_size)
                pieces.append(sock.recv(chunk_size))
                size_rem -= len(pieces[-1])
            msg = ''.join(pieces)
        hdr = msg[:rpc_hdr_len]
        #print "hdr: ", hdr
        assert len(hdr) == rpc_hdr_len, \
               Error("RPC header len actual(%d) != expected(%d)" % (
                        len(hdr), rpc_hdr_len))

        tup = hdr.strip().split(',')
        assert len(tup) == 4, \
               Error("RPC header: num fields(%d) != expected(%d) [hdr:%s]" % (
                        len(tup), 4, hdr))
        ver, enc, comp, body_size = tup
        body_size = int(body_size)
        msg = msg[rpc_hdr_len:]
        if len(msg) < body_size:
            # we didn't receive all of the body--read the rest
            pieces = [ msg ]
            size_rem = body_size - len(msg)
            while size_rem > 0:
                chunk_size = min(size_rem, read_chunk_size)
                pieces.append(sock.recv(chunk_size))
                size_rem -= len(pieces[-1])
            msg = ''.join(pieces)
        assert len(msg) == body_size, \
               Error("RPC body len actual(%d) != expected(%d)" % (
                        len(msg), body_size))
        self.decode(hdr, msg)

    def __repr__(self):
        return "<%s>" % (self.__class__.__name__)

class RpcRequest(RpcMessage):
    """
    RpcRequest

    Represents a request message to be sent out to a host
    with published services.
    """

    def __init__(self):
        super(RpcRequest, self).__init__()
        self.method = ""
        self.args   = []
        self.kwdargs = {}
        self.auth = []
        self.callback = False
        self.isasync = False
        self.callback_id = uuid4().int

    def toDict(self):
        return {
            'method': self.method,
            'args': self.args,
            'kwdargs': self.kwdargs,
            'auth': self.auth,
            'id': self.callback_id
            }

    def fromDict(self, d):
        self.method = d['method']
        self.args = d['args']
        self.kwdargs = d['kwdargs']
        self.auth = d['auth']
        self.callback_id = d['id']

    def __repr__(self):
        return "<%s: %s (#args:%d, #kwdargs:%d)>" % (self.__class__.__name__,
                                                      self.method,
                                                      len(self.args),
                                                      len(self.kwdargs))

    @property
    def id(self):
        return self.callback_id

class RpcResponse(RpcMessage):
    """
    RpcResponse

    Represents a response message from a remote call.
    """

    def __init__(self, result=None, status=-1, error=None):
        super(RpcResponse, self).__init__()
        self.status = status
        self.result = result
        self.error  = error

    def toDict(self):
        return {
            'status': self.status,
            'result': self.result,
            'error': self.error,
            }

    def fromDict(self, d):
        self.status = d['status']
        self.result = d['result']
        self.error = d['error']

    def __repr__(self):
        return "<%s: status:%d>" % (self.__class__.__name__, self.status)


class SocketRPCServer(object):
    """
    Basic Socket-RPC server.
    """

    def __init__(self, host, port, ev_quit=None,
                 logger=None, numthreads=5,
                 timeout=socket_timeout,
                 num_connect=default_num_connect,
                 threaded=True, threadPool=None,
                 authDict=None, cert_file=None,
                 context=None, localOnly=False):

        self.host = host
        self.port = port
        self.timeout = 5.0
        self.num_connect = num_connect

        self.authDict = authDict
        # to communicate between server and workers
        self.queue = Queue.Queue()

        if ev_quit == None:
            ev_quit = threading.Event()
        self.ev_quit = ev_quit

        if logger == None:
            logger = logging.getLogger("RPCServer")
        self.logger = logger
        self.threaded = threaded
        self.threadPool = threadPool

        host = ''
        self._address = (host, port)

        self._services = {}

        n = bump_worker_group()
        self.worker_group = "wg%d" % (n)

        self._num_threads = max(int(numthreads), 1)

        self.receiver = None

    def check_auth(self, req):
        """
        Check authentication of RPC request.  req.auth should be a tuple
        of (username, password).  Returns True if authentication succeeded,
        False otherwise.
        """
        if self.authDict == None:
            return True

        if len(req.auth) != 2:
            return False
        username, password = req.auth

        if username not in self.authDict:
            return False

        # TODO: handle encrypted passwords
        if self.authDict[username] == password:
            return True

        return False


    def handle_request(self, req):
        resp = RpcResponse()
        resp.address = req.address
        # match client's encoding
        resp.encoding = req.encoding

        self.logger.debug("recv: %s(%s, %s)" % (
            req.method, req.args, req.kwdargs))

        if req.method == "__services__":
            service = {'method' : self._serviceListReq}
        else:
            service = self._services.get(req.method, None)

        # authenticate packet
        ok = self.check_auth(req)
        if not ok:
            resp.result = None
            resp.status = -1
            resp.error = "Authentication error"

        elif service == None:
            resp.result = None
            resp.status = -1
            resp.error = "Non-existant service: %s" % req.method

        else:
            try:
                resp.result = service['method'](*req.args, **req.kwdargs)
                resp.status = 0

            except Exception as e:
                resp.status = 1
                resp.result = None
                resp.error = str(e)

        return resp


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
                sock, req = tup
                if sock == None:
                    # Termination sentinal
                    queue.put(tup)
                    break

            except Queue.Empty:
                continue

            try:
                # read request
                #self.logger.info("reading request...")
                req.recv(sock)

                # handle request and send response
                #self.logger.info("handling request...")
                resp = self.handle_request(req)

                #self.logger.info("sending reply...")
                resp.send(sock)
                #self.logger.debug("sent response: %s" % resp)

            except Exception as e:
                self.logger.error("Worker error: %s" % (
                    str(e)))

            finally:
                try:
                    sock.close()
                except:
                    pass

        self.logger.info("worker %d shutting down..." % (i))

    def server(self, queue):

        # Our receiving socket
        srvsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srvsock.settimeout(self.timeout)
        srvsock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srvsock.bind(self._address)
        srvsock.listen(self.num_connect)
        self.receiver = srvsock
        self.logger.debug("Listening @ %s" % str(self._address))

        socklist = [ srvsock ]

        self.logger.info("server starting request loop...")
        # Request processing loop
        try:
            while not self.ev_quit.isSet():
                try:
                    rlist, wlist, elist = select.select(socklist, [], [],
                                                        1.0)
                    #print rlist, wlist, elist
                    if len(rlist) == 0:
                        continue

                    # Get client connection
                    clisock, cliaddr = srvsock.accept()
                    clisock.settimeout(self.timeout)

                    req = RpcRequest()
                    req.address = cliaddr

                    if self.threaded:
                        # If we are a threaded server, then hand this request
                        # off to a thread via the queue
                        queue.put((clisock, req))

                    else:
                        # otherwise handle the request ourself
                        req.recv(clisock)
                        resp = self.handle_request(req)
                        resp.send(clisock)

                except socket.timeout:
                    self.logger.error("Timed out (>%.2f) reading socket" % (
                        self.timeout))

                except Exception as e:
                    self.logger.error("Server socket error: %s" % (
                        str(e)))

        finally:
            self.logger.debug("server terminating request loop...")
            # special flag to terminate workers
            queue.put((None, None))
            self.stop()

        self.logger.info("server exiting.")


    def start(self, use_thread=True):
        """
        start()

        Start the RPC server.
        """
        if self.receiver is not None:
            raise RuntimeError("Cannot start RPC Server more than once.")

        self.logger.debug("Starting RPC thread loop w/ %d worker(s)" % (
            self._num_threads))

        # queue for communicating with workers
        queue = Queue.Queue()

        # Threaded server.  Start N workers either as new threads or using
        # the thread pool, if we have one.
        if self.threaded:
            for i in range(self._num_threads):
                if self.threadPool == None:
                    thread = threading.Thread(target=self.worker,
                                              name="RPC-Worker-%d" % (i+1),
                                              args=[i, queue])
                    thread.daemon = False
                    thread.start()
                else:
                    task = Task.FuncTask2(self.worker, i, queue)
                    self.threadPool.addTask(task)

        # Start the server
        if not use_thread:
            self.server(queue)

        else:
            if self.threadPool == None:
                thread = threading.Thread(target=self.server,
                                          name="RPC-Device",
                                          args=[queue])
                thread.daemon = False
                thread.start()

            else:
                task = Task.FuncTask2(self.server, queue)
                self.threadPool.addTask(task)


    def stop(self):
        """
        stop()

        Stop the server thread process
        """
        self.logger.debug("Stop request made for RPC thread loop")
        self.ev_quit.set()

        if self.receiver:
            self.receiver.close()
            self.receiver = None

    def server_close(self):
        self.stop()

    def register_instance(self, obj):
        for name in dir(obj):
            if not name.startswith('_'):
                method = getattr(obj, name)
                self.register_function(name)

    def register_function(self, method):
        self.publishService(method)

    def publishService(self, method):
        """
        publishService (object method)

        Publishes the given callable, to be made available to
        remote procedure calls.
        """
        name = method.__name__
        argSpec = inspect.getargspec(method)
        spec = argSpec.args
        if argSpec.varargs:
            spec.append("*%s" % argSpec.varargs)
        if argSpec.keywords:
            spec.append("**%s" % argSpec.keywords)

        self._services[name] = {
            'format' : "%s(%s)" % (name, ', '.join(spec)),
            'method' : method,
            'doc' : method.__doc__
            }

    def _serviceListReq(self):
        services = []
        for name, v in six.iteritems(self.services):
            services.append({'service' : name, 'format' : v['format'], 'doc' : v['doc']})
        return services

    def services(self):
        return self._services

class ClientProxy(object):
    """
        rpc.call("myFunction", callback=processResponse, args=(1, "a"), kwdargs={'flag':True, 'option':"blarg"})
    """

    def __init__(self, host, port, auth, logger=None, allow_none=False,
                 context=None, localOnly=False):

        self._address = (host, port)
        self.auth = auth
        self._sender = None
        self.timeout = 5.0

        if logger == None:
            logger = logging.getLogger("SocketRPC")
        self.logger = logger
        self._lock = threading.RLock()
        #print "proxy made"

    def __del__(self):
        self.logger.debug("closing connection")
        self.close()

    def close(self):
        """
        Close the client connection.
        """
        if self._sender != None:
            self._sender.close()

    def availableServices(self):
        """
        availableServices() -> RpcResponse

        Asks the remote server to tell us what services
        are published.
        Returns an RpcResponse object
        """
        return self.call("__services__")


    def reset(self):
        # TODO: release all callers waiting on old lock?
        #print "resetting"
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5.0)
        self._sender = sock
        #print "reset complete"


    def call(self, method, callback=None, isasync=False, args=[], kwdargs={}):
        """
        call(str method, object callback=None, bool isasync=False, list args=[], dict kwdargs={})

        Make a remote call to the given service name, with an optional callback
        and arguments to be used.
        Can be run either blocking or asynchronously.

        By default, this method blocks until a response is received.
        If isasync=True, returns immediately with an empty RpcResponse. If a callback
        was provided, it will be executed when the remote call returns a result.
        """

        #print "req made"
        req = RpcRequest()
        req.method = method
        req.args   = args
        req.kwdargs = kwdargs
        req.auth = self.auth
        req.isasync = isasync
        req.address = self._address

        self.logger.debug("Sending a RPC call to method: %s" % method)
        # we are running this as a blocking call
        if self._sender == None:
            self.reset()

        with self._lock:
            self.logger.debug("Calling RPC server at: %s" % (
                str(self._address)))
            time_start = time.time()
            try:
                try:
                    self.reset()
                    self._sender.connect(self._address)

                    req.send(self._sender)
                    #print "request sent"
                except Exception as e:
                    #print "Error sending request: %s" % (str(e))
                    self.logger.error("Error sending request: %s" % (
                        str(e)))

                #print("client sent. waiting for reply...")
                self.logger.debug("client sent. waiting for reply...")
                rlist, wlist, elist = select.select([self._sender], [], [],
                                                    self.timeout)
                if len(rlist) == 0:
                    errmsg = ("Got no reply within %.3f time limit" % (
                        self.timeout))
                    self.logger.error(errmsg)
                    #print "no reply"
                    raise socketTimeout(errmsg)

                #print "got reply"
                resp = RpcResponse()
                resp.recv(self._sender)
                #print("got response...")

            finally:
                self._sender.close()

            elapsed = time.time() - time_start
            self.logger.debug("reply in %.6f sec" % (elapsed))

        result = resp
        return result


#
# ------------------ CONVENIENCE FUNCTIONS ------------------
#

class ServiceProxy(object):

    def __init__(self, proxy):
        self.proxy = proxy

    def call(self, attrname, args, kwdargs):
        res = self.proxy.call(attrname, args=args, kwdargs=kwdargs)

        if res.status == -1:
            raise Error("Method '%s' does not exist at service" % (
                attrname))

        if res.status != 0:
            raise Error("Remote exception: %s" % (
                res.error))

        return res.result


def make_serviceProxy(host, port, auth=False, secure=False, timeout=None):
    """
    Convenience function to make a XML-RPC service proxy.
    'auth' is None for no authentication, otherwise (user, passwd)
    'secure' should be True if you want to use SSL, otherwise vanilla http.
    """
    try:
        if secure:
            raise Error("Currently no secure socket layer for SocketRPC")

        proxy = ClientProxy(host, port, auth, allow_none=True)

        return ServiceProxy(proxy)

    except Exception as e:
        raise Error("Can't create proxy to service found on '%s:%d': %s" % (
            host, port, str(e)))


def get_serverClass(secure=False):
    if secure:
        raise Error("Currently no secure socket layer for SocketRPC")
    return SocketRPCServer


#END
