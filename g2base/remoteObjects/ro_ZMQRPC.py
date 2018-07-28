#
# ro_ZMQRPC.py -- ZeroMQ-based RPC services for remoteObjects system
#
# Eric Jeschke (eric@naoj.org)
#
"""
"""
from __future__ import print_function
import sys, os, time
import threading
from uuid import uuid4
import inspect
import logging
import zlib

import zmq
from zmq import ZMQError

from g2base.six.queue import queue as Queue
from g2base import six
from g2base import Task

from . import ro_codec

# Timeout value for ZMQ-RPC sockets
socket_timeout = 0.25

# RPC version we implement
rpc_version = '2.0'

TEMPDIR = '/tmp'

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
        self.compress = False
        self.rpc_version = rpc_version

    def encode(self):
        d = self.toDict()
        self.encoding = ro_codec.encoding
        msg = ro_codec.content_encode(d)
        if self.compress:
            msg = zlib.compress(msg)
        hdr = '%s,%s,%d' % (
            self.rpc_version, self.encoding, len(msg))
        return hdr, msg

    def decode(self, hdr, msg):
        items = hdr.split(',')
        if self.compress:
            msg = zlib.decompress(msg)
        self.encoding = ro_codec.encoding
        d = ro_codec.content_decode(msg)
        self.fromDict(d)

    def send(self, socket, flags=0):
        hdr, msg = self.encode()
        socket.send(hdr, flags=zmq.SNDMORE)
        return socket.send(msg, flags=flags)

    def recv(self, socket, flags=0):
        hdr = socket.recv(flags)
        #print("hdr: ", hdr)
        more = socket.getsockopt(zmq.RCVMORE)
        msg = ''
        if more:
            msg = socket.recv(flags)
        #print("msg: ", msg)
        self.decode(hdr, msg)

    def __repr__(self):
        return "<%s: status:%d>" % (self.__class__.__name__, self.status)

class RpcRequest(RpcMessage):
    """
    RpcRequest

    Represents a request message to be sent out to a host
    with published services. Used by RpcConnection when doing
    calls.
    """

    def __init__(self):
        super(RpcRequest, self).__init__()
        self.method = ""
        self.args = []
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
    Wraps around the result from the remote call.
    Used by splRpc when replying to a call from RpcConnection.
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


class ZMQRPCServer(object):
    """
    Basic ZMQ-RPC server.
    """

    def __init__(self, host, port, ev_quit=None,
                 logger=None, numthreads=5,
                 timeout=socket_timeout,
                 threaded=True, threadPool=None,
                 authDict=None, cert_file=None,
                 context=None, localOnly=False):

        self.host = host
        self.port = port
        self.timeout = 0.25
        self.authDict = authDict

        if ev_quit == None:
            ev_quit = threading.Event()
        self.ev_quit = ev_quit
        self.device_ready = threading.Event()

        if logger == None:
            logger = logging.getLogger("ZMQRPC")
        self.logger = logger
        self.useThread = threaded
        self.threadPool = threadPool

        self._context = context or zmq.Context.instance()

        # for local processing
        if localOnly:
            self._address = "ipc://%s/%d.ipc" % (TEMPDIR, port)
        else:
            host = '*'
            self._address = "tcp://%s:%d" % (host, port)

        self._services = {}

        n = bump_worker_group()
        self.worker_group = "wg%d" % (n)
        self._worker_url = "inproc://%s" % (self.worker_group)

        self._num_threads = max(int(numthreads), 1)

        self.receiver = None
        self.dealer = None

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

    def worker(self, idx):
        # Wait for server to be ready
        self.device_ready.wait()

        sock = self._context.socket(zmq.REP)
        sock.connect(self._worker_url)
        sock.linger = 0

        ## poller = zmq.Poller()
        ## poller.register(sock, zmq.POLLIN)

        self.logger.debug("Group %s: Worker %d ready to receive" % (
            self.worker_group, idx))

        while not self.ev_quit.isSet():

            try:
                # poll for a packet
                # NOTE: ZMQ pollers introduce a big performance hit!
                ## res = poller.poll(int(self.timeout*1000))
                ## conn = dict(res)
                ## if conn and conn.get(sock) == zmq.POLLIN:
                ##     req = RpcRequest()
                ##     req.recv(sock, flags=zmq.NOBLOCK)
                ##     self.logger.debug("recv: %s" % (str(conn)))
                ## else:
                ##     # no packet--iterate
                ##     continue
                req = RpcRequest()
                req.recv(sock)

                # handle request and send response
                resp = self.handle_request(req)
                resp.send(sock)
                self.logger.debug("sent response: %s" % resp)

            except ZMQError as e:
                if e.errno == zmq.ETERM and self.ev_quit.isSet():
                    break
                else:
                    sock.close()
                    raise

        self.logger.debug("Group %s: worker %d exiting..." % (
            self.worker_group, idx))
        sock.close()
        self.logger.debug("Group %s: worker %d done." % (
            self.worker_group, idx))

    def _worker2(self, req, _id, queue):
        # handle request and send response
        resp = self.handle_request(req)
        self.logger.debug("Response crafted, putting to queue")
        queue.put((_id, resp))

    def server1(self, queue):
        """
        Not to be called directly. Use start()
        """
        self.receiver = self._context.socket(zmq.ROUTER)
        self.receiver.linger = 0
        self.receiver.bind(self._address)
        self.logger.debug("Listening @ %s" % (self._address))

        self.dealer = self._context.socket(zmq.DEALER)
        self.dealer.linger = 0
        self.dealer.bind(self._worker_url)

        self.device_ready.set()
        try:
            # blocks
            ret = zmq.device(zmq.QUEUE, self.receiver, self.dealer)

        except ZMQError as e:
            self.logger.error("Server ZMQ error: %s" % (
                str(e)))
            # stop() generates a valid ETERM, otherwise its unexpected
            if not (e.errno == zmq.ETERM and self.ev_quit.set()):
                raise

        finally:
            self.logger.debug("server terminating ZMQ device...")
            self.stop()

        self.logger.debug("server exiting.")

    def server2(self, queue):
        frontend = self._context.socket(zmq.ROUTER)
        frontend.linger = 0
        frontend.bind(self._address)
        self.receiver = frontend
        self.logger.debug("Listening @ %s" % self._address)

        ## backend = self._context.socket(zmq.DEALER)
        ## backend.linger = 0
        ## backend.bind(self._worker_url)
        ## self.dealer = backend

        self.device_ready.set()

        # Initialize poll set
        poller = zmq.Poller()
        poller.register(frontend, zmq.POLLIN)

        # Switch messages between sockets
        try:
            while not self.ev_quit.isSet():
                socks = dict(poller.poll(1000))

                try:
                    if socks.get(frontend) == zmq.POLLIN:
                        # we have a request!
                        _id = frontend.recv()
                        _sep = frontend.recv()    # ZMQ address separator
                        #print("recv: ID is %s" % _id)
                        req = RpcRequest()
                        req.recv(frontend, flags=zmq.NOBLOCK)
                        self.logger.debug("recv: %s" % (str(req)))

                        if self.useThread:
                            if self.threadPool != None:
                                # hand it to a thread in our threadpool to handle it
                                self.logger.debug("handing off request to threadPool")
                                task = Task.FuncTask2(self._worker2, req, _id, queue)
                                self.threadPool.addTask(task)

                        else:
                            resp = self.handle_request(req)
                            queue.put((_id, resp))

                    # check queue for any results coming back to be sent out
                    have_responses = True
                    while have_responses:
                        try:
                            #print("checking responses")
                            (_id, resp) = queue.get(block=False)
                            #print("got response")
                            self.logger.debug("Sending response...")
                            frontend.send(_id, zmq.SNDMORE)
                            frontend.send('', zmq.SNDMORE)  # separator
                            resp.send(frontend, flags=zmq.NOBLOCK)
                            self.logger.debug("sent response: %s" % resp)

                        except Queue.Empty:
                            #print("no responses")
                            have_responses = False

                    time.sleep(0)

                except ZMQError as e:
                    self.logger.error("Server ZMQ error: %s" % (
                        str(e)))
                    # stop() generates a valid ETERM, otherwise its unexpected
                    if not (e.errno == zmq.ETERM and self.ev_quit.set()):
                        raise

                except Exception as e:
                    self.logger.error("Server fatal error: %s" % (str(e)))
                    break
        finally:
            self.logger.debug("server terminating ZMQ device...")
            self.stop()

        self.logger.debug("server exiting.")

    def server3(self, queue):
        backend = self._context.socket(zmq.DEALER)
        backend.linger = 0
        self.dealer = backend

        # Switch messages between sockets
        try:
            while not self.ev_quit.isSet():
                try:
                    # check queue for any results coming back to be sent out
                    have_responses = True
                    while have_responses:
                        try:
                            print("checking responses")
                            (_id, resp) = queue.get(block=True, timeout=1.0)
                            print("got response")
                            self.logger.debug("Sending response...")
                            backend.send(_id, zmq.SNDMORE)
                            backend.send('', zmq.SNDMORE)  # separator
                            resp.send(backend, flags=zmq.NOBLOCK)
                            self.logger.debug("sent response: %s" % resp)

                        except Queue.Empty:
                            print("no responses")
                            have_responses = False

                except ZMQError as e:
                    self.logger.error("Server ZMQ error: %s" % (
                        str(e)))
                    # stop() generates a valid ETERM, otherwise its unexpected
                    if not (e.errno == zmq.ETERM and self.ev_quit.set()):
                        raise

                except Exception as e:
                    self.logger.error("Server fatal error: %s" % (str(e)))
                    break
        finally:
            self.logger.debug("server terminating ZMQ device...")
            self.stop()

        self.logger.debug("server exiting.")


    def server(self, queue):
        return self.server3(queue)

    def start(self, useThread=False):
        """
        start()

        Start the RPC server.
        """
        if self.receiver is not None:
            raise RuntimeError("Cannot start RPC Server more than once.")

        self.logger.debug("Starting RPC thread loop w/ %d worker(s)" % (
            self._num_threads))

        if not self.useThread:
            for i in range(self._num_threads):
                if self.threadPool == None:
                    thread = threading.Thread(target=self.worker,
                                              name="RPC-Worker-%d" % (i+1),
                                              args=[i])
                    thread.daemon = True
                    thread.start()

                else:
                    task = Task.FuncTask2(self.worker, i)
                    self.threadPool.addTask(task)

        # Start the server
        if self.threadPool == None:
            thread = threading.Thread(target=self.server,
                                      name="RPC-Device",
                                      args=[queue])
            thread.daemon = True
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

        if self.dealer:
            self.dealer.close()

        self._context.term()


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

        self._services[name] = {'format' : "%s(%s)" % (name, ', '.join(spec)), 'method' : method, 'doc' : method.__doc__}

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

        self._context = context or zmq.Context.instance()
        if localOnly:
            self._address = "ipc://%s/%d.ipc" % (TEMPDIR, port)
        else:
            self._address = "tcp://%s:%d" % (host, port)
        self.auth = auth
        self._sender = None
        self._poller = None
        self.timeout = 1000.0
        self.retries = 3
        self._lock = threading.RLock()

        if logger == None:
            logger = logging.getLogger("ZMQRPC")
        self.logger = logger

    def __del__(self):
        self.logger.debug("closing connection")
        self.close()

    def close(self):
        """
        Close the client connection.
        """
        try:
            if self._sender != None:
                self._sender.close()
        except:
            pass
        finally:
            self._sender = None

    def availableServices(self):
        """
        availableServices() -> RpcResponse

        Asks the remote server to tell us what services
        are published.
        Returns an RpcResponse object
        """
        return self.call("__services__")


    def reset(self):
        self.close()
        #self._lock = threading.RLock()

        self._sender = self._context.socket(zmq.REQ)
        # self._sender = self._context.socket(zmq.DEALER)
        # self._sender.setsockopt(zmq.IDENTITY, "FOO" )
        self._sender.setsockopt(zmq.LINGER, 0)
        self.logger.debug("connecting to %s..." % self._address)
        self._sender.connect(self._address)

        self._poller = zmq.Poller()
        self._poller.register(self._sender, zmq.POLLIN)


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

        req = RpcRequest()
        req.method = method
        req.args   = args
        req.kwdargs = kwdargs
        req.auth = self.auth
        req.isasync  = isasync

        if self._sender == None:
            self.reset()

        retries = self.retries
        got_response = False

        # Implements the "lazy pirate pattern" for reliability as described
        # here: http://zguide.zeromq.org/py:lpclient
        with self._lock:
            while retries > 0:
                retries -= 1
                self.logger.debug("Calling RPC server at: %s" % self._address)
                time_start = time.time()
                req.send(self._sender, flags=zmq.NOBLOCK)

                self.logger.debug("client sent. waiting for reply...")
                res = self._poller.poll(int(self.timeout*1000))
                conn = dict(res)
                if conn and conn.get(self._sender, None) == zmq.POLLIN:
                    resp = RpcResponse()
                    resp.recv(self._sender, flags=zmq.NOBLOCK)
                    got_response = True
                    break

                    elapsed = time.time() - time_start
                    self.logger.debug("reply in %.6f sec" % (elapsed))
                    #self.logger.debug("Got reply to method %s: %s" % (
                    #    method, resp))
                else:
                    errmsg = ("Got no reply within %.3f time limit" % (
                        self.timeout))
                    self.logger.error(errmsg)
                    self.logger.warn("Resetting client...")
                    self.reset()

        if not got_response:
            raise socketTimeout(errmsg)

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
            raise Error("Currently no secure socket layer for ZMQ-RPC")

        proxy = ClientProxy(host, port, auth, allow_none=True)

        return ServiceProxy(proxy)

    except Exception as e:
        raise Error("Can't create proxy to service found on '%s:%d': %s" % (
            host, port, str(e)))


def get_serverClass(secure=False):
    if secure:
        raise Error("Currently no secure socket layer for ZMQ-RPC")
    return ZMQRPCServer

#END
