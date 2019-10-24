#
# ro_generic.py -- generic transport class for remoteObjects system
#
import logging
import threading
import uuid
import time

from g2base import Task
from g2base import six
if six.PY2:
    import Queue
else:
    import queue as Queue

from ..exceptions import (RemoteObjectsError, TimeoutError,
                          AuthenticationError, ServiceUnavailableError)

from ..secure.sec_fernet import Secure

sec_key = b'Qfc4c-ATOKFYMd5nCu6MgfdRwLDFG1_aPbIpyak4T7s='
#
# ------------------ SERVER ------------------
#

class GenericRPCServer(object):
    """
    Generic RPC server.

    An implementation must provide these methods.
    """

    def __init__(self, name, host, port,
                 ev_quit=None, timeout=0.25,
                 logger=None, encoding=None, secure=False,
                 threaded=False, threadPool=None, numthreads=5,
                 authDict=None, **kwargs):
        """Most parameters for backwards constructor compatibility;
        implementations are free to ignore ones they don't need.
        """
        self.name = name
        self.host = host
        self.port = port

        if logger:
            self.logger = logger
        else:
            self.logger = logging.Logger('null')

        # Our termination flag
        if not ev_quit:
            self.ev_quit = threading.Event()
        else:
            self.ev_quit = ev_quit

        # multithreaded server support
        self.threaded = threaded
        self.threadPool = threadPool
        self._num_threads = numthreads
        self.timeout = timeout
        self.inbox = None

        self.auth_dict = authDict
        self._packer = encoding

        self.secure = secure
        self.crypt = None
        if self.secure:
            self.crypt = Secure(key=sec_key)

        # dictionary of names to methods we can call
        self.methods = {}

    def register_function(self, method):
        """Register that the `method` can be called from this server."""
        self.methods[method.__name__] = method

    def register_method(self, name, method):
        """Register that the `method` can be called from this server,
        via `name`."""
        self.methods[name] = method

    def authenticate(self, request):
        # raise an exception if request does not pass authentication
        if self.auth_dict is None:
            # no authentication enabled
            return

        if 'username' not in request or 'password' not in request:
            errmsg = "server requires authentication and no credentials in request"
            self.logger.error(errmsg)
            raise AuthenticationError(errmsg)

        user = request['username']
        if user not in self.auth_dict:
            self.logger.error("user '{}' not in authorization dict".format(user))
            raise AuthenticationError("authentication error for user or password")

        if request['password'] != self.auth_dict[user]:
            self.logger.error("password mismatch req: '{}' auth: '{}'".format(request['password'], self.auth_dict[user]))
            raise AuthenticationError("authentication error for user or password")


    def serve_forever(self):
        raise RemoteObjectsError("subclass should override this method!")

    def worker(self, i, queue):
        self.logger.info("worker %d spinning up..." % (i))

        while not self.ev_quit.is_set():
            try:
                # NOTE: if timeout is specified, then performance
                # drops *substantially*.
                thunk = queue.get(block=True, timeout=None)

                if thunk is None:
                    # Termination sentinal
                    queue.put(thunk)
                    break

            except Queue.Empty:
                continue

            try:
                thunk()

            except Exception as e:
                self.logger.error("Worker error: {}".format(e), exc_info=True)

        self.logger.info("worker %d shutting down..." % (i))

    def start(self, use_thread=False):
        # Threaded server.  Start N workers either as new threads or using
        # the thread pool, if we have one.
        if self.threaded:
            # queue for communicating with workers
            self.inbox = Queue.Queue()

            for i in range(self._num_threads):
                if self.threadPool == None:
                    thread = threading.Thread(target=self.worker,
                                              name="RPC-Worker-%d" % (i+1),
                                              args=[i, self.inbox])
                    thread.daemon = False
                    thread.start()
                else:
                    task = Task.FuncTask2(self.worker, i, self.inbox)
                    self.threadPool.addTask(task)

        # Start serving thread
        if not use_thread:
            self.serve_forever()

        else:
            if self.threadPool is None:
                thread = threading.Thread(target=self.serve_forever)
                #thread.daemon = True
                thread.start()

            else:
                task = Task.FuncTask2(self.serve_forever)
                self.threadPool.addTask(task)

    def stop(self):
        """Stop all threads and workers."""
        if self.inbox is not None:
            # sentinal quit value for workers
            self.inbox.put(None)
        self.ev_quit.set()

#
# ------------------ CLIENT ------------------
#

class GenericRPCClient(object):
    """Implements a proxy object for the remote object.  The class name
    is not important.  It has a single method for calling the remote
    object: `call`().
    """

    def __init__(self, name, packer, logger=None,
                 auth=None, secure=None):
        self.name = name
        self.auth = auth
        self.secure = secure
        self.logger = logger

        self.packer = packer
        if self.secure:
            self.crypt = Secure(key=sec_key)

    def call(self, method, args, kwargs, timeout=None):
        """Make an RPC call to the remote object.

        Parameters
        ----------
        method : str
            Name of the method to be called on the remote object

        args : list or tuple
            Positional parameters to the method

        kwargs : dict
            Optional keyword parameters to the method (if supported)

        Returns
        -------
        res : basic type
            unmarshalled result of the call

        Raises
        ------
        `NameError` : if no such method exists on the remote side
        `FailoverError`: if the service does not seem to exist
        `Exception`: other exception on the remote side
        """
        #print('in call')

        req = { 'method': method,
                'args': args,
                'kwargs': kwargs,
                'id': str(uuid.uuid4()),
                'time_req': time.time(),
                }

        #print('auth is', self.auth)
        if self.auth is not None:
            req['username'], req['password'] = self.auth

        #print('req', req)

        # pack up request
        req_pkt = self.packer.pack(req)

        if self.secure:
            req_pkt = self.crypt.encrypt(req_pkt)

        rep_pkt = self.rpc_call(req, req_pkt, timeout=timeout)

        if self.secure:
            rep_pkt = self.crypt.decrypt(rep_pkt)

        #print('unpacking, rep_pkt=', rep_pkt)
        reply = self.packer.unpack(rep_pkt)

        if reply['id'] != req['id']:
            raise RemoteObjectsError("reply id ({}) does not match request ({})".format(reply['id'], req['id']))

        #print('reply', reply)
        if reply['status_code'] != 0:
            raise RemoteObjectsError(reply['error_msg'])

        elapsed = time.time() - req['time_req']
        #print("result (%s) in %f sec" % (reply['result'], elapsed))
        return reply['result']

#
# ------------------ MODULE FUNCTIONS ------------------
#

def get_serverClass(secure=False):
    """Returns the class of the server for this transport. `secure` is
    True if the class should support a secure transport.
    """
    return GenericRPCServer


def make_serviceProxy(name, host, port, auth=None, encoding=None,
                      secure=False, timeout=None):
    """
    Make a service proxy (client) for the remote object service.

    Parameters
    ----------
    name : str
        name of the service

    host : str
        hostname where the service exists

    port : int
        port where the service exists

    auth : tuple of (username, password) or None
        tuple of authentication credentials or None for no auth

    encoding : str
        name of the packer to be used for making buffers

    secure : bool
        True if the connection should be secured, False otherwise

    timeout : float
        timeout in seconds that should generate a TimeoutError if the
        call does not complete in that amount of time.
    """
    if secure:
        raise Error("No secure implementation for this transport")

    # If successful, return an object that has an attribute called
    # "proxy". NOTE THAT: service proxies do not require that the service be
    # available when they are created!
    return GenericRPCClient()

    # In the rare circumstance that a proxy can't be constructed due
    # to a missing package, bad configuration or ??
    raise ValueError("Can't create proxy to service '%s' found on '%s:%d': %s" % (
        name, host, port, str(e)))

#END
