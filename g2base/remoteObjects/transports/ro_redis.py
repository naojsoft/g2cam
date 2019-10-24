#
# ro_redis.py -- redis backend for remoteObjects system
#
import redis
import uuid
import time
import threading
import math

from g2base import Task
from g2base import six
if six.PY2:
    import Queue
else:
    import queue as Queue

# exceptions that may be raised in this module
from ..exceptions import (RemoteObjectsError, TimeoutError,
                          AuthenticationError, ServiceUnavailableError)
from ..ro_packer import get_packer
from .ro_generic import GenericRPCServer, GenericRPCClient

# TODO: how to determine hub address?
redis_url = "redis://localhost:6379"

#
# ------------------ SERVER ------------------
#

class RedisSimpleRPCServer(GenericRPCServer):
    """
    Redis RPC server.
    """

    def __init__(self, name, host, port, **kwargs):
        """Most parameters for backwards constructor compatibility;
        implementations are free to ignore ones they don't need.
        """
        super(RedisSimpleRPCServer, self).__init__(name, host, port, **kwargs)

        # See: make_serviceProxy()
        host = '_'
        self.list_name = "{}:{}:{}".format(name, host, port)
        self.client = None
        self.retry_interval = 10.0

        if self._packer is None:
            self._packer = 'msgpack'
        self.packer = get_packer(self._packer)

        # client seems to be lazy and doesn't try to connect to server
        # until you attempt an operation
        self.connect()

    def connect(self):
        self.client = redis.from_url(redis_url)

    def serve_forever(self):
        self.logger.info("starting redis RPC server for " + self.list_name)
        while not self.ev_quit.is_set():
            try:
                res = self.client.brpop(self.list_name, timeout=1)
                if res is None:
                    # timeout
                    continue
                channel, req_pkt = res

            except redis.exceptions.ConnectionError as e:
                self.logger.error("redis communication error: {}".format(e))
                self.logger.error("retrying in {} seconds ...".format(self.retry_interval))
                self.ev_quit.wait(self.retry_interval)
                continue

            except Exception as e:
                errmsg = "error receiving RPC request: {}".format(e)
                self.logger.error(errmsg, exc_info=True)
                continue

            if not self.threaded:
                self.process_packet(req_pkt)

            else:
                thunk = (lambda x: (lambda: self.process_packet(x)))(req_pkt)
                self.inbox.put(thunk)

    def send_reply(self, request, reply_pkt):
        try:
            self.client.lpush(request['id'], reply_pkt)
            # What if client doesn't read this before expiry?
            self.client.expire(request['id'], 30)

        except Exception as e:
            errmsg = "error making RPC reply: {}".format(e)
            self.logger.error("request was: {}".format(str(request)),
                              exc_info=True)

    def process_packet(self, req_pkt):

        try:
            if self.secure:
                self.logger.debug("decrypting secure packet")
                req_pkt = self.crypt.decrypt(req_pkt)

            request = self.packer.unpack(req_pkt)

        except Exception as e:
            self.logger.error("error unpacking RPC request: {}".format(e),
                              exc_info=True)
            return

        self.logger.debug("request is: {}".format(str(request)))

        reply = dict(id=request['id'], status_code=0)

        if self.auth_dict is not None:
            try:
                self.authenticate(request)

            except Exception as e:
                errmsg = "authentication error: {}".format(e)
                self.logger.error(errmsg)
                reply['status_code'] = 3
                reply['error_msg'] = errmsg

        if reply['status_code'] == 0:
            method = self.methods.get(request['method'], None)
            if method is None:
                errmsg = "method '{}' not at this service".format(request['method'])
                self.logger.error(errmsg)
                reply['status_code'] = 1
                reply['error_msg'] = errmsg

            else:
                try:
                    result = method(*request['args'], **request['kwargs'])
                    reply['status_code'] = 0
                    reply['result'] = result

                except Exception as e:
                    errmsg = "error invoking method '{}': {}".format(request['method'], e)
                    self.logger.error(errmsg, exc_info=True)
                    reply['status_code'] = 2
                    reply['error_msg'] = errmsg

        if reply['status_code'] != 0:
            # log request for debugging errors
            self.logger.info("errored request was: {}".format(str(request)))

        reply['time_rep'] = time.time()

        try:
            rep_pkt = self.packer.pack(reply)

            if self.secure:
                self.logger.debug("encrypting packet")
                rep_pkt = self.crypt.encrypt(rep_pkt)

        except Exception as e:
            self.logger.error("error packing RPC reply: {}".format(e),
                              exc_info=True)
            return

        self.send_reply(request, rep_pkt)

#
# ------------------ CLIENT ------------------
#

class RedisSimpleRPCClient(GenericRPCClient):

    def __init__(self, redis_url, list_name, name, packer,
                 **kwargs):
        super(RedisSimpleRPCClient, self).__init__(name, packer, **kwargs)

        self.redis_url = redis_url
        self.list_name = list_name
        self.client = None

    def connect(self):
        self.client = redis.from_url(redis_url)

    def rpc_call(self, req, req_pkt, timeout=None):

        if timeout is not None:
            # redis only allows integer timeouts
            timeout = int(math.ceil(timeout))

        if self.client is None:
            try:
                #print('connect')
                self.connect()

            except Exception as e:
                self.client = None
                if self.logger is not None:
                    self.logger.error("client connect error: {}".format(e),
                                      exc_info=True)
                raise ServiceUnavailableError("Error creating redis client: {}".format(e))

        res = None
        try:
            #print("calling server: req: {}".format(req))
            self.client.lpush(self.list_name, req_pkt)

            # wait for reply
            #print("waiting for server reply; timeout={}".format(timeout))
            res = self.client.brpop(req['id'], timeout=timeout)
            #print('res', res)

        except Exception as e:
            #print('error', str(e))
            if self.logger is not None:
                self.logger.error("client call error: {}".format(e),
                                  exc_info=True)
            raise ServiceUnavailableError(e)

        if res is None:
            # timeout
            raise TimeoutError("Timed out waiting for server to respond.")

        channel, reply_pkt = res

        return reply_pkt

#
# ------------------ MODULE FUNCTIONS ------------------
#

def get_serverClass(secure=False):
    """Returns the class of the server for this transport. `secure` is
    True if the class should support a secure transport.
    """
    return RedisSimpleRPCServer


def make_serviceProxy(name, host, port, auth=None, encoding=None,
                      secure=False, logger=None):
    """[See documentation in ro_generic.py]"""

    # NOTE: host must match that used in server!
    # See: RedisSimpleRPCServer constructor
    host = '_'
    list_name = "{}:{}:{}".format(name, host, port)

    if encoding is None:
        encoding = 'msgpack'
    packer = get_packer(encoding)

    # If successful, return an object that has an attribute called
    # "proxy". NOTE THAT: service proxies do not require that the service be
    # available when they are created!
    return RedisSimpleRPCClient(redis_url, list_name, name, packer,
                                auth=auth, secure=secure, logger=logger)
