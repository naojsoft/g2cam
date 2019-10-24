#
# ro_socket.py -- socket-based RPC services for remoteObjects system
#
# Eric Jeschke (eric@naoj.org)
#
import uuid
import time
import socket
import select

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


class RpcMessage(object):

    def __init__(self, encoding, secure):
        super(RpcMessage, self).__init__()

        self.encoding = encoding
        self.secure = secure
        self.address = None
        self.rpc_version = rpc_version

    def encode(self, msg):
        hdr = '%s,%s,%s,%d' % (
            self.rpc_version, self.encoding,
            str(self.secure), len(msg))
        hdr = hdr.encode('latin1')
        # pad header to required size
        hdr += b' ' * (rpc_hdr_len - len(hdr))
        if len(hdr) != rpc_hdr_len:
            raise Error("RPC header len actual(%d) != expected(%d)" % (
                len(hdr), rpc_hdr_len))
        return hdr + msg

    def send(self, sock, msg, flags=0):
        pkt = self.encode(msg)
        num_bytes = len(pkt)
        size = sock.sendall(pkt)
        #print("sent %d bytes" % (num_bytes))

    def recv(self, sock, flags=0):
        #print("reading hdr")
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
            msg = b''.join(pieces)
        hdr = msg[:rpc_hdr_len].decode('latin1')
        #print("hdr: ", hdr)
        if len(hdr) != rpc_hdr_len:
            raise Error("RPC header len actual(%d) != expected(%d)" % (
                len(hdr), rpc_hdr_len))

        tup = hdr.strip().split(',')
        if len(tup) != 4:
            raise Error("RPC header: num fields(%d) != expected(%d) [hdr:%s]" % (
                len(tup), 4, hdr))
        ver, enc, secure, body_size = tup
        body_size = int(body_size)
        #secure = bool(secure)
        msg = msg[rpc_hdr_len:]
        if len(msg) < body_size:
            # we didn't receive all of the body--read the rest
            pieces = [ msg ]
            size_rem = body_size - len(msg)
            while size_rem > 0:
                chunk_size = min(size_rem, read_chunk_size)
                pieces.append(sock.recv(chunk_size))
                size_rem -= len(pieces[-1])
            msg = b''.join(pieces)
        if len(msg) != body_size:
            raise Error("RPC body len actual(%d) != expected(%d)" % (
                len(msg), body_size))
        return msg

    def __repr__(self):
        return "<%s>" % (self.__class__.__name__)


class SocketRPCServer(GenericRPCServer):
    """
    Basic Socket-RPC server.
    """

    def __init__(self, name, host, port, **kwargs):

        super(SocketRPCServer, self).__init__(name, host, port, **kwargs)
        #self.timeout = 5.0
        self.num_connect = default_num_connect

        host = ''
        self._address = (host, port)

        if self._packer is None:
            self._packer = 'msgpack'
        self.packer = get_packer(self._packer)

        self.receiver = None

    def setup(self):
        self.logger.info("initializing native socket RPC server on {}:{}".format(self.host, self.port))

        # Our receiving socket
        srvsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srvsock.settimeout(self.timeout)
        srvsock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srvsock.bind(self._address)
        srvsock.listen(self.num_connect)
        srvsock.setblocking(False)
        self.receiver = srvsock
        self.logger.debug("Listening @ %s" % str(self._address))

    def serve_forever(self):

        self.setup()
        socklist = [ self.receiver ]

        self.logger.info("server starting request loop...")
        # Request processing loop
        while not self.ev_quit.is_set():
            try:
                rlist, wlist, elist = select.select([self.receiver], [], [],
                                                    1.0)
                #print(rlist, wlist, elist)
                if len(rlist) == 0:
                    continue

                # Get client connection
                clisock, cliaddr = self.receiver.accept()
                clisock.settimeout(self.timeout)

            except socket.timeout:
                self.logger.error("Timed out (>%.2f) reading socket" % (
                    self.timeout))
                continue

            except Exception as e:
                self.logger.error("Server socket error: %s" % (
                    str(e)))
                continue

            if not self.threaded:
                self.process_request(clisock, cliaddr)

            else:
                thunk = (lambda x, y: (lambda: self.process_request(x, y)))(clisock, cliaddr)
                self.inbox.put(thunk)

        self.logger.debug("server terminating request loop...")

        # special flag to terminate workers
        self.logger.info("server exiting.")

    def process_request(self, clisock, cliaddr):

        try:
            msg = RpcMessage(self.packer.kind, self.secure)
            req_pkt = msg.recv(clisock)

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

            msg.send(clisock, rep_pkt)

        finally:
            try:
                clisock.close()
            except Exception:
                pass

    def stop(self):
        super(SocketRPCServer, self).stop()

        if self.receiver:
            self.receiver.close()
            self.receiver = None


#
# ------------------ CLIENT ------------------
#

class SocketRPCClient(GenericRPCClient):

    def __init__(self, host, port, name, packer, **kwargs):
        super(SocketRPCClient, self).__init__(name, packer, **kwargs)

        self._address = (host, port)
        self._sender = None

    def rpc_call(self, req, req_pkt, timeout=None):

        if timeout is None:
            timeout = 1000000

        #print('making message')
        msg = RpcMessage(self.packer.kind, self.secure)

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            try:
                sock.setblocking(False)
                if timeout is not None:
                    sock.settimeout(timeout)
                #print('connect', self._address)
                sock.connect(self._address)

            except Exception as e:
                #sock = None
                if self.logger is not None:
                    self.logger.error("client connect error: {}".format(e),
                                      exc_info=True)
                raise ServiceUnavailableError("Error creating socket client: {}".format(e))

            rep_pkt = None
            try:
                #print("calling server: req: {}".format(req))
                msg.send(sock, req_pkt)

                #print("client sent. waiting for reply...")
                rlist, wlist, elist = select.select([sock], [], [],
                                                    timeout)
                if len(rlist) == 0:
                    errmsg = ("Got no reply within %.3f time limit" % (
                        timeout))
                    if self.logger is not None:
                        self.logger.error(errmsg)
                    #print("no reply")
                    raise TimeoutError(errmsg)

                #print("got reply, unpacking")
                rep_pkt = msg.recv(sock)

            except Exception as e:
                #print('error', str(e))
                if self.logger is not None:
                    self.logger.error("client call error: {}".format(e),
                                      exc_info=True)

                raise ServiceUnavailableError(e)

        #print("returning packet")
        return rep_pkt

#
# ------------------ MODULE FUNCTIONS ------------------
#

def get_serverClass(secure=False):
    """Returns the class of the server for this transport. `secure` is
    True if the class should support a secure transport.
    """
    return SocketRPCServer


def make_serviceProxy(name, host, port, auth=None, encoding=None,
                      secure=False, logger=None):
    """[See documentation in ro_generic.py]"""

    # NOTE: host must match that used in server!
    if encoding is None:
        encoding = 'msgpack'
    packer = get_packer(encoding)

    # If successful, return an object that has an attribute called
    # "proxy". NOTE THAT: service proxies do not require that the service be
    # available when they are created!
    return SocketRPCClient(host, port, name, packer,
                           auth=auth, secure=secure, logger=logger)
