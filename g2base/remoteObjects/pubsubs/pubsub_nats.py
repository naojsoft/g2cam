#
# This is open-source software licensed under a BSD license.
# Please see the file LICENSE.txt for details.
#
"""
In order to run this pubsub transport, you need to have the pynats module
installed.  Get it from here:

    https://github.com/Gr1N/nats-python.git

And you need to be running a gnats pubsub server:

./gnatsd --addr 127.0.0.1 --port 4222

"""
import threading
import time

from g2base import Callback
from g2base.remoteObjects import ro_packer

from pynats import NATSClient


class PubSub(Callback.Callbacks):

    def __init__(self, host='localhost', port=4222, logger=None):
        Callback.Callbacks.__init__(self)

        self.host = host
        self.port = port
        self.logger = logger

        self.pubsub = None
        self.timeout = 0.2

        url = "nats://%s:%d" % (self.host, self.port)
        self.pubsub = NATSClient(url)
        self.pubsub.connect()

        #self.reconnect()

    def reconnect(self):
        self.pubsub.reconnect()

    def publish(self, channel, envelope, pack_info):
        packet = ro_packer.pack(envelope, pack_info)

        self.pubsub.publish(channel, payload=packet)

    def publish_many(self, channels, envelope, pack_info):
        packet = ro_packer.pack(envelope, pack_info)

        with self.lock:
            for channel in channels:
                self.redis.publish(channel, packet)

    def subscribe(self, channel):
        if not self.has_callback(channel):
            self.enable_callback(channel)

        cb_fn = self.create_callback_fn(channel)

        subscription = self.pubsub.subscribe(channel, callback=cb_fn)

    def unsubscribe(self, channel):
        # TODO: need to unsubscribe with the subscription
        self.pubsub.unsubscribe(channel)

    def start(self, ev_quit=None):
        if ev_quit is None:
            ev_quit = threading.Event()
        self.ev_quit = ev_quit

        t = threading.Thread(target=self.subscribe_loop,
                             args=[ev_quit])
        t.start()

    def stop(self):
        self.ev_quit.set()

    def flush(self):
        # no-op
        pass

    def listen(self):
        # TODO: need to be able to set a timeout on this
        self.pubsub.wait(count=1)

    def subscribe_loop(self, ev_quit):
        self.ev_quit = ev_quit
        while not ev_quit.is_set():
            try:
                self.listen()

            except Exception as e:
                self.logger.error("Error in msg handling loop: %s" % (str(e)))
                self.reconnect()

    def create_callback_fn(self, channel):
        def _cb(msg):
            try:
                packet = msg.payload
                envelope = ro_packer.unpack(packet)

            except Exception as e:
                # need traceback
                if self.logger is not None:
                    self.logger.error("Error unpacking payload: %s" % (str(e)))
                raise

            # this will catch its own exceptions so that we don't
            # kill the subscribe loop
            self.make_callback(channel, channel, envelope)

        return _cb
