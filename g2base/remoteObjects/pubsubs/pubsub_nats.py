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
        self.subscriptions = set([])
        self.timeout = 0.2
        self.reconnect_interval = 20.0

        url = "nats://%s:%d" % (self.host, self.port)
        self.pubsub = NATSClient(url)
        self.pubsub.connect()

        #self.reconnect()

    def reconnect(self):
        self.pubsub.reconnect()

    def publish(self, channel, envelope, pack_info):
        if self.pubsub is None:
            raise ConnectionError("pubsub is not connected")
        packet = ro_packer.pack(envelope, pack_info)
        self.pubsub.publish(channel, payload=packet)

    def subscribe(self, channel):
        self.subscriptions.add(channel)

        if not self.has_callback(channel):
            self.enable_callback(channel)

        cb_fn = self.create_callback_fn(channel)

        if self.pubsub is not None:
            subscription = self.pubsub.subscribe(channel, callback=cb_fn)

    def unsubscribe(self, channel):
        if channel in self.subscriptions:
            self.subscriptions.remove(channel)

            if self.pubsub is not None:
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
        while not ev_quit.is_set():
            try:
                self.reconnect()

                # subscribe or re-subscribe
                for name in self.subscriptions:
                    self.subscribe(name)

                while not ev_quit.is_set():
                    self.listen()

            except Exception as e:
                self.pubsub = None
                self.logger.error(f"Error in msg handling loop: {e}")
                ev_quit.wait(self.reconnect_interval)

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
