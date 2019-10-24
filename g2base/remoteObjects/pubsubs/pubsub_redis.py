#
# This is open-source software licensed under a BSD license.
# Please see the file LICENSE.txt for details.
#
"""
In order to run this pubsub transport, you need to have the python
'redis-py' module installed.  AND you need to be running a Redis server.
"""
import threading
import time
from g2base import six
if six.PY2:
    import Queue
else:
    import queue as Queue

import redis

from g2base import Callback

from g2base.remoteObjects import ro_packer


class PubSub(Callback.Callbacks):

    def __init__(self, host='localhost', port=6379, db=1,
                 logger=None):
        Callback.Callbacks.__init__(self)

        self.host = host
        self.port = port
        self.db = db
        self.logger = logger

        self.redis = None
        self.pubsub = None
        self.timeout = 0.2
        self.queue = Queue.Queue()

    def reconnect(self):
        self.redis = redis.StrictRedis(host=self.host, port=self.port,
                                       db=self.db)
        ## self.redis = BufferedRedis(host=self.host, port=self.port,
        ##                            db=self.db)
        self.pubsub = self.redis.pubsub(ignore_subscribe_messages=True)
        self.pubsub.subscribe('admin')

    def publish(self, channel, envelope, pack_info):
        packet = ro_packer.pack(envelope, pack_info)
        # redis client is supposedly thread-safe, but not pubsub
        self.redis.publish(channel, packet)

    def publish_many(self, channels, envelope, pack_info):
        packet = ro_packer.pack(envelope, pack_info)

        for channel in channels:
            # redis client is supposedly thread-safe, but not pubsub
            self.redis.publish(channel, packet)

    def flush(self):
        if hasattr(self.redis, 'flush'):
            # only our BufferedRedis has flush
            self.redis.flush()

    def subscribe(self, channel):
        if not self.has_callback(channel):
            self.enable_callback(channel)

        thunk = lambda: self.pubsub.subscribe(channel)
        self.queue.put(thunk)

    def unsubscribe(self, channel):
        thunk = lambda: self.pubsub.unsubscribe(channel)
        self.queue.put(thunk)

    def start(self, ev_quit=None):
        if ev_quit is None:
            ev_quit = threading.Event()
        self.ev_quit = ev_quit

        t = threading.Thread(target=self.subscribe_loop,
                             args=[ev_quit])
        t.start()

    def stop(self):
        self.ev_quit.set()

    def process_thunk(self):
        try:
            try:
                thunk = self.queue.get(block=False)

            except Queue.Empty:
                return

            thunk()

        except Exception as e:
            if self.logger is not None:
                self.logger.error("error in processing thunk: {}".format(e),
                                  exc_info=True)
                return

    def listen(self):
        pkt = None
        try:
            try:
                pkt = self.pubsub.get_message(timeout=self.timeout)
                #self.logger.error("NORMAL timeout")
                if pkt is None:
                    # timeout
                    return
            except redis.exceptions.TimeoutError:
                self.logger.error("ABNORMAL timeout")
                # timeout
                return

            except redis.exceptions.ConnectionError:
                # TODO: handle server disconnection
                self.logger.warning("server disconnected us")
                return

            # this is Redis API--packet will have type, channel and
            # data fields.
            if pkt['type'] != "message":
                return

        except Exception as e:
            if self.logger is not None:
                self.logger.error("error in getting message: {}".format(e),
                                  exc_info=True)
                self.logger.error("pkt: {}".format(str(pkt)))
                return

        channel = pkt['channel']
        channel = channel.decode('utf-8')
        try:
            packet = pkt['data']
            envelope = ro_packer.unpack(packet)

        except Exception as e:
            if self.logger is not None:
                self.logger.error("Error unpacking payload: {}".format(e),
                                  exc_info=True)
            raise

        # this will catch its own exceptions so that we don't
        # kill the subscribe loop
        self.make_callback(channel, channel, envelope)

    def subscribe_loop(self, ev_quit):
        self.ev_quit = ev_quit

        self.reconnect()

        while not ev_quit.is_set():

            self.process_thunk()

            self.listen()
