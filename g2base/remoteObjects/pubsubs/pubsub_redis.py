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

import redis

from g2base import Callback

from g2base.remoteObjects import ro_packer


class BufferedRedis(redis.Redis):
    """
    Wrapper for Redis pub-sub that uses a pipeline internally
    for buffering message publishing. A thread is run that
    periodically flushes the buffer pipeline.
    """

    def __init__(self, *args, **kwargs):
        super(BufferedRedis, self).__init__(*args, **kwargs)

        self.flush_interval = 0.001
        self.flush_size = 1000

        self.buffer = self.pipeline()
        self.lock = threading.Lock()
        t = threading.Thread(target=self.flusher, args=[])
        t.start()

    def flush(self):
        """
        Manually flushes the buffer pipeline.
        """
        with self.lock:
            self.buffer.execute()

    def flusher(self):
        """
        Thread that periodically flushes the buffer pipeline.
        """
        while True:
            time.sleep(self.flush_interval)
            with self.lock:
                self.buffer.execute()

    def publish(self, *args, **kwargs):
        """
        Overrides publish to use the buffer pipeline, flushing
        it when the defined buffer size is reached.
        """
        with self.lock:
            self.buffer.publish(*args, **kwargs)
            if len(self.buffer.command_stack) >= self.flush_size:
                self.buffer.execute()

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

        self.reconnect()

    def reconnect(self):
        self.redis = redis.StrictRedis(host=self.host, port=self.port,
                                       db=self.db)
        ## self.redis = BufferedRedis(host=self.host, port=self.port,
        ##                            db=self.db)
        self.pubsub = self.redis.pubsub()
        self.pubsub.subscribe('admin')

    def publish(self, channel, envelope, pack_info):
        packet = ro_packer.pack(envelope, pack_info)

        self.redis.publish(channel, packet)

    def flush(self):
        if hasattr(self.redis, 'flush'):
            # only our BufferedRedis has flush
            self.redis.flush()

    def subscribe(self, channel):
        if not self.has_callback(channel):
            self.enable_callback(channel)

        self.pubsub.subscribe(channel)

    def unsubscribe(self, channel):
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

    def listen(self):
        pkt = self.pubsub.get_message(timeout=self.timeout)
        if pkt is None:
            # timeout
            return

        # this is Redis API--packet will have type, channel and
        # data fields.
        if pkt['type'] != "message":
            return

        channel = pkt['channel']
        channel = channel.decode('utf-8')
        try:
            packet = pkt['data']
            envelope = ro_packer.unpack(packet)

        except Exception as e:
            if self.logger is not None:
                self.logger.error("Error unpacking payload: %s" % (str(e)))
            raise
            # need traceback

        # this will catch its own exceptions so that we don't
        # kill the subscribe loop
        self.make_callback(channel, channel, envelope)

    def subscribe_loop(self, ev_quit):
        self.ev_quit = ev_quit
        while not ev_quit.is_set():
            self.listen()

# END
