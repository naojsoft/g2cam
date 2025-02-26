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
        self.ev_quit = None

        self.redis = None
        self.pubsub = None
        self.lock = threading.RLock()
        self.subscriptions = set([])
        self.pending_subscribes = set([])
        self.pending_unsubscribes = set([])
        self.timeout = 0.2
        self.reconnect_interval = 20.0
        self.socket_timeout = 60.0
        self.ping_interval = 30.0

    def reconnect(self):
        self.redis = redis.StrictRedis(host=self.host, port=self.port,
                                       db=self.db,
                                       socket_timeout=self.socket_timeout,
                                       health_check_interval=self.ping_interval)
        ## self.redis = BufferedRedis(host=self.host, port=self.port,
        ##                            db=self.db)
        self.pubsub = self.redis.pubsub(ignore_subscribe_messages=True)
        ## self.pubsub.subscribe('admin')

    def publish(self, channel, envelope, pack_info):
        if self.redis is None:
            raise ConnectionError("pubsub is not connected")
        packet = ro_packer.pack(envelope, pack_info)

        self.redis.publish(channel, packet)

    def flush(self):
        if hasattr(self.redis, 'flush'):
            # only our BufferedRedis has flush
            self.redis.flush()

    def subscribe(self, channel):
        with self.lock:
            self.pending_subscribes.add(channel)

        if not self.has_callback(channel):
            self.enable_callback(channel)

    def unsubscribe(self, channel):
        with self.lock:
            if channel in self.subscriptions:
                self.pending_unsubscribes.add(channel)

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
        pkt = None
        try:
            pkt = self.pubsub.get_message(timeout=self.timeout)
            if pkt is None:
                # "normal" timeout
                return

        except redis.exceptions.TimeoutError as e:
            if self.logger is not None:
                self.logger.error("abnormal timeout--server restart?")
            # timeout
            raise e

        except redis.exceptions.ConnectionError as e:
            # TODO: handle server disconnection
            if self.logger is not None:
                self.logger.warning("server disconnected us")
            raise e

        except Exception as e:
            if self.logger is not None:
                self.logger.error(f"error in getting message: {e}",
                                  exc_info=True)
                self.logger.error("pkt: {}".format(str(pkt)))
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
                self.logger.error("Error unpacking payload: %s" % (str(e)),
                                  exc_info=True)
            raise

        # this will catch its own exceptions so that we don't
        # kill the subscribe loop
        self.make_callback(channel, channel, envelope)

    def subscribe_loop(self, ev_quit):
        while not ev_quit.is_set():
            try:
                self.reconnect()

                # subscribe or re-subscribe
                with self.lock:
                    subscriptions = self.subscriptions.copy()
                for name in subscriptions:
                    self.pubsub.subscribe(channel)

                while not ev_quit.is_set():

                    # take care of any pending subscribes/unsubscribes
                    with self.lock:
                        pending_subs = self.pending_subscribes.copy()
                        self.pending_subscribes = set([])
                        self.subscriptions |= pending_subs
                        pending_unsubs = self.pending_unsubscribes.copy()
                        self.pending_unsubscribes = set([])
                        self.subscriptions -= pending_unsubs
                    if len(pending_subs) > 0:
                        for channel in pending_subs:
                            self.pubsub.subscribe(channel)
                    if len(pending_unsubs) > 0:
                        for channel in pending_unsubs:
                            self.pubsub.unsubscribe(channel)

                    # handle any incoming subscriptions
                    self.listen()

            except Exception as e:
                self.redis = None
                self.pubsub = None
                self.logger.error(f"Error listening for messages: {e}",
                                  exc_info=True)
                ev_quit.wait(self.reconnect_interval)

    def close(self):
        try:
            self.redis.close()
        except Exception as e:
            pass
        self.redis = None
