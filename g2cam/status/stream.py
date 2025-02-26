# This is open-source software licensed under a BSD license.
# Please see the file LICENSE.txt for details.

import time

import redis

from g2base.remoteObjects import ro_packer
from g2base import Bunch


class StatusStream(object):
    """
    Status streaming client for Subaru Gen2 Observation Control System.

    IMPORTANT:

    [1] Do not rely on the internals of this class API!!!  The publish/
    subscribe method and details are subject to change over time!

    """
    def __init__(self, host='localhost', port=9989,
                 db=0, username=None, password=None,
                 topics=None, pack_type='msgpack', logger=None):
        self.ps_host = host
        self.ps_port = port
        self.ps_db = db
        self.ps_user = username
        self.ps_pswd = password
        if topics is None:
            topics = ['status']
        self.ps_topics = topics
        self.pack_info = Bunch.Bunch(ptype=pack_type)
        self.logger = logger

        self.timeout = 0.2
        self.reconnect_interval = 20.0
        self.socket_timeout = 60.0

        # for Redis streaming
        self.rs = None
        self.pubsub = None

    def connect(self):
        # TODO: username, password ignored for now
        self.rs = redis.StrictRedis(host=self.ps_host, port=self.ps_port,
                                    db=self.ps_db,
                                    socket_timeout=self.socket_timeout,
                                    health_check_interval=30.0)
        self.pubsub = self.rs.pubsub(ignore_subscribe_messages=True)
        self.pubsub.subscribe(*self.ps_topics)

    def reconnect(self):
        self.close()
        return self.connect()

    def subscribe(self, topic):
        if not topic in self.ps_topics:
            self.ps_topics.append(topic)
            self.connect()

    def unsubscribe(self, topic):
        if topic in self.ps_topics:
            self.ps_topics.remove(topic)
            self.connect()

    def publish(self, topic, envelope, pack_info=None):
        """Publish on a topic.

        Parameters
        ----------
        topic : str
            Topic/channel used to publish something

        envelope : dict
            Dictionary containing items to be published

        NOTE: envelope should contain key "mtype" that is a string
        indicating how to handle the contents.
        """
        if pack_info is None:
            pack_info = self.pack_info
        envelope['pack_time'] = time.time()
        try:
            buf = ro_packer.pack(envelope, pack_info)
            self.rs.publish(topic, buf)

        except Exception as e:
            if self.logger is not None:
                self.logger.error("error in publishing message: {}".format(e),
                                  exc_info=True)
                self.logger.error("envelope: {}".format(str(envelope)))

    def listen(self, queue):
        pkt = None
        try:
            pkt = self.pubsub.get_message(timeout=self.timeout)
            #if self.logger is not None:
            #    self.logger.error("NORMAL timeout")
            if pkt is None:
                # timeout
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
                self.logger.error("error in getting message: {}".format(e),
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
                self.logger.error("Error unpacking payload: {}".format(e),
                                  exc_info=True)

        queue.put(envelope)

    def subscribe_loop(self, ev_quit, queue):

        while not ev_quit.is_set():
            try:
                self.reconnect()

                while not ev_quit.is_set():

                    self.listen(queue)

            except Exception as e:
                if self.logger is not None:
                    self.logger.error("Error connecting to status feed: {}".format(e),
                                      exc_info=True)
                ev_quit.wait(self.reconnect_interval)

    def close(self):
        try:
            self.rs.close()
        except Exception as e:
            pass
        self.rs = None
