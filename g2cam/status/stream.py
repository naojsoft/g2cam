# This is open-source software licensed under a BSD license.
# Please see the file LICENSE.txt for details.

import redis

from g2base.remoteObjects import ro_packer


class StatusStream(object):
    """
    Status streaming client for Subaru Gen2 Observation Control System

    USAGE:


    IMPORTANT:

    [1] Do not rely on the internals of this class API!!!  The publish/
    subscribe method and details are subject to change over time!

    """
    def __init__(self, host='localhost', port=9989,
                 db=0, username=None, password=None,
                 logger=None):
        self.ps_host = host
        self.ps_port = port
        self.ps_db = db
        self.ps_user = username
        self.ps_pswd = password
        self.logger = logger

        self.timeout = 0.2
        self.reconnect_interval = 20.0

        # for Redis status streaming
        self.rs = None
        self.pubsub = None

    def connect(self):
        # TODO: username, password ignored for now
        self.rs = redis.StrictRedis(host=self.ps_host, port=self.ps_port,
                                    db=self.ps_db)
        self.pubsub = self.rs.pubsub(ignore_subscribe_messages=True)
        self.pubsub.subscribe('status')

    def reconnect(self):
        return self.connect()

    def listen(self, queue):
        pkt = None
        try:
            try:
                pkt = self.pubsub.get_message(timeout=self.timeout)
                #if self.logger is not None:
                #    self.logger.error("NORMAL timeout")
                if pkt is None:
                    # timeout
                    return
            except redis.exceptions.TimeoutError:
                #if self.logger is not None:
                #    self.logger.error("ABNORMAL timeout")
                # timeout
                return

            except redis.exceptions.ConnectionError:
                # TODO: handle server disconnection
                #if self.logger is not None:
                #    self.logger.warning("server disconnected us")
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
        return self.rs.close()
        self.rs = None
