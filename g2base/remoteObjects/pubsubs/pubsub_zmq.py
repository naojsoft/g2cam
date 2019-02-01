#
# This is open-source software licensed under a BSD license.
# Please see the file LICENSE.txt for details.
#
"""
In order to run this pubsub transport, you need to have the python
'zmq' module installed.  AND you need to be running a the zmq pubsub
hub defined in this file.  Like so:

   $ python pubsub_zmq.py

"""
import sys
import threading
import time

import zmq

from g2base import Callback, ssdlog
from g2base.remoteObjects import ro_packer


class PubSub(Callback.Callbacks):

    def __init__(self, host='localhost', port=5562, db=1,
                 logger=None):
        Callback.Callbacks.__init__(self)

        self.host = host
        self.port = port
        self.db = db
        self.logger = logger

        self.context = None
        self.pub = None
        self.sub = None
        self.timeout = 0.2

        self.reconnect()

    def reconnect(self):
        self.context = zmq.Context()
        self.pub = self.context.socket(zmq.PUSH)
        self.pub.connect("tcp://%s:%s" % (self.host, self.port))
        self.pub.setsockopt(zmq.LINGER, 0)
        self.pub.setsockopt(zmq.SNDHWM, 10000)
        self.sub = self.context.socket(zmq.SUB)
        self.sub.connect("tcp://%s:%s" % (self.host, self.port - 1))
        self.sub.setsockopt(zmq.LINGER, 0)
        self.sub.setsockopt(zmq.RCVHWM, 10000)
        # poller makes us take a big hit in efficiency
        # better to use the time out feature
        self.sub.setsockopt(zmq.RCVTIMEO, 1000)
        # self.poller = zmq.Poller()
        # self.poller.register(self.sub, zmq.POLLIN)

    def publish(self, channel, envelope, pack_info):
        packet = ro_packer.pack(envelope, pack_info)
        try:
            #self.pub.send_unicode("%s %s" % (channel, message), zmq.NOBLOCK)
            #self.pub.send_unicode("%s %s" % (channel, message))
            channel = channel.encode('utf-8')
            buf = b"%s %s" % (channel, packet)
            self.pub.send(buf)

        except zmq.error.Again:
            # what to do here? -- schedule a retry?
            print('AGAIN error')
            pass

    def flush(self):
        #self.pub.close()
        pass

    def subscribe(self, channel):
        if not self.has_callback(channel):
            self.enable_callback(channel)

        # ZMQ doesn't like unicode
        channel_b = channel.encode()
        self.sub.setsockopt(zmq.SUBSCRIBE, channel_b)

    def unsubscribe(self, channel):
        # ZMQ doesn't like unicode
        channel_b = channel.encode()
        self.sub.setsockopt(zmq.UNSUBSCRIBE, channel_b)

    def start(self, ev_quit=None):
        if ev_quit is None:
            ev_quit = threading.Event()
        self.ev_quit = ev_quit

        t = threading.Thread(target=self.subscribe_loop,
                             args=[ev_quit])
        t.start()

    def stop(self):
        self.ev_quit.set()

    ## def listen(self):
    ##     while True:
    ##         channel, _, data = self.sub.recv().partition(b' ')
    ##         yield {"type": "message", "channel": channel, "data": data}

    def get_message(self, timeout=None):
        if timeout is None:
            timeout = self.timeout

        data = None
        t_ = time.time()
        while time.time() - t_ < timeout:
            try:
                data = self.sub.recv()
                break
            except zmq.error.Again:
                # timeout at micro timing cycle
                continue

        if data is not None:
            channel, _, data = data.partition(b' ')
            channel = channel.decode('utf-8')
            pkt = {"type": "message", "channel": channel, "data": data}
            return pkt

    def get_message2(self, timeout=None):
        if timeout is not None:
            evts = self.poller.poll(timeout)
            if len(evts) == 0:
                return None

        data = self.sub.recv()

        if data is not None:
            channel, _, data = data.partition(b' ')
            channel = channel.decode('utf-8')
            pkt = {"type": "message", "channel": channel, "data": data}
            return pkt

    def listen(self):
        #print('waiting for message')
        pkt = self.get_message(timeout=self.timeout)
        if pkt is None:
            # timeout
            #print('timeout')
            return

        #print('got message', pkt)
        # this is Redis API--packet will have type, channel and
        # data fields.
        if pkt['type'] != "message":
            return

        channel = pkt['channel']
        try:
            packet = pkt['data']
            envelope = ro_packer.unpack(packet)

        except Exception as e:
            # need traceback
            if self.logger is not None:
                self.logger.error("Error unpacking payload: %s" % (str(e)))
            raise

        # this will catch its own exceptions so that we don't
        # kill the subscribe loop
        self.make_callback(channel, channel, envelope)

    def subscribe_loop(self, ev_quit):
        self.ev_quit = ev_quit
        while not ev_quit.is_set():
            self.listen()


class Server(object):

    def __init__(self, host='localhost', port=5562, logger=None):

        self.host = host
        self.port = port
        self.logger = logger

    def service_loop(self, ev_quit):
        """This loop manages the pubsub 'hub' process."""
        context = zmq.Context()
        receiver = context.socket(zmq.PULL)
        receiver.setsockopt(zmq.RCVHWM, 10000)
        receiver.setsockopt(zmq.LINGER, 0)
        receiver.bind("tcp://*:%s" % (self.port))
        sender = context.socket(zmq.PUB)
        sender.setsockopt(zmq.SNDHWM, 10000)
        sender.setsockopt(zmq.LINGER, 0)
        sender.bind("tcp://*:%s" % (self.port - 1))

        # TODO: would maybe need to set a timeout to be able to see
        # an ev_quit event
        while not ev_quit.is_set():
            sender.send(receiver.recv())


def main(options, args):
    logger = ssdlog.make_logger('recv', options)

    server = Server(host=options.host, port=options.port, logger=logger)
    ev_quit = threading.Event()
    try:
        server.service_loop(ev_quit)

    except (KeyboardInterrupt, SystemExit):
        pass

if __name__ == '__main__':
    from optparse import OptionParser

    usage = "usage: %prog [options]"
    optprs = OptionParser(usage=usage)

    optprs.add_option("--debug", dest="debug", default=False,
                      action="store_true",
                      help="Enter the pdb debugger on main()")
    optprs.add_option("--host", dest="host", metavar="HOST",
                      default='localhost',
                      help="Bind to HOST")
    optprs.add_option("--profile", dest="profile", action="store_true",
                      default=False,
                      help="Run the profiler on main()")
    optprs.add_option("--port", dest="port", type="int", default=5562,
                      help="Register using PORT", metavar="PORT")
    ssdlog.addlogopts(optprs)

    (options, args) = optprs.parse_args(sys.argv[1:])

    if len(args) != 0:
        optprs.error("incorrect number of arguments")

    # Are we debugging this?
    if options.debug:
        import pdb

        pdb.run('main(options, args)')

    # Are we profiling this?
    elif options.profile:
        import profile

        print(("%s profile:" % sys.argv[0]))
        profile.run('main(options, args)')

    else:
        main(options, args)

# END
