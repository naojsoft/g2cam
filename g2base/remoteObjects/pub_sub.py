#
# pub_sub.py -- Subaru Remote Objects Publish/Subscribe module
#

import time
import threading

from g2base import six
if six.PY2:
    import Queue
else:
    import queue as Queue

from g2base import Bunch, Task, Callback
from g2base.remoteObjects import ps_cfg

from .ro_pubsub import PubSub as InternalPS

ro_OK = 0


class PubSubError(Exception):
    """General class for exceptions raised by this module.
    """
    pass

class PubSub(Callback.Callbacks):
    """Base class for publish/subscribe entities.
    """

    ######## INTERNAL METHODS ########

    def __init__(self, name, logger,
                 ev_quit=None, threadPool=None, numthreads=30,
                 outlimit=4, inlimit=12):
        """
        Constructor for the PubSubBase class.
            name        pubsub name
            logger      logger to be used for any diagnostic messages
            threadPool  optional, threadPool for serving PubSub activities
            numthreads  if a threadPool is NOT furnished, the number of
                          threads to allocate
        """

        super(PubSub, self).__init__()

        self.logger = logger
        self.name = name
        self.numthreads = numthreads
        # this limits the number of incoming and outgoing connections
        self.outlimit = outlimit
        self.inlimit = inlimit
        self.outqueue = Queue.PriorityQueue()

        # Termination event
        if not ev_quit:
            ev_quit = threading.Event()
        self.ev_quit = ev_quit

        # If we were passed in a thread pool, then use it.  If not,
        # make one.  Record whether we made our own or not.
        if threadPool != None:
            self.threadPool = threadPool
            self.mythreadpool = False

        else:
            self.threadPool = Task.ThreadPool(logger=self.logger,
                                              ev_quit=self.ev_quit,
                                              numthreads=self.numthreads)
            self.mythreadpool = True

        # For task inheritance:
        self.tag = 'PubSub'
        self.shares = ['logger', 'threadPool']

        # Holds information about remote subscriptions
        self._remote_sub_info = set([])

        # Timeout for remote updates
        self.remote_timeout = 30.0

        # Interval between remote subscription updates
        self.update_interval = 60.0

        # warn us when the outgoing queue size exceeds this
        self.qlen_warn_limit = 100
        # but don't warn us more often than this interval
        self.qlen_warn_interval = 10.0

        self.pubsub = InternalPS(logger=self.logger)
        # TODO: allow auto configuration of optimal packer for transport
        self.pack_info = Bunch.Bunch(ptype='msgpack')

        # set up the channel map
        self.chmap = ChannelMap()
        ps_cfg.setup(self.chmap)
        self.chmap.recalc_constituents()

        self.enable_callback('update')

    def get_threadPool(self):
        return self.threadPool

    def _delivery_daemon(self, i):
        last_warn = time.time()

        while not self.ev_quit.is_set():
            n = self.get_qlen()
            cur_time = time.time()
            if ((i == 0) and (n > self.qlen_warn_limit) and
                (cur_time - last_warn > self.qlen_warn_interval)):
                self.logger.warning("Queue size %d exceeds limit %d" % (
                    n, self.qlen_warn_limit))
                last_warn = cur_time

            try:
                #priority, msg = self.outqueue.get(True, 0.25)
                wrapper = self.outqueue.get(True, 0.25)
                self.logger.debug("envelope: {}".format(wrapper.envelope))

            except Queue.Empty:
                continue

            self.make_callback('update', wrapper.envelope)

    def get_qlen(self):
        return self.outqueue.qsize()

    def update_remote_subscriptions_loop(self):

        while not self.ev_quit.is_set():
            time_end = time.time() + self.update_interval

            channels = list(self._remote_sub_info)
            self.logger.debug("updating remote subscriptions: %s" % (
                str(channels)))
            for channel in channels:
                try:
                    self.pubsub.subscribe(channel)
                    #self.pubsub.add_callback(channel, self.subscribe_update_cb)

                except Exception as e:
                    self.logger.error("Error pinging remote subscription %s: %s" % (
                            str(tup), str(e)))

            # Sleep for remainder of desired interval.  We sleep in
            # small increments so we can be responsive to changes to
            # ev_quit
            cur_time = time.time()
            self.logger.debug("Waiting interval, remaining: %f sec" % (
                time_end - cur_time))

            while (cur_time < time_end) and (not self.ev_quit.is_set()):
                time.sleep(0.0)
                self.ev_quit.wait(min(0.1, time_end - cur_time))
                cur_time = time.time()

            self.logger.debug("End interval wait")

        self.logger.info("exiting remote subscriptions update loop")

    def subscribe_update_cb(self, pubsub, channel, envelope):
        """
        This is our callback that gets called when the internal pubsub
        receives a message.  We decouple this call from delivering the
        callback to our local subscribers.
        """
        # Verify that this was sent by one of our compatible peers
        if envelope['mtype'] != 'pubsub':
            return

        # Avoid cyclic dependencies--don't update ourselves if we
        # originated this event
        if self.name in envelope['names']:
            return ro_OK

        # If successful, update our subscribers
        # NOTE: this decouples the thread in internal pubsub from callbacks
        # that take time to process it, and also makes sure that priority
        # messages are handled before others
        self.outqueue.put(EnvelopeWrapper(envelope))

    ######## PUBLIC METHODS ########

    def start(self, wait=True):
        """Start any background threads, etc. used by this pubsub.
        """

        # Start our thread pool (if we created it)
        if self.mythreadpool:
            self.threadPool.startall(wait=wait)

        self.logger.info("PubSub background tasks started.")

    def stop(self, wait=True):
        """Stop any background threads, etc. used by this pubsub.
        """
        # Stop our thread pool (if we created it)
        if self.mythreadpool:
            self.threadPool.stopall(wait=wait)

        self.logger.info("PubSub background tasks stopped.")

    def notify(self, value, channels, priority=0):
        """
        Method called by local users of this PubSub to update it
        with new and changed items.
            value       usually a dict, but can be any marshallable value
            channels    a list of channel names to send the specified update
        """
        names = [self.name]
        # form a 'pubsub' message
        envelope = dict(mtype='pubsub', names=names, payload=value,
                        timestamp=time.time(),
                        priority=priority, channels=channels)

        # NOTE: could create tasks in the threadpool to publish the
        # item, if the publishing becomes a bottleneck
        for channel in channels:
            channel_list = self.chmap.constituents[channel]
            self.pubsub.publish_many(channel_list, envelope, self.pack_info)

    def subscribe(self, subscriber, channels, options):
        # a no-op in this version
        return ro_OK

    def unsubscribe(self, subscriber, channels, options):
        # a no-op in this version
        return ro_OK

    def publish_to(self, subscriber, channels, options):
        # a no-op in this version
        pass

    def add_channels(self, channel_names):
        # a no-op in this version
        pass

    def subscribe_remote(self, publisher, channels, options):
        # NOTE: 'publisher' and 'options' args retained for backwards
        # API compatibility

        for channel in channels:
            self.pubsub.subscribe(channel)
            self._remote_sub_info.add(channel)
            self.pubsub.add_callback(channel, self.subscribe_update_cb)

    def unsubscribe_remote(self, publisher, channels, options):
        # NOTE: 'publisher' and 'options' args retained for backwards
        # API compatibility

        for channel in channels:
            self.pubsub.unsubscribe(channel)
            if channel in self._remote_sub_info:
                self._remote_sub_info.remove(channel)

    def subscribe_cb(self, fn_update, channels):
        """Register local subscriber callback (_fn_update_)
        for updates on channel(s) _channels_.
        """
        if not callable(fn_update):
            raise PubSubError('subscriber functions must be callables')

        # NOTE: for compatibility with remote objects pub/sub v1--
        # it has this signature for callbacks
        def _anon_fn(pubsub, channel, msg):
            fn_update(msg['payload'], msg['names'], msg['channels'])

        for channel in channels:
            if not self.pubsub.has_callback(channel):
                self.pubsub.enable_callback(channel)
            self.pubsub.add_callback(channel, _anon_fn)

    def start_server(self, svcname=None, host=None, port=None,
                     ping_interval=None,
                     strict_registration=False,
                     threaded_server=None,
                     authDict=None, default_auth=None,
                     secure=None, cert_file=None,
                     ns=None,
                     usethread=True, wait=True, timeout=None):

        self.logger.info("Starting remote subscriptions update loop...")
        t = Task.FuncTask(self.update_remote_subscriptions_loop, [], {},
                          logger=self.logger)
        t.init_and_start(self)

        # Start up delivery daemons
        for i in range(self.outlimit):
            t = Task.FuncTask2(self._delivery_daemon, i)
            t.init_and_start(self)

        self.logger.info("Starting server...")

        # Start the internal pubsub, to receive subscription callbacks

        if not usethread:
            self.pubsub.start(ev_quit=self.ev_quit)

        else:
            # Use one of our threadPool to run the server
            t = Task.FuncTask2(self.pubsub.subscribe_loop, self.ev_quit)
            t.init_and_start(self)

    def stop_server(self, wait=True, timeout=None):
        self.logger.info("Stopping server...")
        self.pubsub.stop()


class EnvelopeWrapper(object):
    """Simple class to wrap pubsub envelopes in so that they can be
    inserted and retrieved from a Queue.PriorityQueue.
    """

    def __init__(self, envelope):
        self.envelope = envelope

    def __lt__(self, other):
        return self.envelope['priority'] < other.envelope['priority']


class ChannelMap(object):

    def __init__(self):
        self.channels = set([])
        self.aggregates = dict()
        self.constituents = dict()

    def add_channel(self, channel):
        self.channels.add(channel)

    def add_channels(self, channels):
        self.channels = self.channels.union(set(channels))

    def aggregate(self, channel, channels):
        """
        Establish a new aggregate channel (channel) based on a group of
        other channels (channels).  (channels) may contain aggregate or
        non-aggregate channels.
        """
        self.aggregates[channel] = set(channels)

        #self.recalc_consitutents()

    def calc_constituents(self, channel):
        """
        Returns the set of subaggregate and nonaggregate channels
        associated with the channel.
        """
        def _calc_constituents(channel, visited):
            res = set([])

            # Only process this if it is an aggregate channel and we haven't
            # visited it yet
            if (channel not in self.aggregates) or (channel in visited):
                return res

            visited.add(channel)

            # For each subchannel in our aggregate set:
            #   - add it to the results
            #   - if IT is an aggregate, recurse and add its constituents
            for sub_ch in self.aggregates[channel]:
                res.add(sub_ch)
                if sub_ch in self.aggregates:
                    res.update(_calc_constituents(sub_ch, visited))

            return res

        res = _calc_constituents(channel, set([]))
        res.add(channel)
        return res

    def recalc_constituents(self):
        self.constituents = dict()
        for channel in self.channels:
            self.constituents[channel] = self.calc_constituents(channel)


# END
