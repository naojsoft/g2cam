#
# PubSub.py -- Subaru Remote Objects Publish/Subscribe module
#
"""
Main issues to think about/resolve:

  [ ] Bidirectional channel subscriptions
  [X] Local subscriber callbacks
  [X] Ability to set up ad-hoc channels based on aggregate channels;
        e.g. TaskManager needs to pull combined feed
  [ ] Permissions/access issues
"""

import sys
import os
import time
import itertools
import logging
import random
import threading
import traceback
from collections import deque as Deque
import queue

from g2base import Bunch, Task, ssdlog
from g2base.remoteObjects import remoteObjects as ro
from g2base.remoteObjects import Timer
from g2base.remoteObjects.ro_config import *

version = '20201115.0'

# Subscriber options
TWO_WAY = 'bidirectional'
CH_ALL  = '*'

#: What a pubsub listens for when it is not told otherwise.
#:
#: XML-RPC stays first, so it remains the primary in the registration and an
#: un-upgraded publisher finds it exactly where it always was; g2rpc-tcp sits
#: alongside for anything that can speak it, and a publisher takes the faster
#: of the two without being configured to.  Measured here, a delivery costs
#: 589us over XML-RPC against 272us over g2rpc-tcp, and g2rpc-tcp carries
#: msgpack, which is 2.1-2.4x faster to encode than json on these payloads
#: and about a quarter smaller on the wire.
#:
#: Listening two ways costs one permanent worker more than listening one
#: way; see the check in :py:meth:`PubSub.start_server`.
default_pubsub_transport = ['xmlrpc', 'g2rpc-tcp']


class PubSubError(Exception):
    """General class for exceptions raised by this module.
    """
    pass

class PubSub:
    """Base class for publish/subscribe entities.
    """

    ######## INTERNAL METHODS ########

    def __init__(self, name, logger,
                 ev_quit=None, threadPool=None, numthreads=30,
                 minthreads=None, outlimit=4, inlimit=12):
        """
        Constructor for the PubSubBase class.
            name        pubsub name
            logger      logger to be used for any diagnostic messages
            threadPool  optional, threadPool for serving PubSub activities
            numthreads  if a threadPool is NOT furnished, the most threads
                          to allocate
            minthreads  if a threadPool is NOT furnished, the fewest to
                          keep.  Defaults to numthreads, which makes the
                          pool a fixed size, as it always was.

        A pubsub permanently occupies a worker for each delivery daemon,
        one for the subscription loop, one for the server's start task and
        one for each threaded listener -- about nine of them for a pubsub
        listening two ways.  Everything above that is only wanted while
        calls are in flight, so a pool asked to start at `minthreads` finds
        its own level there and gives the rest back: measured, a pubsub
        started at 2 settles at 9 and delivers exactly as fast as one
        holding 30.  What must still be large enough is `numthreads`, since
        that is the ceiling on calls being served at once.
        """

        super().__init__()

        self.logger = logger
        self.name = name
        self.numthreads = numthreads
        # this limits the number of incoming and outgoing connections
        self.outlimit = outlimit
        self.inlimit = inlimit
        self.outqueue = queue.PriorityQueue()
        # A tiebreaker for the queue.  Its items are (priority, record), and
        # two equal priorities send Python on to compare the records -- which
        # reach the payload dicts and raise TypeError, taking the update with
        # them.  Equal priorities are not hypothetical: the two backlog
        # requeues below share one timestamp, so the exposure is worst while
        # a subscriber is catching up after a failure.  A counter between the
        # two keeps them apart and preserves FIFO order within a priority.
        self._seq = itertools.count()

        # Handles to subscriber remote proxies
        self._partner = {}
        # Defines aggregate channels
        self.aggregates = Bunch.threadSafeBunch()

        # Termination event
        if not ev_quit:
            ev_quit = threading.Event()
        self.ev_quit = ev_quit

        # If we were passed in a thread pool, then use it.  If not,
        # make one.  Record whether we made our own or not.
        if threadPool is not None:
            self.threadPool = threadPool
            self.mythreadpool = False

        else:
            self.threadPool = Task.ThreadPool(logger=self.logger,
                                              ev_quit=self.ev_quit,
                                              numthreads=self.numthreads,
                                              minthreads=minthreads)
            self.mythreadpool = True

        # used for delaying redeliveries
        self.tfact = Timer.TimerFactory(ev_quit=self.ev_quit)

        # For task inheritance:
        self.tag = 'PubSub'
        self.shares = ['logger', 'threadPool']

        # For handling subscriber info
        self._lock = threading.RLock()
        self._proxy_lock = threading.RLock()
        self._sub_info = {}

        # Holds proxies to other pubsubs
        self._proxyCache = {}

        # Holds information about remote subscriptions
        #self._remote_sub_info = set([])
        self._remote_sub_info = {}

        # Timeout for remote updates
        self.remote_timeout = 10.0

        # Interval between remote subscription updates
        self.update_interval = 10.0

        # number of seconds to wait before unsubscribing a subscriber
        # who is unresponsive
        self.failure_limit = 30.0
        # Initial delay assigned after a delivery failure.  It used to be a
        # millisecond, which is below any round trip worth the name: the
        # first few retries were spent before the network could have
        # changed its mind.
        self.redelivery_delay = 0.05
        # How much to spread retries either side of the computed delay, as a
        # fraction of it.  Without this every partner that failed on the
        # same tick -- which is what a name service or a host restarting
        # looks like -- retries in lockstep, and keeps doing so all the way
        # up the backoff.
        self.redelivery_jitter = 0.5
        # delay is increased by this factor with each subsequent failure
        self.redelivery_increase_factor = 2.0
        # delay is decreased by this factor with each subsequent success
        self.redelivery_decrease_factor = 0.6666
        # this is the maximum delivery delay
        self.max_delivery_delay = 1.0

        # The most updates to carry in one call to a subscriber.  Batches
        # form on their own while a call to that subscriber is in flight, so
        # this is a ceiling rather than a target: under light traffic nothing
        # accumulates and nothing is delayed.
        self.batch_limit_num = 100

        # warn us when the outgoing queue size exceeds this
        self.qlen_warn_limit = 100
        # but don't warn us more often than this interval
        self.qlen_warn_interval = 10.0

        self.cb_subscr_cnt = 0


    def _debug(self, fmt, *args):
        """Log at debug level without building the message first.

        The delivery path formatted its arguments into a string *before*
        calling the logger, so a 120-key status value cost 17.5us to render
        on every update whether or not anyone was reading debug output --
        half the cost of the update itself.

        Loggers here are not all :py:class:`logging.Logger`; some are the
        minimal stand-ins from remoteObjects, which take a single string and
        have no isEnabledFor.  So ask, and fall back to formatting for the
        ones that cannot say.
        """
        logger = self.logger
        enabled = getattr(logger, 'isEnabledFor', None)
        if enabled is not None and not enabled(logging.DEBUG):
            return
        logger.debug(fmt % args if args else fmt)

    def get_threadPool(self):
        return self.threadPool


    def add_channel(self, channelName):
        with self._lock:
            self._get_channelInfo(channelName, create=True)


    def add_channels(self, channelNames):
        with self._lock:
            for channelName in channelNames:
                self._get_channelInfo(channelName, create=True)


    def get_channels(self):
        with self._lock:
            return list(self._sub_info.keys())


    def loadConfig(self, moduleName):
        with self._lock:
            try:
                self.logger.debug("Loading configuration from '%s'" % (
                        moduleName))
                cfg = my_import(moduleName)

            except ImportError as e:
                raise PubSubError("Can't load configuration '%s': %s" % (
                        moduleName, str(e)))

            if hasattr(cfg, 'setup'):
                cfg.setup(self)


    def loadConfigs(self, moduleList):
        for moduleName in moduleList:
            self.loadConfig(moduleName)


    def _get_channelInfo(self, channelName, create=True):
        # Should only get called from within a lock!

        if channelName in self._sub_info:
            return self._sub_info[channelName]

        elif create:
            # Need to create subscriber bunch for this channel.
            bunch = Bunch.Bunch(channel=channelName,
                                subscribers=set([]),
                                computed_channels=set([channelName]),
                                computed_subscribers=set([]))
            self._sub_info[channelName] = bunch
            return bunch

        else:
            raise PubSubError("No such channel exists: '%s'" % channelName)


    def _subscribe(self, subscriber, proxy_obj, channels, options):
        """Add a subscriber (named by _subscriber_ and accessible via
        object _proxy_obj_) to channel (or list of channels) _channels_
        and with options _options_.

        This is an internal method.  See class 'PubSub' and its method
        subscribe() for a public interface.
        """
        channels = set(channels)

        can_unsubscribe = True
        if isinstance(options, dict):
            # Does subscriber allow us to unsubscribe them if they are
            # unreachable?  Default=True
            if 'unsub' in options:
                can_unsubscribe = options['unsub']

        with self._lock:
            try:
                partner = self._partner[subscriber]
                if partner is None:
                    raise KeyError("Partner for subscriber '%s' is None" % (
                        subscriber))
                partner.proxy = proxy_obj

            except KeyError:
                # Record proxy in _partner table
                partner = Bunch.Bunch(proxy=proxy_obj,
                                      subscribe_options=options,
                                      #lock=threading.RLock(),
                                      time_failure=None,
                                      delivery_delay=self.redelivery_delay,
                                      timer=self.tfact.timer(),
                                      backlog=Deque(maxlen=1000),
                                      # Updates waiting to go to this
                                      # subscriber, oldest first, and
                                      # whether a call to it is in flight.
                                      # One at a time is what keeps us from
                                      # shuffling them on the way out.
                                      pending=Deque(),
                                      sending=False,
                                      queued=False,
                                      # Whether this run of failures has
                                      # already had the proxy replaced.
                                      proxy_rebuilt=False,
                                      # Updates the backlog had to drop
                                      # because it was full.
                                      dropped=0,
                                      # Cleared the first time a batch call
                                      # is refused, which is how an
                                      # un-upgraded subscriber is spotted.
                                      takes_batches=True,
                                      can_unsubscribe=can_unsubscribe)
                partner.timer.add_callback('expired',
                                           self._timer_cb, subscriber, partner)
                self._partner[subscriber] = partner

            for channel in channels:
                bunch = self._get_channelInfo(channel, create=True)
                bunch.subscribers.add(subscriber)

            # Compute subscriber relationships
            self.compute_subscribers()


    def _unsubscribe(self, subscriber, channels, options):
        """Delete a subscriber (named by _subscriber_) to channel (or list
        of channels) described by _channels_.

        This is an internal method.  See class 'PubSub' and its method
        unsubscribe() for a public interface.
        """
        channels = set(channels)

        with self._lock:
            for channel in channels:

                bunch = self._get_channelInfo(channel)

                try:
                    bunch.subscribers.remove(subscriber)

                except KeyError:
                    #raise PubSubError("No subscriber '%s' to channel '%s'" % (
                    #    subscriber, channel))
                    # For now, silently ignore requests to unsubscribe from channels
                    # they are not a member of
                    pass

            # Compute subscriber relationships
            self.compute_subscribers()


    def remove_subscriber(self, subscriber):
        channels = self.get_channels()

        self._unsubscribe(subscriber, channels, [])

        # Delete proxy entry
        with self._lock:
            try:
                del self._partner[subscriber]
            except KeyError:
                pass


    def _named_update(self, value, names, channels, priority=0):
        """
        Internal method to push a _value_.  _names_ are the
        names of the pubsubs doing the updating.  _channels_ is the
        channel(s) to which this update applies.
        """
        self._debug("update: names=%s, channels=%s value=%s",
                    names, channels, value)

        self._subscriber_update(value, names, channels, priority)


    def _subscriber_update(self, value, names, channels, priority):
        """
        Internal method to update all subscribers who would be affected
        by these channels.  Triggered by a _named_update() call, which creates
        a task to call this method via the thread pool.  The update includes
        any local objects or remote objects by proxy.
        """
        self._debug("subscriber update: names=%s, channels=%s value=%s",
                    names, channels, value)

        # Get a list of partners that we should update for this value
        subscribers, all_channels = self._get_subscribers(channels)
        # sets don't go across remoteObjects (yet)
        all_channels = list(all_channels)

        self._debug("subscribers for channel=%s are %s",
                    channels, subscribers)

        # Add ourself to the set of names (prevents cyclic updates)
        if self.name in names:
            updnames = names
        else:
            updnames = names[:]
            updnames.append(self.name)

        # Update them.  Silently log errors.
        for subscriber in subscribers:
            # Don't update any originators.
            if subscriber in names:
                continue

            with self._lock:
                try:
                    partner = self._partner[subscriber]
                    if partner is None:
                        raise KeyError("Partner for subscriber '%s' is None" % (
                            subscriber))

                except KeyError:
                    self.logger.warn("No information for subscriber '%s': dropping them" % (
                        subscriber))
                    self.remove_subscriber(subscriber)
                    continue

                queue_record = (subscriber, value, updnames,
                                all_channels, priority)

                # If there is a current failure indicated for this subscriber
                # then add this update to the partner's backlog
                if partner.time_failure is not None:
                    self._backlog_add(subscriber, partner, [queue_record])
                    continue

                partner.pending.append(queue_record)
                if partner.sending or partner.queued:
                    # Somebody is already on their way to this subscriber
                    # and will take this with them.
                    continue
                partner.queued = True

            # queue the subscriber, not the update: what to send is whatever
            # has gathered by the time a delivery thread gets there.
            self._enqueue(time.time() + priority, subscriber)


    def _send_batch(self, subscriber, partner, proxy_obj, records):
        """Hand a run of updates to one subscriber, in order.

        One call carries them all where the subscriber can take one, and
        that is most of what batching buys: the round trip is paid once
        rather than per update.

        Whether it can take one is discovered by asking rather than by
        announcing.  A subscriber that has not been upgraded has no
        remote_update_many, so the call is refused; the same records then go
        one at a time, and if *that* works the refusal was about the method
        and not about the subscriber, so we stop offering.  A subscriber
        that is really unreachable fails both ways and is handled as a
        failure always has been.
        """
        if len(records) > 1 and partner.takes_batches:
            updates = [(value, names, channels)
                       for _sub, value, names, channels, _pri in records]
            try:
                proxy_obj.remote_update_many(updates)
                return True

            except Exception as e:
                self._debug("subscriber '%s' would not take a batch of %d: "
                            "%s", subscriber, len(updates), e)

        # One at a time, in the order they were published.
        for _sub, value, names, channels, _pri in records:
            proxy_obj.remote_update(value, names, channels)

        if len(records) > 1 and partner.takes_batches:
            # They took the updates but not the batch, so the batch call is
            # what they lack.  Stop paying for the attempt.
            with self._lock:
                partner.takes_batches = False
            self.logger.info("subscriber '%s' does not take batched updates; "
                             "sending them singly from now on" % (subscriber,))
        return True

    def _individual_update(self, subscriber):
        """Deliver whatever has gathered for one subscriber.

        Called with a subscriber rather than an update, because what to send
        is whatever accumulated while the last call to them was in flight.
        Only one delivery thread is ever inside here for a given subscriber,
        which is what stops us handing their updates over in a different
        order from the one they were published in.
        """
        partner = None
        with self._lock:
            try:
                partner = self._partner[subscriber]
                if partner is None:
                    raise KeyError("Partner for subscriber '%s' is None" % (
                        subscriber))

            except KeyError:
                self.logger.warn("No information for subscriber '%s': dropping them" % (
                    subscriber))
                self.remove_subscriber(subscriber)
                return 0

            partner.queued = False
            if partner.sending:
                # Somebody is already on their way to this subscriber, and
                # will take whatever is waiting.  Only one of us at a time
                # is what keeps their updates in order.
                return 0

            records = []
            while partner.pending and len(records) < self.batch_limit_num:
                records.append(partner.pending.popleft())
            if not records:
                return 0

            partner.sending = True
            proxy_obj = partner.proxy

        value, names, channels = records[0][1], records[0][2], records[0][3]
        self._debug("attempting to update subscriber '%s' on channels(%s)"
                    "  with %d update(s), first value: %s",
                    subscriber, channels, len(records), value)

        success = False
        try:
            success = self._send_batch(subscriber, partner, proxy_obj,
                                       records)

        except Exception as e:
            # TODO: capture and log traceback
            self.logger.error("cannot update subscriber '%s': %s" % (
                subscriber, str(e)))

        # failure to deliver update!
        with self._lock:
            # Whoever comes next for this subscriber is free to go.
            partner.sending = False

            # Claiming `queued` and then not queuing anything is how a
            # partner gets stranded: nothing will claim it again, and
            # nothing is on the way.  So it is only claimed where the
            # enqueue certainly follows -- which is here, on success, and
            # not on the failure path, where the timer decides when we next
            # try and the backlog holds what to send.
            more = None
            if success and partner.pending and not partner.queued:
                partner.queued = True
                more = subscriber

            if success:
                # successful update

                # decrease future delays by decrease retry factor
                partner.delivery_delay = max(self.redelivery_delay,
                                             partner.delivery_delay *
                                             self.redelivery_decrease_factor)

                backlog_n = len(partner.backlog)
                if (partner.time_failure is None) and (backlog_n == 0):
                    # no current outstanding failures
                    if more is not None:
                        self._enqueue(time.time(), more)
                    return

                if backlog_n == 0:
                    partner.time_failure = None
                    partner.proxy_rebuilt = False
                    self.logger.info("subscriber '%s' backlog caught up" % (
                        subscriber))
                    if more is not None:
                        self._enqueue(time.time(), more)
                    return

                # <-- there is a backlog and a history of failure

                # partner gets a new lease on life
                cur_time = time.time()
                partner.time_failure = cur_time

                try:
                    queue_record = partner.backlog.popleft()

                except IndexError:
                    # backlog is empty--this should NOT happen due to test above
                    partner.time_failure = None
                    return

                # Put it back at the front of what is waiting, so the
                # backlog drains ahead of anything published since, and
                # send for this subscriber.
                partner.pending.appendleft(queue_record)
                if not partner.queued:
                    partner.queued = True
                    self._enqueue(cur_time, subscriber)

            else:
                # failure!
                # the whole run goes to the backlog, in the order it was
                # published, so that nothing is reordered by having failed
                self._backlog_add(subscriber, partner, records)

                delivery_delay = min(self.max_delivery_delay,
                                     partner.delivery_delay)
                # Spread the actual firing without disturbing the schedule:
                # delivery_delay keeps doubling as before, and only when
                # the timer is set does chance get a say.
                spread = delivery_delay * self.redelivery_jitter
                delivery_delay = max(0.0, delivery_delay
                                     + random.uniform(-spread, spread))
                # increase future delays by increase retry factor
                partner.delivery_delay = min(self.max_delivery_delay,
                                             partner.delivery_delay *
                                             self.redelivery_increase_factor)

                if partner.time_failure is None:
                    # no existing failure in effect--so no timer running
                    partner.time_failure = time.time()

                # set up a delay before retrying
                self.logger.debug("setting timer")
                partner.timer.cond_set(delivery_delay)

    def _backlog_add(self, subscriber, partner, records):
        """Add to a partner's backlog, and notice what falls off the end.

        The backlog is a bounded deque, so appending to a full one discards
        from the front.  Dropping the oldest is the right end for status --
        what a subscriber missed matters less than where things now stand --
        but it happened silently, and a subscriber could lose hundreds of
        updates with nothing said.
        """
        room = partner.backlog.maxlen
        if room is not None:
            overflow = len(partner.backlog) + len(records) - room
            if overflow > 0:
                had = partner.dropped
                partner.dropped += overflow
                if had == 0 or partner.dropped // 1000 != had // 1000:
                    self.logger.warning(
                        "backlog for subscriber '%s' is full (%d); dropping "
                        "the oldest updates -- %d lost so far"
                        % (subscriber, room, partner.dropped))

        partner.backlog.extend(records)

    def _timer_cb(self, timer, subscriber, partner):

        def __requeue(subscriber, partner):
            # pull an update off of the backlog and requeue it
            with self._lock:
                self.logger.debug("timer expired, checking updates for subscriber '%s'" % (subscriber))
                cur_time = time.time()

                if partner.time_failure is not None:
                    # already failing--should we give on this subscriber?
                    failure_interval = cur_time - partner.time_failure
                    if failure_interval > self.failure_limit:
                        if partner.can_unsubscribe:
                            self.logger.warning("subscriber '%s' failure interval has exceeded limit--unsubscribing them" % (
                                subscriber))
                            self.remove_subscriber(subscriber)
                            return

                try:
                    self.logger.info("backlog for subscriber '%s' is %d" % (
                        subscriber, len(partner.backlog)))
                    queue_record = partner.backlog.popleft()

                except IndexError:
                    # backlog is empty
                    partner.time_failure = None
                    partner.proxy_rebuilt = False
                    return

                # A proxy looked up by name already re-resolves itself
                # when a call fails -- call_failover() asks the name
                # service again and tries the other providers -- so
                # rebuilding it on every retry threw away one that had just
                # healed and paid for another lookup to get back where we
                # were.  Once per episode is enough to clear a proxy that
                # is genuinely stale.
                if not partner.proxy_rebuilt:
                    partner.proxy_rebuilt = True
                    self.proxy_error(subscriber, partner)

                # Ahead of anything published since, so the backlog keeps
                # its order relative to itself.
                partner.pending.appendleft(queue_record)
                already_queued = partner.queued
                partner.queued = True

            if not already_queued:
                self._enqueue(cur_time, subscriber)

        task = Task.FuncTask(__requeue, [subscriber, partner], {},
                             logger=self.logger)
        task.init_and_start(self)

    def _enqueue(self, priority, queue_record):
        """Put an update on the delivery queue.

        The sequence number is what stops two equal priorities being settled
        by comparing the records themselves, which ends at the payload dicts
        and raises.
        """
        self.outqueue.put((priority, next(self._seq), queue_record))

    def _delivery_daemon(self, i):
        last_warn = time.time()

        while not self.ev_quit.isSet():
            n = self.get_qlen()
            cur_time = time.time()
            if ((i == 0) and (n > self.qlen_warn_limit) and
                (cur_time - last_warn > self.qlen_warn_interval)):
                self.logger.warn("Queue size %d exceeds limit %d" % (
                    n, self.qlen_warn_limit))
                last_warn = cur_time

            try:
                priority, _seq, subscriber = self.outqueue.get(True, 0.25)

                self._individual_update(subscriber)

            except queue.Empty:
                continue

    def get_qlen(self):
        return self.outqueue.qsize()

    def get_qelts(self):
        # (priority, subscriber) for each subscriber with updates waiting;
        # elt is (priority, sequence, subscriber).
        res = [ (elt[0], elt[2]) for elt in self.outqueue.queue ]
        return res

    ######## PUBLIC METHODS ########

    def start(self, wait=True):
        """Start any background threads, etc. used by this pubsub.
        """

        #self.ev_quit.clear()

        # Start our thread pool (if we created it)
        if self.mythreadpool:
            self.threadPool.startall(wait=wait)

        # Start up the timer factory
        self.tfact.wind()

        # Start up delivery daemons
        for i in range(self.outlimit):
            self.threadPool.addTask(Task.FuncTask2(self._delivery_daemon, i))

        self.logger.info("PubSub background tasks started.")


    def stop(self, wait=True):
        """Stop any background threads, etc. used by this pubsub.
        """
        self.tfact.quit()

        # Stop our thread pool (if we created it)
        if self.mythreadpool:
            self.threadPool.stopall(wait=wait)

        self.logger.info("PubSub background tasks stopped.")


    def aggregate(self, channel, channels):
        """
        Establish a new aggregate channel (channel) based on a group of
        other channels (channels).  (channels) may contain aggregate or
        non-aggregate channels.
        """
        with self._lock:
            self.aggregates[channel] = set(channels)

            # Update subscriber relationships
            self.compute_subscribers()


    def deaggregate(self, channel):
        """
        Delete an aggregate channel (channel).
        """
        with self._lock:
            self.aggregates.delitem(channel)

            # Update subscriber relationships
            self.compute_subscribers()


    def _get_constituents(self, channel, visited):
        """
        Internal helper function used by the 'get_constituents' method.
        """

        res = set([])

        with self._lock:
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
                    res.update(self._get_constituents(sub_ch, visited))

            return res


    def get_constituents(self, channel):
        """
        Returns the set of subaggregate and nonaggregate channels associated with the
        channel.
        """
        res = self._get_constituents(channel, set([]))

        return list(res)


    def compute_subscribers(self):
        """
        Internal helper function used by the subscribe(), unsubscribe(),
        aggregate() and deaggregate(), methods.
        """

        with self._lock:
            # PASS 1
            # For each channel, initialize its set of computed subscribers
            # to the explicitly subscribed set
            for channel in self.get_channels():
                bunch = self._get_channelInfo(channel)
                bunch.computed_subscribers = bunch.subscribers.copy()
                if self.name in bunch.computed_subscribers:
                    bunch.computed_subscribers.remove(self.name)

            # PASS 2 (aggregates only)
            # For each *aggregate* channel, get the constituents
            # and add any of the aggregate's subscribers to the non-aggregate
            # channels' computed_subscribers.
            for agg_channel in list(self.aggregates.keys()):

                # Get my subscribers
                bunch = self._get_channelInfo(agg_channel)
                my_subscribers = bunch.subscribers
                #my_subscribers = bunch.computed_subscribers
                #self.logger.debug("subscribers(%s) = %s" % (agg_channel,
                #                                            list(my_subscribers)))

                # Get my constituents
                constituents = self.get_constituents(agg_channel)
                #self.logger.debug("constituents(%s) = %s" % (agg_channel,
                #                                             list(constituents)))

                for constituent in constituents:
                    # Add aggregate channel's subscribers to constituent's
                    bunch = self._get_channelInfo(constituent)
                    bunch.computed_subscribers.update(my_subscribers)
                    # Add aggregate channel name to consituent's
                    bunch.computed_channels.add(agg_channel)

                    # Remove self to avoid circular loops
                    if self.name in bunch.computed_subscribers:
                        bunch.computed_subscribers.remove(self.name)

            # PASS 3 (DEBUG ONLY)
            #for channel in self.get_channels():
            #    bunch = self._get_channelInfo(channel)
            #    self.logger.debug("%s --> %s" % (channel,
            #                                     str(list(bunch.computed_subscribers))))

    def _get_subscribers(self, channels):
        """Get the list of subscriber names that match subscriptions for
        a given channel or channels AND get the list of all channels that
        this aggregates to.
        """
        if isinstance(channels, str):
            channels = [channels]
        self._debug("channels=%s", channels)

        with self._lock:
            # Optomization for case where there is only one channel
            if len(channels) == 1:
                channel = channels[0]
                try:
                    bunch = self._sub_info[channel]

                    return (bunch.computed_subscribers, bunch.computed_channels)

                except KeyError:
                    return (set([]), set([]))

            else:
                # Otherwise we have to compute the union of all the channels
                # computed subscribers
                subscribers = set([])
                all_channels = set([])

                for channel in channels:
                    try:
                        bunch = self._sub_info[channel]
                        subscribers.update(bunch.computed_subscribers)
                        all_channels.update(bunch.computed_channels)

                    except KeyError:
                        continue

                # Remove self to avoid circular loops (shouldn't need to do this
                # because it should have already been done in compute_subscribers)
                if self.name in subscribers:
                    subscribers.remove(self.name)

                return (subscribers, all_channels)


    def get_subscribers(self, channels):
        """
        remoteObjects callable version of _get_subscribers (currently sets are not
        supported on XML-RPC, so we cannot guarantee that apps written in other
        languages will be able to access it).
        """
        subscribers, all_channels = self._get_subscribers(channels)

        return (list(subscribers), list(all_channels))


    def _monitor_update(self, value, names, channels, priority):
        # update monitor
        self.monitor_update(value, names, channels)

        # if successful, update our subscribers
        self._named_update(value, names, channels, priority=priority)


    def remote_update(self, value, names, channels):
        """method called by another PubSub to update this one
        with new and changed items.
        """
        # Avoid cyclic dependencies--don't update ourselves if we
        # originated this event
        if self.name in names:
            return ro.OK

        # We are already running on a worker: the RPC server handed this
        # call to one.  Storing the value takes microseconds and passing it
        # on only puts it on the delivery queue, so handing either to a
        # second thread costs far more than doing them here.
        #
        # What the task did do is swallow failures, and that is kept
        # deliberately.  Letting one reach the publisher sounds better --
        # it would retry from its backlog -- but a value this subscriber
        # cannot store is not one the publisher can fix by sending again,
        # and a partner marked failed has every later update queued behind
        # the bad one.  So one poison value would stall the whole feed while
        # it was retried.  For a status stream, moving on is worth more than
        # not losing one value.
        try:
            if hasattr(self, 'monitor_update'):
                self._monitor_update(value, names, channels, 0)
            else:
                self._named_update(value, names, channels, priority=0)

        except Exception as e:
            # No exc_info=: the loggers passed in here are not all
            # logging.Logger, and the minimal ones take a message and
            # nothing else -- so asking for a traceback would raise inside
            # the handler that exists to stop things raising.
            self.logger.error(
                "update from %s on channels %s could not be handled: %s\n%s"
                % (names, channels, e, traceback.format_exc()))

        return ro.OK


    def remote_update_many(self, updates):
        """Several updates from one publisher, oldest first.

        The batched form of :py:meth:`remote_update`, and the reason
        batching is worth anything: one call carries a run of updates that
        gathered while the last call was in flight, so the round trip is
        paid once rather than per update.

        They are applied in the order they were published.  Nothing on a
        network guarantees that order across calls, but there is no reason
        for us to lose it within one.

        A publisher discovers whether a subscriber has this by calling it;
        one that has not been upgraded refuses, and is sent single updates
        from then on.
        """
        for update in updates:
            value, names, channels = update
            self.remote_update(value, names, channels)

        return ro.OK

    def setup_batch(self, limit_sec=None, limit_num=100):
        """Set how many updates may travel together in one call.

        Batches gather on their own: an update goes out immediately unless a
        call to that subscriber is already in flight, in which case it waits
        for the next one and travels with whatever else arrived meanwhile.
        So nothing is ever delayed to make a batch, and this is a ceiling
        rather than a target -- under light traffic none form at all.

        :param limit_sec: **Accepted and ignored.**  It is here because
            services already pass it -- ``setup_batch(0.1)`` and
            ``setup_batch(0.25)`` appear in a dozen places -- and because
            those calls have never done anything: this was a stub until
            batching existed.

            Honouring it would mean holding updates back for a window,
            which buys fewer calls at the price of that much latency on
            every update after the first.  Batches here already cost no
            latency, so there is nothing to buy and a synchronisation
            system is the wrong place to spend a tenth of a second.  If you
            do want a floor on the call rate, ask and it can be added as
            its own thing rather than smuggled in behind an argument that
            has meant nothing for years.
        :param limit_num: The most updates to carry in one call.
        """
        if limit_num is not None:
            self.batch_limit_num = max(1, int(limit_num))

    def notify(self, value, channels, priority=0):
        """
        Method called by local users of this PubSub to update it
        with new and changed items.
            value
            channels    one (a string) or more (a list) of channel names to
                        which to send the specified update

        The work done here is only working out who wants this value and
        putting it on the delivery queue -- 3.5us for a small one -- and the
        sending is the delivery threads' job either way.  Handing that to
        the thread pool cost 63us to defer 3.5us of work, so the caller
        waited longer for the hand-off than for the thing itself.

        The task also swallowed anything that went wrong in there, and that
        is kept: publishing a value should not fail the caller's own work
        over a fault in the machinery carrying it.  It matters more than it
        looks -- notify() and update() are public, so the method scan
        exposes them, and the caller can be a remote one.
        """
        names = [ self.name ]
        try:
            self._named_update(value, names, channels, priority=priority)

        except Exception as e:
            self.logger.error("could not publish to channels %s: %s\n%s"
                              % (channels, e, traceback.format_exc()))

    def clear_proxy_cache(self):
        with self._lock:
            self._proxyCache = {}

    def proxy_error(self, subscriber, partner):
        self.remove_proxies([subscriber])

        # Try to rebuild this proxy
        proxy = self._getProxy(subscriber, partner.subscribe_options)
        with self._lock:
            partner.proxy = proxy

    def remove_proxies(self, nameList):
        """Remove remoteObject proxies to remote pubsubs (subscribers).
        """
        with self._proxy_lock:
            for name in nameList:
                try:
                    del self._proxyCache[name]
                except KeyError:
                    # already deleted?  in any case, it's ok
                    pass

    def _getProxy(self, subscriber, options):
        """Internal method to create & cache remoteObject proxies to remote
        pubsubs (subscribers).
        """
        try:
            # If we already have a proxy for the _svcname_, return it.
            with self._proxy_lock:
                return self._proxyCache[subscriber]

        except KeyError:
            # Create a new proxy to the external pubsub and cache it

            # Fill in possible authentication and security params
            kwdargs = { 'timeout': self.remote_timeout }
            #kwdargs = {}
            if 'auth' in options:
                kwdargs['auth'] = options['auth']
            if 'secure' in options:
                kwdargs['secure'] = options['secure']
            if 'transport' in options:
                kwdargs.update(self._transport_options(subscriber,
                                                       options['transport']))

            # subscriber can be a service name or a host:port
            if ':' not in subscriber:
                proxy_obj = ro.remoteObjectProxy(subscriber, **kwdargs)
            else:
                (host, port) = subscriber.split(':')
                port = int(port)
                proxy_obj = ro.remoteObjectClient(host, port, **kwdargs)

            with self._lock:
                self._proxyCache[subscriber] = proxy_obj
            self.logger.debug("Created proxy for '%s'" % (subscriber))

            return proxy_obj


    def _transport_options(self, subscriber, transport):
        """Turn a 'transport' option into what a proxy understands.

        A **list** is an order of preference, and cannot fail: whatever the
        subscriber offers that we both know is used, and its registration
        decides the rest.  A **string** pins one protocol, which is what to
        say when only one will do -- and means a subscriber that does not
        offer it is unreachable rather than reached more slowly.

        Nothing has to be said at all.  Left out, a proxy follows the
        subscriber's registration and takes the fastest way in it offers,
        which is usually what was wanted.
        """
        if isinstance(transport, (list, tuple)):
            return {'prefer': list(transport)}

        self.logger.debug(
            "subscriber '%s' pinned to transport '%s'; it will be "
            "unreachable if it does not offer that"
            % (subscriber, transport))
        return {'transport': transport}

    def subscribe(self, subscriber, channels, options):
        """Register a subscriber (named by _subscriber_) for updates on
        channel(s) _channels_.

        This call is expected to be called via remoteObjects.
        """
        def __sub(subscriber, channels, options):
            self.logger.debug("registering '%s' as a subscriber for '%s'." % (
                subscriber, channels))

            if not options:
                options = {}

            if isinstance(channels, str):
                channels = [channels]

            try:
                proxy_obj = self._getProxy(subscriber, options)

                self._subscribe(subscriber, proxy_obj, channels, options)
                self.logger.debug("local registration of '%s' successful." % (
                    subscriber))

            except PubSubError as e:
                self.logger.error("registration of '%s' for '%s' failed: %s" % (
                    subscriber, channels, str(e)))

        task = Task.FuncTask2(__sub, subscriber, channels, options)
        task.init_and_start(self)

        return ro.OK


    def unsubscribe(self, subscriber, channels, options):
        """Unregister a subscriber (named by _subscriber_) for updates on
        channel(s) _channels_.

        This call is expected to be called via remoteObjects.
        """
        def __unsub(subscriber, channels, options):
            self.logger.debug("unregistering '%s' as a subscriber for '%s'." % (
                subscriber, channels))

            if not options:
                options = {}

            if isinstance(channels, str):
                channels = [channels]

            try:
                #proxy_obj = self._getProxy(subscriber, options)

                self._unsubscribe(subscriber, channels, options)
                self.logger.debug("local unregistration of '%s' successful." % (
                    subscriber))

            except PubSubError as e:
                self.logger.error("unregistration of '%s' for '%s' failed: %s" % \
                                 (subscriber, str(channels), str(e)))

        task = Task.FuncTask2(__unsub, subscriber, channels, options)
        task.init_and_start(self)

        return ro.OK


    def publish_to(self, subscriber, channels, options):
        """Register a subscriber (named by _subscriber_) for updates on
        channel(s) _channels_.

        This method is just a shortcut for subscribing someone with an
        option never to remove them.
        """
        options1 = options.copy()
        options1.setdefault('unsub', False)

        return self.subscribe(subscriber, channels, options1)


    def _subscribe_remote(self, publisher, channels, options):
        self.logger.debug("Subscribing ourselves to publisher %s channels=%s options=%s" % (
            publisher, str(channels), str(options)))
        try:
            # Fill in possible authentication and security params
            kwdargs = {}
            if 'pubauth' in options:
                kwdargs['auth'] = options['pubauth']
            if 'pubsecure' in options:
                kwdargs['secure'] = options['pubsecure']
            if 'pubtransport' in options:
                kwdargs.update(self._transport_options(publisher,
                                                       options['pubtransport']))
            if 'name' in options:
                name = options['name']
            else:
                name = self.name

            pub_proxy = self._getProxy(publisher, kwdargs)

            self.logger.debug("Subscribing %s to %s options=%s" % (
                    name, str(channels), str(options)))
            pub_proxy.subscribe(name, channels, options)

        except Exception as e:
            self.logger.error("registration via '%s' for '%s' failed: %s" % \
                             (publisher, str(channels), str(e)))


    def _unsubscribe_remote(self, publisher, channels, options):
        try:
            # Fill in possible authentication and security params
            kwdargs = {}
            if 'pubauth' in options:
                kwdargs['auth'] = options['pubauth']
            if 'pubsecure' in options:
                kwdargs['secure'] = options['pubsecure']
            if 'name' in options:
                name = options['name']
            else:
                name = self.name

            pub_proxy = self._getProxy(publisher, kwdargs)

            self.logger.debug("Unsubscribing %s to %s options=%s" % (
                    name, str(channels), str(options)))
            pub_proxy.unsubscribe(name, channels, options)

        except Exception as e:
            self.logger.error("unregistration via '%s' for '%s' failed: %s" % \
                             (publisher, str(channels), str(e)))


    def subscribe_remote(self, publisher, channels, options):

        # Necessary to add to a set; list objects are not hashable
        if type(channels) is str:
            channels = (channels,)
        elif type(channels) is list:
            channels = tuple(channels)
        assert(type(channels) is tuple)

        if not options:
            options = {}

        with self._lock:
            #self._remote_sub_info.add((publisher, channels, options))
            self._remote_sub_info[(publisher, channels)] = (publisher,
                                                            channels, options)

            self._subscribe_remote(publisher, channels, options)


    def unsubscribe_remote(self, publisher, channels, options):

        # Necessary to add to a set; list objects are not hashable
        if type(channels) is str:
            channels = (channels)
        elif type(channels) is list:
            channels = tuple(channels)
        assert(type(channels) is tuple)

        with self._lock:
            #self._remote_sub_info.remove((publisher, channels, options))
            del self._remote_sub_info[(publisher, channels)]

            self._unsubscribe_remote(publisher, channels, options)


    def subscribe_cb(self, fn_update, channels):
        """Register local subscriber callback (_fn_update_)
        for updates on channel(s) _channels_.

        This call is expected to be a local call.
        """
        if not callable(fn_update):
            raise PubSubError('subscriber functions must be callables')

        # TODO: make sure this is a unique name
        with self._lock:
            subscriber = fn_update.__name__ + str(self.cb_subscr_cnt)
            self.cb_subscr_cnt += 1

        self.logger.debug("registering '%s' as a subscriber for '%s'." % (
            subscriber, channels))

        class anonClass:
            def __init__(self, update, parent):
                self.update = update
                self.parent = parent

            def remote_update(self, value, names, channels):
                try:
                    # This is being called from a thread in the workers
                    self.update(value, names, channels)
                    return ro.OK
                    ## task = Task.FuncTask(self.update, (value, names, channels),
                    ##                      {}, logger=self.parent.logger)
                    ## task.init_and_start(self.parent)

                except Exception as e:
                    # Don't requeue local subscribers
                    self.parent.logger.error("Error updating local subscriber: %s" % (
                        str(e)))

        local_obj = anonClass(fn_update, self)

        self._subscribe(subscriber, local_obj, channels, {})
        self.logger.debug("local registration of '%s' successful." % (
            subscriber))


    def unsubscribe_cb(self, fn_update, channels):
        """Unregister a subscriber (_fn_update_) for updates on
        channel(s) _channels_.

        This call is expected to be a local call.
        """
        subscriber = fn_update.__name__
        self.logger.debug("unregistering '%s' as a subscriber for '%s'." % (
            subscriber, channels))

        self._unsubscribe(subscriber, channels, {})
        self.logger.debug("local unregistration of '%s' successful." % (
            subscriber))


    def _affordable_transports(self, transport, usethread, asked_for):
        """Trim the listeners to what the thread pool can actually serve.

        Every listener holds a worker for as long as the service runs, and
        so does each delivery daemon and the subscription loop.  When those
        add up to the whole pool there is nobody left to handle a request,
        and the service does not fail: it accepts calls and never answers,
        on every protocol at once, with nothing in the log.

        Listening two ways rather than one moves that threshold, so a pool
        that was adequate before this became the default may not be now.
        A default must not break a service that worked, so when nothing was
        asked for the extra listeners are dropped -- back to XML-RPC alone
        if need be, which is where such a service already was -- and the
        reason is logged.  When the transports *were* asked for, quietly
        ignoring them would be worse than refusing, so that raises.
        """
        listeners = ([transport] if isinstance(transport, str)
                     else list(transport))
        pool = getattr(self.threadPool, 'numthreads', None)
        if pool is None:
            return listeners

        # delivery daemons, the subscription loop, the server's own start
        # task, and one worker left over to answer a call with.
        reserved = self.outlimit + 1 + (1 if usethread else 0) + 1
        affordable = pool - reserved

        if affordable >= len(listeners):
            return listeners

        if asked_for or affordable < 1:
            raise PubSubError(
                "'%s' has a thread pool of %d, which cannot serve %d "
                "listener(s) alongside %d delivery daemon(s): it needs at "
                "least %d, or it would accept calls and never answer them.  "
                "Give the pubsub numthreads=%d, or fewer transports, or a "
                "smaller outlimit."
                % (self.name, pool, len(listeners), self.outlimit,
                   reserved + len(listeners), reserved + len(listeners)))

        kept = listeners[:affordable]
        self.logger.warning(
            "'%s' has a thread pool of %d, which can serve %d listener(s) "
            "alongside %d delivery daemon(s), so %s will be served and %s "
            "will not.  Give the pubsub numthreads=%d to serve them all."
            % (self.name, pool, affordable, self.outlimit, ','.join(kept),
               ','.join(listeners[affordable:]), reserved + len(listeners)))
        return kept

    # TODO: deprecate this and make apps create their own remoteObjectServer
    # with a delegate to this object??
    def start_server(self, svcname=None, host=None, port=None,
                     ping_interval=default_ns_ping_interval,
                     strict_registration=False,
                     threaded_server=default_threaded_server,
                     authDict=None, default_auth=use_default_auth,
                     secure=default_secure, cert_file=default_cert,
                     ns=None, transport=None,
                     usethread=True, wait=True, timeout=None):
        """Expose this pubsub for remote subscribers.

        :param transport: One protocol name, or a list of them with the
            most widely spoken first.  A list means this pubsub listens
            every one of those ways at once, so a publisher updating us
            reaches for the fastest it can speak while one that has not
            been upgraded still finds XML-RPC where it expects it.

            Nothing has to be negotiated for that: a publisher looks us up
            by name and the registration says what we answer to.

            Left out, :py:data:`default_pubsub_transport` is used, trimmed
            to what the thread pool can serve; named explicitly, a pool too
            small to serve them raises rather than quietly serving fewer.
        """
        if not svcname:
            svcname = self.name
        asked_for = transport is not None
        if not asked_for:
            transport = default_pubsub_transport
        transport = self._affordable_transports(transport, usethread,
                                                asked_for)
        # make our RO server for remote interface
        self.server = ro.remoteObjectServer(svcname=svcname, obj=self,
                                            logger=self.logger,
                                            ev_quit=self.ev_quit,
                                            host=host,
                                            port=port, usethread=usethread,
                                            threadPool=self.threadPool,
                                            threaded_server=threaded_server,
                                            numthreads=self.inlimit,
                                            transport=transport,
                                            authDict=authDict, default_auth=default_auth,
                                            secure=secure, cert_file=cert_file)

        self.server.ro_register_stacktraces_dump()

        self.logger.info("Starting remote subscriptions update loop...")
        t = Task.FuncTask(self.update_remote_subscriptions_loop, [], {},
                          logger=self.logger)
        t.init_and_start(self)

        self.logger.info("Starting server...")
        if not usethread:
            self.server.ro_start(wait=wait, timeout=timeout)

        else:
            # Use one of our threadPool to run the server
            t = Task.FuncTask(self.server.ro_start, [], {},
                              logger=self.logger)
            t.init_and_start(self)
            if wait:
                self.server.ro_wait_start(timeout=timeout)

    def stop_server(self, wait=True, timeout=None):
        self.logger.info("Stopping server...")
        #self.server.ro_stop(wait=wait, timeout=timeout)
        # This is not quite working correctly...
        self.server.ro_stop(wait=False, timeout=timeout)


    def update_remote_subscriptions_loop(self):

        while not self.ev_quit.isSet():
            time_end = time.time() + self.update_interval

            with self._lock:
                #tups = list(self._remote_sub_info)
                tups = self._remote_sub_info.values()

            self.logger.debug("updating remote subscriptions: %s" % (
                str(tups)))
            for tup in tups:
                try:
                    (publisher, channels, options) = tup

                    self._subscribe_remote(publisher, channels, options)

                except Exception as e:
                    self.logger.error("Error pinging remote subscription %s: %s" % (
                            str(tup), str(e)))

            # Sleep for remainder of desired interval.  We sleep in
            # small increments so we can be responsive to changes to
            # ev_quit
            cur_time = time.time()
            self.logger.debug("Waiting interval, remaining: %f sec" % \
                              (time_end - cur_time))

            while (cur_time < time_end) and (not self.ev_quit.isSet()):
                time.sleep(0)
                self.ev_quit.wait(min(0.1, time_end - cur_time))
                cur_time = time.time()

            self.logger.debug("End interval wait")

        self.logger.info("exiting remote subscriptions update loop")


def my_import(name):
    mod = __import__(name)
    components = name.split('.')
    for comp in components[1:]:
        mod = getattr(mod, comp)
    return mod


def main(options, args):

    # Create top level logger.
    logger = ssdlog.make_logger(options.svcname, options)

    # Initialize remote objects subsystem.
    try:
        ro.init()
        ro.write_pid_file(os.path.join('/tmp', options.svcname + '.pid'))

    except ro.remoteObjectError as e:
        logger.error("Error initializing remote objects subsystem: %s" % str(e))
        sys.exit(1)

    ev_quit = threading.Event()
    usethread=False

    # Create our pubsub and start it
    pubsub = PubSub(options.svcname, logger,
                    numthreads=options.numthreads,
                    outlimit=options.outlimit,
                    inlimit=options.inlimit)

    # Load configurations, if any specified
    if options.config:
        pubsub.loadConfigs(options.config.split(','))

    logger.info("Starting pubsub...")
    pubsub.start()
    try:
        try:
            pubsub.start_server(port=options.port, wait=True,
                                 usethread=usethread)

        except KeyboardInterrupt:
            logger.error("Caught keyboard interrupt!")

    finally:
        logger.info("Stopping pubsub...")
        if usethread:
            pubsub.stop_server(wait=True)
        pubsub.stop()


# END
