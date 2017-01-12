#!/usr/bin/env python
#
# PubSub.py -- Subaru Remote Objects Publish/Subscribe module
#
# Eric Jeschke (eric@naoj.org)
#

"""
Main issues to think about/resolve:

  [ ] Bidirectional channel subscriptions
  [X] Local subscriber callbacks
  [X] Ability to set up ad-hoc channels based on aggregate channels;
        e.g. TaskManager needs to pull combined feed
  [ ] Permissions/access issues
"""
from __future__ import print_function

import sys, os, time
import threading
from collections import deque as Deque
from g2base import six
if six.PY2:
    import Queue
else:
    import queue as Queue

from g2base import Bunch, Task, ssdlog
from g2base.remoteObjects import remoteObjects as ro
from g2base.remoteObjects import Timer
from g2base.remoteObjects.ro_config import *

version = '20160330.0'

# Subscriber options
TWO_WAY = 'bidirectional'
CH_ALL  = '*'


class PubSubError(Exception):
    """General class for exceptions raised by this module.
    """
    pass

class PubSubBase(object):
    """Base class for publish/subscribe entities.
    """

    ######## INTERNAL METHODS ########

    def __init__(self, name, logger,
                 ev_quit=None, threadPool=None, numthreads=30,
                 outlimit=6, inlimit=12):
        """
        Constructor for the PubSubBase class.
            name        pubsub name
            logger      logger to be used for any diagnostic messages
            threadPool  optional, threadPool for serving PubSub activities
            numthreads  if a threadPool is NOT furnished, the number of
                          threads to allocate
        """

        super(PubSubBase, self).__init__()

        self.logger = logger
        self.name = name
        self.numthreads = numthreads
        # this limits the number of incoming and outgoing connections
        self.outlimit = outlimit
        self.inlimit = inlimit
        self.outqueue = Queue.PriorityQueue()

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
        if threadPool != None:
            self.threadPool = threadPool
            self.mythreadpool = False

        else:
            self.threadPool = Task.ThreadPool(logger=self.logger,
                                              ev_quit=self.ev_quit,
                                              numthreads=self.numthreads)
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

        # number of seconds to wait before unsubscribing a subscriber
        # who is unresponsive
        self.failure_limit = 30.0
        # initial delay assigned after a delivery failure
        self.redelivery_delay = 0.001
        # delay is increased by this factor with each subsequent failure
        self.redelivery_increase_factor = 2.0
        # delay is decreased by this factor with each subsequent success
        self.redelivery_decrease_factor = 0.6666
        # this is the maximum delivery delay
        self.max_delivery_delay = 1.0

        # warn us when the outgoing queue size exceeds this
        self.qlen_warn_limit = 100
        # but don't warn us more often than this interval
        self.qlen_warn_interval = 10.0

        self.cb_subscr_cnt = 0


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
        self.logger.debug("update: names=%s, channels=%s value=%s" % (
            str(names), str(channels), str(value)))

        self._subscriber_update(value, names, channels, priority)


    def _subscriber_update(self, value, names, channels, priority):
        """
        Internal method to update all subscribers who would be affected
        by these channels.  Triggered by a _named_update() call, which creates
        a task to call this method via the thread pool.  The update includes
        any local objects or remote objects by proxy.
        """
        self.logger.debug("subscriber update: names=%s, channels=%s value=%s" % (
            str(names), str(channels), str(value)))
        #self.logger.debug("value=%s" % (str(value)))

        # Get a list of partners that we should update for this value
        subscribers, all_channels = self._get_subscribers(channels)
        # sets don't go across remoteObjects (yet)
        all_channels = list(all_channels)

        self.logger.debug("subscribers for channel=%s are %s" % (
            str(channels), str(subscribers)))

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
                    partner.backlog.append(queue_record)
                    continue

            adj_priority = time.time() + priority

            # queue up this update
            self.outqueue.put((adj_priority, queue_record))


    def _individual_update(self, queue_record):
        subscriber, value, names, channels, priority = queue_record
        self.logger.debug("attempting to update subscriber '%s' on channels(%s)  with value: %s" % (
            subscriber, str(channels), str(value)))

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

            proxy_obj = partner.proxy

        success = False
        try:
            proxy_obj.remote_update(value, names, channels)
            success = True

        except Exception as e:
            # TODO: capture and log traceback
            self.logger.error("cannot update subscriber '%s': %s" % (
                subscriber, str(e)))

        # failure to deliver update!
        with self._lock:
            if success:
                # successful update

                # decrease future delays by decrease retry factor
                partner.delivery_delay = max(self.redelivery_delay,
                                             partner.delivery_delay *
                                             self.redelivery_decrease_factor)

                backlog_n = len(partner.backlog)
                if (partner.time_failure is None) and (backlog_n == 0):
                    # no current outstanding failures
                    return

                if backlog_n == 0:
                    partner.time_failure = None
                    self.logger.info("subscriber '%s' backlog caught up" % (
                        subscriber))
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

                # TODO: we are only releasing one update at a time
                # and not more to be done in parallel--this could result in
                # a slow comeback.  BUT, maybe slow is good if the client
                # is experiencing congestion
                adj_priority = cur_time
                self.outqueue.put((adj_priority, queue_record))

            else:
                # failure!
                # add this update to the partner's backlog
                partner.backlog.append(queue_record)

                delivery_delay = min(self.max_delivery_delay,
                                     partner.delivery_delay)
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
                    return

                # There may be a problem with this partner/proxy:
                # try to rebuild it
                self.proxy_error(subscriber, partner)

            adj_priority = cur_time

            self.outqueue.put((adj_priority, queue_record))

        task = Task.FuncTask(__requeue, [subscriber, partner], {},
                             logger=self.logger)
        task.init_and_start(self)

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
                priority, queue_record = self.outqueue.get(True, 0.25)

                self._individual_update(queue_record)

            except Queue.Empty:
                continue

    def get_qlen(self):
        return self.outqueue.qsize()

    def get_qelts(self):
        res = [ (elt[0], elt[1][0]) for elt in self.outqueue.queue ]
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
        if isinstance(channels, six.string_types):
            channels = [channels]
        self.logger.debug("channels=%s" % str(channels))

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

        # Monitor update--this can be removed once Monitor is
        # no longer a subclass of PubSub
        if hasattr(self, 'monitor_update'):
            task = Task.FuncTask(self._monitor_update,
                                 (value, names, channels, 0),
                                 {}, logger=self.logger)
        else:
            task = Task.FuncTask(self._named_update,
                                 (value, names, channels),
                                 {'priority': 0},
                                 logger=self.logger)

        task.init_and_start(self)

        return ro.OK


    def notify(self, value, channels, priority=0):
        """
        Method called by local users of this PubSub to update it
        with new and changed items.
            value
            channels    one (a string) or more (a list) of channel names to
                        which to send the specified update
        """
        names = [ self.name ]
        task = Task.FuncTask(self._named_update,
                             (value, names, channels),
                             {'priority': priority},
                             logger=self.logger)

        task.init_and_start(self)

    def proxy_error(self, subscriber, partner):
        pass

class PubSub(PubSubBase):
    """Like a PubSubBase, but adds support for creating and caching
    remote object handles based on service names (i.e. abstract out
    remoteObjects proxy creation).
    """

    def __init__(self, name, logger,
                 ev_quit=None, threadPool=None, numthreads=30,
                 outlimit=4, inlimit=12):
        """Constructor.
        """

        super(PubSub, self).__init__(name, logger,
                                     ev_quit=ev_quit,
                                     threadPool=threadPool,
                                     numthreads=numthreads,
                                     outlimit=outlimit,
                                     inlimit=inlimit)

        # Holds proxies to other pubsubs
        self._proxyCache = {}

        # Holds information about remote subscriptions
        #self._remote_sub_info = set([])
        self._remote_sub_info = {}

        # Timeout for remote updates
        self.remote_timeout = 10.0

        # Interval between remote subscription updates
        self.update_interval = 10.0


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
                kwdargs['transport'] = options['transport']

            # subscriber can be a service name or a host:port
            if not (':' in subscriber):
                proxy_obj = ro.remoteObjectProxy(subscriber, **kwdargs)
            else:
                (host, port) = subscriber.split(':')
                port = int(port)
                proxy_obj = ro.remoteObjectClient(host, port, **kwdargs)

            with self._lock:
                self._proxyCache[subscriber] = proxy_obj
            self.logger.debug("Created proxy for '%s'" % (subscriber))

            return proxy_obj


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

            if isinstance(channels, six.string_types):
                channels = [channels]

            try:
                proxy_obj = self._getProxy(subscriber, options)

                super(PubSub, self)._subscribe(subscriber, proxy_obj,
                                                channels, options)
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

            if isinstance(channels, six.string_types):
                channels = [channels]

            try:
                #proxy_obj = self._getProxy(subscriber, options)

                super(PubSub, self)._unsubscribe(subscriber, channels, options)
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
                kwdargs['transport'] = options['pubtransport']
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
        if type(channels) == str:
            channels = (channels,)
        elif type(channels) == list:
            channels = tuple(channels)
        assert(type(channels) == tuple)

        if not options:
            options = {}

        with self._lock:
            #self._remote_sub_info.add((publisher, channels, options))
            self._remote_sub_info[(publisher, channels)] = (publisher,
                                                            channels, options)

            self._subscribe_remote(publisher, channels, options)


    def unsubscribe_remote(self, publisher, channels, options):

        # Necessary to add to a set; list objects are not hashable
        if type(channels) == str:
            channels = (channels)
        elif type(channels) == list:
            channels = tuple(channels)
        assert(type(channels) == tuple)

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

        class anonClass(object):
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

        super(PubSub, self)._subscribe(subscriber, local_obj,
                                        channels, {})
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

        super(PubSub, self)._unsubscribe(subscriber, channels, {})
        self.logger.debug("local unregistration of '%s' successful." % (
            subscriber))


    # TODO: deprecate this and make apps create their own remoteObjectServer
    # with a delegate to this object??
    def start_server(self, svcname=None, host=None, port=None,
                     ping_interval=default_ns_ping_interval,
                     strict_registration=False,
                     threaded_server=default_threaded_server,
                     authDict=None, default_auth=use_default_auth,
                     secure=default_secure, cert_file=default_cert,
                     ns=None,
                     usethread=True, wait=True, timeout=None):

        if not svcname:
            svcname = self.name
        # make our RO server for remote interface
        self.server = ro.remoteObjectServer(svcname=svcname, obj=self,
                                            logger=self.logger,
                                            ev_quit=self.ev_quit,
                                            port=port, usethread=usethread,
                                            threadPool=self.threadPool,
                                            threaded_server=threaded_server,
                                            numthreads=self.inlimit,
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


if __name__ == '__main__':

    # Parse command line options with nifty new optparse module
    from optparse import OptionParser

    usage = "usage: %prog [options]"
    optprs = OptionParser(usage=usage, version=('%%prog %s' % version))

    optprs.add_option("--config", dest="config",
                      metavar="FILE",
                      help="Use configuration FILE for setup")
    optprs.add_option("--debug", dest="debug", default=False,
                      action="store_true",
                      help="Enter the pdb debugger on main()")
    optprs.add_option("--outlimit", dest="outlimit", type="int", default=6,
                      help="Limit outgoing connections to NUM", metavar="NUM")
    optprs.add_option("--inlimit", dest="inlimit", type="int", default=20,
                      help="Limit incoming connections to NUM", metavar="NUM")
    optprs.add_option("--numthreads", dest="numthreads", type="int",
                      default=100,
                      help="Use NUM threads", metavar="NUM")
    optprs.add_option("--port", dest="port", type="int", default=None,
                      help="Register using PORT", metavar="PORT")
    optprs.add_option("--profile", dest="profile", action="store_true",
                      default=False,
                      help="Run the profiler on main()")
    optprs.add_option("--svcname", dest="svcname", metavar="NAME",
                      default='pubsub',
                      help="Register using NAME as service name")
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

        print("%s profile:" % sys.argv[0])
        profile.run('main(options, args)')

    else:
        main(options, args)


# END
